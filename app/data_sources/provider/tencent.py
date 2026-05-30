# -*- coding: utf-8 -*-
"""
腾讯财经数据源 Provider

API来源 & 最新信息:
  - 浏览器F12抓包 https://gu.qq.com/ 或 https://stockapp.finance.qq.com/
  - 行情: qt.gtimg.cn/q=sh600519（经典接口，多年未变）
  - K线分钟: ifzq.gtimg.cn/appstock/app/kline/mkline?param=sh600519,m15,300
  - K线日周: web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh600519,day,,,300,qfq
  - mkline: 分钟线（m1/m5/m15/m30/m60），不支持复权
  - fqkline: 日/周线（day/week），支持前/后复权

支持的功能:
  - K线: ✅ 全周期 1m/5m/15m/30m/1H/1D/1W
  - fetch_ticker: ✅ 单只实时行情（qt.gtimg.cn）
  - fetch_batch_quotes: ✅ 原生批量（qt.gtimg.cn/q=a,b,c 500只/批）

  - 港股: ✅ 支持港股K线和行情

单位注意（重要）:
  - fetch_ticker: parts[3]=最新价(元), parts[6]=成交量, 已×100转"股"
  - fetch_batch_quotes: 同上，parts[6]已×100转"股"
  - fetch_kline (_rows_to_dicts): volume(r[5])已×100转"股"
  - 所有价格单位都是"元"，不需要÷
"""

from __future__ import annotations

import itertools
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from app.data_sources.normalizer import (
    normalize_cn_code as to_tencent_code, normalize_hk_code,
)
from app.data_sources.rate_limiter import (
    get_request_headers, get_tencent_limiter,
)
from app.data_sources.provider import register, NotSupportedResult
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ================================================================
# Referer 轮换池 — 提高访问成功率
# ================================================================

class _RefererPool:
    """线程安全的 Referer 轮换池"""

    def __init__(self, referers: List[str]):
        self._referers = referers
        self._cycle = itertools.cycle(referers)
        self._lock = threading.Lock()

    def next(self) -> str:
        with self._lock:
            return next(self._cycle)


# 腾讯 K线接口 Referer 池
_tc_kline_referers = _RefererPool([
    "https://gu.qq.com/",
    "https://finance.qq.com/",
    "https://stockapp.finance.qq.com/",
    "https://stock.qq.com/",
])

# 腾讯行情接口 Referer 池
_tc_quote_referers = _RefererPool([
    "https://qt.gtimg.cn/",
    "https://gu.qq.com/",
    "https://finance.qq.com/",
    "https://stockapp.finance.qq.com/",
])


def _lower(code: str) -> str:
    """将股票代码转为小写并去除首尾空格，用于腾讯API参数"""
    return (code or "").strip().lower()


# 内部周期 → 腾讯API参数映射
_TF_MAP = {
    "1m": ("mkline", "m1"),   "5m": ("mkline", "m5"),
    "15m": ("mkline", "m15"), "30m": ("mkline", "m30"),
    "1H": ("mkline", "m60"),
    "1D": ("fqkline", "day"), "1W": ("fqkline", "week"),
}


def _parse_time(ds: str) -> Optional[str]:
    """解析时间字符串为标准格式 'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD'"""
    raw = str(ds or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).strftime(
                "%Y-%m-%d %H:%M:%S" if " " in raw else "%Y-%m-%d"
            )
        except ValueError:
            continue
    # 纯数字时间戳（毫秒/秒）
    try:
        ts = int(float(raw))
        if ts > 10**12:
            ts = ts // 1000
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _rows_to_dicts(rows: list, timeframe: str = "1D") -> List[Dict[str, Any]]:
    """将腾讯API返回的原始行数据转换为标准化K线字典列表"""
    out = []
    _daily_tfs = {"1D", "1W"}
    for r in rows:
        if not isinstance(r, (list, tuple)) or len(r) < 6:
            continue
        ts = _parse_time(r[0])
        if ts is None:
            continue
        try:
            o, c, h, low, vol = float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])
        except (TypeError, ValueError):
            continue
        # 统一时间格式: 日线以上 YYYY-MM-DD, 分时线 YYYY-MM-DD HH:MM:00
        if timeframe in _daily_tfs:
            ts = ts[:10]
        else:
            try:
                _dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
                ts = _dt.strftime("%Y-%m-%d %H:%M") + ":00"
            except (ValueError, TypeError):
                ts = ts[:16] + ":00"
        out.append({
            "time": ts, "open": round(o, 4), "high": round(h, 4),
            "low": round(low, 4), "close": round(c, 4), "volume": round(vol * 100, 2),
        })
    return out


# [并发常量] 最大并发线程数 — Coordinator.allocate_threads() 据此分配 worker。
# ⚠️ 请勿删除或随意修改: 此常量直接影响调度层线程分配，改错会导致请求过载或资源浪费。
# 选值依据: 腾讯HTTP接口，限流 min_interval=1.0s，6并发较稳健。
# 同步位置: source_config.py max_workers 需与此值保持一致。
MAX_CONCURRENCY = 6

@register(priority=10)
class TencentDataSource:
    """
    腾讯财经数据源 — A股首选数据源（priority=10）。

    能力:
      - K线: 全周期（分钟/日/周），通过 mkline 和 fqkline 两个API
      - 行情: 单只实时行情（qt.gtimg.cn）
      - 批量行情: 单次HTTP获取多只（最多500只/批）
      - 全市场行情: 多批次拼接（每批500只，通过东财获取代码列表）
      - 港股: 支持港股K线和行情

    线程安全性:
      - 实例方法无状态，线程安全
      - 通过 get_tencent_limiter() 进行全局限流
    """

    name = "tencent"
    priority = 10
    max_concurrency = MAX_CONCURRENCY
    min_interval = 1.0
    jitter_min = 0.5
    jitter_max = 1.5

    capabilities = {
        "kline": True,
        "kline_priority": 15,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D", "1W"},
        "kline_batch": True,
        "quote": True,
        "quote_priority": 10,
        "batch_quote": True,
        "batch_quote_priority": 10,
        "hk": True,
        "markets": {"CNStock", "HKStock"},
    }

    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 300,
        timeout: int = 10,
        start_date: str = "", end_date: str = "",
    ) -> Dict[str, Any]:
        if start_date:
            from app.data_sources.provider import calc_kline_count
            count = calc_kline_count(timeframe, start_date, end_date)

        c = _lower(to_tencent_code(code))
        if not c:
            return {}

        endpoint, tc_tf = _TF_MAP.get(timeframe, (None, None))
        if not endpoint:
            return {}

        if endpoint == "mkline":
            url = "https://ifzq.gtimg.cn/appstock/app/kline/mkline"
            params = {"param": f"{c},{tc_tf},{int(count)}"}
        else:
            url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
            # fqkline 原生支持日期范围: param=code,period,start,end,count,adj
            sd = start_date if start_date else ""
            ed = end_date if end_date else ""
            # adj: "qfq"=前复权, "hfq"=后复权, ""=不复权
            params = {"param": f"{c},{tc_tf},{sd},{ed},{int(count)},"}

        resp = requests.get(
            url, headers=get_request_headers(referer=_tc_kline_referers.next()),
            params=params, timeout=timeout,
        )

        if resp.status_code != 200:
            logger.warning("[tencent] %s %s HTTP %s", timeframe, c, resp.status_code)
            return {}

        try:
            data = resp.json()
        except Exception:
            logger.warning("[tencent] %s %s JSON解析失败, body前100字: %s", timeframe, c, (resp.text or "")[:100])
            return {}

        if not isinstance(data, dict) or int(data.get("code", 0)) != 0:
            return {}

        root = (data.get("data") or {}).get(c)
        if not isinstance(root, dict):
            logger.warning("[tencent] %s %s root不是dict, data.keys=%s", timeframe, c, list((data.get("data") or {}).keys()))
            return {}

        rows = None
        if endpoint == "mkline":
            rows = root.get(tc_tf)
        else:
            # fqkline 返回的 key 随 adj 变化:
            #   adj="qfq" → "qfqday", adj="hfq" → "hfqday", adj="" → "day"
            adj_key = tc_tf
            arr = root.get(adj_key)
            if isinstance(arr, list) and arr:
                rows = arr
            if rows is None:
                # 兜底: 按后缀匹配
                for k, v in root.items():
                    if isinstance(v, list) and v and str(k).lower().endswith(tc_tf):
                        rows = v
                        break

        bars = _rows_to_dicts(rows, timeframe) if isinstance(rows, list) else []
        if not bars:
            return {}
        return {"bars": bars, "count": len(bars)}

    def fetch_ticker(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        c = _lower(to_tencent_code(code))
        if not c:
            return None

        get_tencent_limiter().wait()
        resp = requests.get(
            f"https://qt.gtimg.cn/q={c}",
            headers=get_request_headers(referer=_tc_quote_referers.next()),
            timeout=timeout,
        )
        try:
            resp.encoding = "gbk"
        except Exception:
            pass

        text = (resp.text or "").strip()
        if not text or "~" not in text:
            return None

        try:
            start = text.index('="') + 2
            end = text.rindex('"')
            parts = text[start:end].split("~")
        except Exception:
            return None

        if len(parts) < 6:
            return None

        def _f(i, d=0.0):
            try:
                return float(parts[i]) if i < len(parts) and parts[i] else d
            except Exception:
                return d

        last, prev = _f(3), _f(4)
        chg = round(last - prev, 4) if prev else 0
        vol = _f(6)
        raw_time = parts[30].strip() if len(parts) > 30 and parts[30] else ""
        time_str = ""
        if len(raw_time) == 14 and raw_time.isdigit():
            time_str = f"{raw_time[:4]}-{raw_time[4:6]}-{raw_time[6:8]} {raw_time[8:10]}:{raw_time[10:12]}:{raw_time[12:14]}"
        return {
            "last": last, "change": chg,
            "changePercent": round(chg / prev * 100, 2) if prev else 0,
            "high": _f(33, last), "low": _f(34, last),
            "open": _f(5) or last, "previousClose": prev,
            "volume": vol * 100, "time": time_str,
            "name": (parts[1] or "").strip(),
            "symbol": (parts[2] or "").strip(),
        }

    def fetch_batch_quotes(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        if not codes:
            return {}
        lowered = [_lower(to_tencent_code(c)) for c in codes if c]
        if not lowered:
            return {}

        batch_size = 500
        batches = [lowered[i:i + batch_size] for i in range(0, len(lowered), batch_size)]

        if len(batches) <= 1:
            # 只有 1 批，直接串行，没必要开线程池
            result: Dict[str, Dict[str, Any]] = {}
            self._fetch_single_quote_batch(batches[0], result, timeout)
            return result

        # 多批并发
        import concurrent.futures
        result: Dict[str, Dict[str, Any]] = {}
        lock = threading.Lock()
        max_workers = min(len(batches), 5)

        def _fetch_batch(batch):
            local: Dict[str, Dict[str, Any]] = {}
            self._fetch_single_quote_batch(batch, local, timeout)
            if local:
                with lock:
                    result.update(local)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_fetch_batch, b) for b in batches]
            concurrent.futures.wait(futures, timeout=timeout + 5)

        return result

    def _fetch_single_quote_batch(
        self, batch: List[str], result: Dict[str, Dict[str, Any]], timeout: int
    ):
        """单批次行情请求（内部辅助，供并发调用）"""
        get_tencent_limiter().wait()
        try:
            resp = requests.get(
                f"https://qt.gtimg.cn/q={','.join(batch)}",
                headers=get_request_headers(referer=_tc_quote_referers.next()),
                timeout=timeout,
            )
            resp.encoding = "gbk"
        except Exception:
            return

        for line in (resp.text or "").strip().split("\n"):
            line = line.strip().rstrip(";")
            if "=" not in line or '""' in line:
                continue
            try:
                var_name, data = line.split("=", 1)
                parts = data.strip('"').split("~")
                if len(parts) < 6 or not parts[1]:
                    continue
                for c in batch:
                    if c in var_name:
                        last = float(parts[3]) if parts[3] else 0
                        if last <= 0:
                            break
                        prev = float(parts[4]) if parts[4] else 0
                        chg = round(last - prev, 4) if prev else 0
                        vol = float(parts[6]) if len(parts) > 6 and parts[6] else 0
                        raw_time = parts[30].strip() if len(parts) > 30 and parts[30] else ""
                        # 统一格式: "20260510150000" → "2026-05-10 15:00:00"
                        time_str = ""
                        if len(raw_time) == 14 and raw_time.isdigit():
                            time_str = f"{raw_time[:4]}-{raw_time[4:6]}-{raw_time[6:8]} {raw_time[8:10]}:{raw_time[10:12]}:{raw_time[12:14]}"
                        result[c] = {
                            "last": last, "change": chg,
                            "changePercent": round(chg / prev * 100, 2) if prev else 0,
                            "high": float(parts[33]) if len(parts) > 33 and parts[33] else last,
                            "low": float(parts[34]) if len(parts) > 34 and parts[34] else last,
                            "open": float(parts[5]) if parts[5] else last,
                            "previousClose": prev,
                            "volume": vol * 100,
                            "time": time_str,
                            "name": parts[1].strip(),
                            "symbol": parts[2].strip(),
                        }
                        break
            except Exception:
                continue


