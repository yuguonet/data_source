# -*- coding: utf-8 -*-
"""
新浪财经数据源 Provider

API来源 & 最新信息:
  - 浏览器F12抓包 https://finance.sina.com.cn/ 观察请求
  - 行情: hq.sinajs.cn/list=sh600519（经典接口）
  - K线日线(主): money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData
  - K线日线(备): quotes.sina.cn/cn/api/json_v2.php (JSONP格式)
  - K线分钟: 同上两个接口，scale参数(1/5/15/30/60)
  - hisdata兜底: finance.sina.com.cn/realstock/company/{code}/hisdata/klc_kl.js（正则解析）
  - 注意: money.finance 的1分钟返回null，必须用quotes.sina.cn

支持的功能:
  - K线: ✅ 1m/5m/15m/30m/1H/1D（不含1W）
  - fetch_ticker: ✅ 单只实时行情（hq.sinajs.cn）
  - fetch_batch_quotes: ✅ 原生批量（hq.sinajs.cn/list=a,b,c 500只/批）


单位注意（重要）:
  - fetch_ticker: parts[3]=最新价(元), parts[8]=成交量(股), 不需要×100
  - fetch_batch_quotes: 同上，parts[8]已是"股"
  - fetch_kline: volume字段直接是"股"，不需要×100
  - 所有价格单位都是"元"，不需要÷
"""

from __future__ import annotations

import itertools
import json
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

_TZ_CN = timezone(timedelta(hours=8))

import requests

from app.data_sources.normalizer import normalize_cn_code as to_sina_code
from app.data_sources.rate_limiter import (
    get_request_headers, RateLimiter, get_shared_session,
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


# 新浪 K线接口 Referer 池
_sina_kline_referers = _RefererPool([
    "https://finance.sina.com.cn/",
    "https://stock.finance.sina.com.cn/",
    "https://vip.stock.finance.sina.com.cn/",
    "https://money.finance.sina.com.cn/",
])

# 新浪行情接口 Referer 池
_sina_quote_referers = _RefererPool([
    "https://finance.sina.com.cn/",
    "https://hq.sinajs.cn/",
    "https://stock.finance.sina.com.cn/",
    "https://money.finance.sina.com.cn/",
])


# ================================================================
# 限流器 — 仅非 market_kline 路径使用（fetch_ticker/fetch_batch_quotes）
# market_kline 路径的限流已移至 Coordinator 统一管理
# ================================================================

_sina_quote_limiter = RateLimiter(
    min_interval=0.8,
    jitter_min=0.3,
    jitter_max=1.2,
)
# ================================================================

# 新浪周期 → scale 参数映射
# scale 表示每根K线的分钟数（日线固定为240分钟）
_SINA_TF_TO_SCALE = {
    "1m": 1, "5m": 5, "15m": 15, "30m": 30, "1H": 60, "1D": 240,
}


def _parse_sina_quote(text: str) -> Optional[Dict[str, Any]]:
    """解析新浪行情响应文本"""
    m = re.search(r'\"(.+?)\"', text)
    if not m:
        return None
    parts = m.group(1).split(",")
    if len(parts) < 32:
        return None
    try:
        name = parts[0].strip()
        if not name:
            return None
        open_p = float(parts[1]) if parts[1] else 0.0
        prev_close = float(parts[2]) if parts[2] else 0.0
        last = float(parts[3]) if parts[3] else 0.0
        high = float(parts[4]) if parts[4] else 0.0
        low = float(parts[5]) if parts[5] else 0.0
        volume = float(parts[8]) if parts[8] else 0.0
        amount = float(parts[9]) if parts[9] else 0.0
        if last == 0 and prev_close == 0 and open_p == 0:
            return None
        return {
            "name": name, "open": open_p, "prev_close": prev_close,
            "last": last, "high": high, "low": low,
            "volume": volume, "amount": amount,
        }
    except (ValueError, IndexError):
        return None


def _sina_kline_to_dicts(data: list, count: int, scale: int = 0) -> List[Dict[str, Any]]:
    """将新浪K线JSON数据转换为标准化字典列表"""
    out: List[Dict[str, Any]] = []
    for item in data:
        try:
            dt_str = str(item.get("day", "")).strip()
            if not dt_str:
                continue
            o = float(item.get("open", 0))
            h = float(item.get("high", 0))
            low = float(item.get("low", 0))
            c = float(item.get("close", 0))
            v = float(item.get("volume", 0))
            if o == 0 and c == 0:
                continue
            # 统一时间格式: 日线 YYYY-MM-DD, 分时线 YYYY-MM-DD HH:MM:00
            if scale and scale < 240 and " " in dt_str:
                try:
                    _dt = datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")
                    dt_str = _dt.strftime("%Y-%m-%d %H:%M") + ":00"
                except (ValueError, TypeError):
                    dt_str = dt_str[:16] + ":00"
            else:
                dt_str = dt_str[:10]
            out.append({
                "time": dt_str, "open": round(o, 4), "high": round(h, 4),
                "low": round(low, 4), "close": round(c, 4), "volume": round(v, 2),
            })
        except (ValueError, TypeError, KeyError):
            continue
    out.sort(key=lambda x: x["time"])
    return out[-count:] if len(out) > count else out


def _fetch_sina_kline_hisdata(sc: str, count: int, timeout: int) -> List[Dict[str, Any]]:
    """通过新浪 hisdata 页面获取日线K线（兜底机制）"""
    url = f"https://finance.sina.com.cn/realstock/company/{sc}/hisdata/klc_kl.js"
    resp = get_shared_session().get(
        url,
        headers=get_request_headers(referer=_sina_kline_referers.next()),
        timeout=timeout,
    )
    resp.encoding = "gbk"
    text = resp.text or ""

    pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2}),\s*"
        r"([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*"
        r"([\d.]+)"
    )
    out: List[Dict[str, Any]] = []
    for m in pattern.finditer(text):
        try:
            dt_str, o, c, h, low, v = m.groups()
            o, c, h, low, v = float(o), float(c), float(h), float(low), float(v)
            if o == 0 and c == 0:
                continue
            out.append({
                "time": dt_str, "open": round(o, 4), "high": round(h, 4),
                "low": round(low, 4), "close": round(c, 4), "volume": round(v, 2),
            })
        except (ValueError, TypeError):
            continue
    if len(out) > count:
        out = out[-count:]
    out.sort(key=lambda x: x["time"])
    return out


# ═══════════════ 前复权（共享模块）═══════════════
from app.data_sources.provider.adjustment import apply_fwd_adjust as _apply_fwd_adjust


# [并发常量] 最大并发线程数 — Coordinator.allocate_threads() 据此分配 worker。
# ⚠️ 请勿删除或随意修改: 此常量直接影响调度层线程分配，改错会导致请求过载或资源浪费。
# 选值依据: 新浪反爬较严格，限流 min_interval=1.5s，4并发较保守。
# 同步位置: source_config.py max_workers 需与此值保持一致。
MAX_CONCURRENCY = 4

@register(priority=20)
class SinaDataSource:
    """
    新浪财经数据源 — A股第二选择（priority=20）。

    能力:
      - K线: 日线（JSON API + hisdata 兜底）+ 分钟线（JSONP API）
      - 行情: 单只实时行情（hq.sinajs.cn）
      - 批量行情: 单次HTTP获取多只（最多500只/批）
      - 全市场行情: 多批次拼接（每批500只，通过东财获取代码列表）

    线程安全性:
      - 实例方法无状态，线程安全
      - 行情限流器（_sina_quote_limiter）用于 fetch_ticker/fetch_batch_quotes
      - K线限流已移至 Coordinator 统一管理（min_interval=1.5）
    """

    name = "sina"
    priority = 15
    max_concurrency = MAX_CONCURRENCY
    min_interval = 1.5
    jitter_min = 0.8
    jitter_max = 2.5

    capabilities = {
        "kline": True,
        "kline_priority": 10,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D"},
        "kline_batch": True,
        "quote": True,
        "quote_priority": 15,
        "batch_quote": True,
        "batch_quote_priority": 15,
        "hk": False,
        "markets": {"CNStock"},
    }

    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 300,
        adj: str = "qfq", timeout: int = 10,
        start_date: str = "", end_date: str = "",
    ) -> Dict[str, Any]:
        sc = to_sina_code(code)
        if not sc:
            return {}
        scale = _SINA_TF_TO_SCALE.get(timeframe)
        if scale is None:
            return {}
        # 有日期范围时，取最大可用量再过滤（新浪 API 最多约 2000 根）
        fetch_count = 2000 if (start_date or end_date) else count
        if timeframe != "1D":
            bars = self._fetch_minute_kline(sc, scale, fetch_count, timeout)
        else:
            bars = self._fetch_raw_daily_kline(sc, fetch_count, timeout)
        if bars and adj in ("qfq", "hfq"):
            bars = _apply_fwd_adjust(bars, code)

        # 日期过滤
        if bars and (start_date or end_date):
            from app.data_sources.provider import filter_bars_by_date
            bars = filter_bars_by_date(bars, start_date, end_date)

        if not bars:
            return {}
        return {"bars": bars, "count": len(bars)}

    def _fetch_raw_daily_kline(self, sc: str, count: int, timeout: int) -> List[Dict[str, Any]]:
        # 优先 money.finance（纯 JSON，稳定），兜底 quotes.sina.cn（JSONP）
        bars = self._fetch_money_finance_kline(sc, 240, count, timeout)
        if bars:
            return bars
        return self._fetch_quotes_sina_kline(sc, 240, count, timeout)

    def _fetch_minute_kline(self, sc: str, scale: int, count: int, timeout: int) -> List[Dict[str, Any]]:
        # 1分钟: money.finance 返回 null，必须用 quotes.sina.cn JSONP
        # 5/15/30/60分钟: money.finance 可用且是纯 JSON，优先使用
        if scale == 1:
            bars = self._fetch_quotes_sina_kline(sc, scale, count, timeout)
        else:
            bars = self._fetch_money_finance_kline(sc, scale, count, timeout)
            if not bars:
                bars = self._fetch_quotes_sina_kline(sc, scale, count, timeout)
        return bars

    def _fetch_money_finance_kline(self, sc: str, scale: int, count: int, timeout: int) -> List[Dict[str, Any]]:
        """money.finance.sina.com.cn — 纯 JSON，日线+5/15/30/60分钟可用，1分钟返回 null"""
        url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {"symbol": sc, "scale": scale, "ma": "no", "datalen": min(int(count), 2000)}
        try:
            resp = get_shared_session().get(
                url,
                headers=get_request_headers(referer=_sina_kline_referers.next()),
                params=params, timeout=timeout,
            )
            data = resp.json()
            if isinstance(data, list) and data:
                return _sina_kline_to_dicts(data, count, scale)
        except Exception:
            pass
        return []

    def _fetch_quotes_sina_kline(self, sc: str, scale: int, count: int, timeout: int) -> List[Dict[str, Any]]:
        """quotes.sina.cn — JSONP 格式，全周期可用（含1分钟）"""
        url = "https://quotes.sina.cn/cn/api/jsonp_v2.php/var/CN_MarketDataService.getKLineData"
        params = {"symbol": sc, "scale": scale, "ma": "no", "datalen": min(int(count), 2000)}
        try:
            resp = get_shared_session().get(
                url,
                headers=get_request_headers(referer=_sina_kline_referers.next()),
                params=params, timeout=timeout,
            )
            text = (resp.text or "").strip()
            m = re.search(r'\[.*\]', text, re.DOTALL)
            if m:
                data = json.loads(m.group())
                if isinstance(data, list) and data:
                    return _sina_kline_to_dicts(data, count, scale)
        except Exception:
            pass
        return []

    def fetch_ticker(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        sc = to_sina_code(code)
        if not sc:
            return None
        _sina_quote_limiter.wait()
        resp = get_shared_session().get(
            f"https://hq.sinajs.cn/list={sc}",
            headers=get_request_headers(referer=_sina_quote_referers.next()),
            timeout=timeout,
        )
        resp.encoding = "gbk"
        quote = _parse_sina_quote(resp.text)
        if not quote:
            return None
        last = quote["last"]
        prev = quote["prev_close"]
        chg = round(last - prev, 4) if prev else 0.0
        vol = quote.get("volume", 0)
        time_str = ""
        parts_raw = re.search(r'\"(.+?)\"', resp.text)
        if parts_raw:
            p = parts_raw.group(1).split(",")
            if len(p) > 31 and p[30] and p[31]:
                time_str = f"{p[30].strip()} {p[31].strip()}"
        return {
            "last": last,
            "change": chg,
            "changePercent": round(chg / prev * 100, 2) if prev else 0.0,
            "high": quote.get("high", last),
            "low": quote.get("low", last),
            "open": quote.get("open", last) or last,
            "previousClose": prev,
            "volume": vol, "time": time_str,
            "name": quote.get("name", ""),
            "symbol": sc,
        }

    def fetch_batch_quotes(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        if not codes:
            return {}
        sina_codes = [to_sina_code(c) for c in codes if c]
        if not sina_codes:
            return {}

        batch_size = 500
        batches = [sina_codes[i:i + batch_size] for i in range(0, len(sina_codes), batch_size)]

        if len(batches) <= 1:
            # 只有 1 批，直接串行，没必要开线程池
            result: Dict[str, Dict[str, Any]] = {}
            self._fetch_single_quote_batch(batches[0], result, timeout)
            return result

        # 多批并发
        import concurrent.futures
        result: Dict[str, Dict[str, Any]] = {}
        lock = threading.Lock()
        max_workers = min(len(batches), 2)

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
        query = ",".join(batch)
        _sina_quote_limiter.wait()
        try:
            resp = get_shared_session().get(
                f"https://hq.sinajs.cn/list={query}",
                headers=get_request_headers(referer=_sina_quote_referers.next()),
                timeout=timeout,
            )
            resp.encoding = "gbk"
        except Exception as e:
            logger.warning("[新浪批量行情] 请求失败: %s", e)
            return

        for line in (resp.text or "").strip().split("\n"):
            line = line.strip().rstrip(";")
            m = re.search(r'hq_str_(\w+)="(.+?)"', line)
            if not m:
                continue
            code_str = m.group(1)
            data = m.group(2)
            parts = data.split(",")
            if len(parts) < 6:
                continue
            try:
                name = parts[0].strip()
                if not name:
                    continue
                open_p = float(parts[1]) if parts[1] else 0.0
                prev_close = float(parts[2]) if parts[2] else 0.0
                last = float(parts[3]) if parts[3] else 0.0
                high = float(parts[4]) if parts[4] else 0.0
                low = float(parts[5]) if parts[5] else 0.0
                vol = float(parts[8]) if len(parts) > 8 and parts[8] else 0.0
                if last == 0 and prev_close == 0 and open_p == 0:
                    continue
                chg = round(last - prev_close, 4) if prev_close else 0.0
                time_str = ""
                if len(parts) > 31 and parts[30] and parts[31]:
                    time_str = f"{parts[30].strip()} {parts[31].strip()}"
                result[code_str] = {
                    "name": name, "last": last, "change": chg,
                    "changePercent": round(chg / prev_close * 100, 2) if prev_close else 0.0,
                    "open": open_p, "high": high, "low": low,
                    "previousClose": prev_close, "volume": vol, "time": time_str,
                    "symbol": code_str,
                }
            except (ValueError, IndexError):
                continue


