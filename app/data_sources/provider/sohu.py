# -*- coding: utf-8 -*-
"""
搜狐财经数据源 Provider

API来源 & 最新信息:
  - 浏览器F12抓包 https://q.stock.sohu.com/ 观察请求

━━━ 1. 日/周/月 K线 (hisHq) ━━━
  URL: q.stock.sohu.com/hisHq?code=cn_{code}&start=20200101&end=20261231&period={d|w|m}
  返回: [[{hq: [[日期,开盘,收盘,涨跌额,涨跌幅,最低,最高,成交量,成交额,换手率], ...]}]]
  注意: 返回结果最新在前（倒序），取前limit条即可

━━━ 2. 分钟级K线 (hq.stock.sohu.com) ━━━
  路径规则: hq.stock.sohu.com/{market}/{code_last3}/{biz_code}-9_{period}m.html
  code_last3 = 纯数字代码的后3位, 如 600519 → 519
  例: hq.stock.sohu.com/cn/519/cn_600519-9_5m.html
  支持: 5m / 15m / 30m / 60m
  返回: JSONP kline_Xm([[时间戳,开,收,高,低,成交量(手),成交额,涨跌%,涨跌额,换手率], ...])
  时间戳格式: 'YYMMDDHHMM' (10位), 如 '2605071345' → 2026-05-07 13:45

━━━ 3. 当日分时 (1分钟线) ━━━
  URL: hq.stock.sohu.com/{market}/{code_last3}/{biz_code}-4.html
  返回: JSONP time_data([[昨收,今开,最高,最低,总额],[时间,价格,均价,成交量(手),成交额], ...])

━━━ 4. 单只实时行情 (心跳接口) ━━━
  URL: hq.stock.sohu.com/{market}/{code_last3}/{biz_code}-1.html
  返回: JSONP fortune_hq({...})，含 price_A1 + price_A2 + time 等字段
  price_A1: [code, name, price, change, change%, status, ?, --]
  price_A2 字段映射 (通过页面 data-field 属性验证):
    [0]=均价  [1]=昨收  [2]=现手  [3]=今开  [4]=量比
    [5]=最高  [6]=换手率  [7]=最低  [8]=总手  [9]=涨停
    [10]=市盈(PE)  [11]=跌停  [12]=总金额(万)  [13]=当前价
    [14]=振幅  [15]=?  [16]=总市值  [17]=?
  time: ['2026','05','13','15','00','55']

━━━ 5. 批量行情快照 (getqjson) ━━━
  URL: hqm.stock.sohu.com/getqjson?code=cn_600519,cn_000001,...&cb=xxx
  返回: JSONP {cn_600519: [...], cn_000001: [...], ...}
  字段映射 (通过 -1.html data-field 交叉验证):
    [0]=code  [1]=name  [2]=last  [3]=change%  [4]=change
    [5]=总手(volume, 手)  [6]=现手  [7]=总金额(万)  [8]=换手率
    [9]=量比  [10]=最高  [11]=最低  [12]=PE
    [13]=昨收  [14]=今开  [15]=url  [16]=总市值(亿)  [17]=time
  支持逗号分隔多只股票，单次请求无明显数量限制

支持的功能:
  - K线: ✅ 日线(period=d) + 周线(period=w) + 月线(period=m)
  - K线 分钟线: ✅ 5m / 15m / 30m / 60m（历史分钟K线） + 当日1m分时
  - fetch_ticker: ✅ 通过 -1.html 心跳接口获取实时行情
  - fetch_batch_quotes: ✅ 通过 hqm getqjson 批量接口（一次请求多只）


单位注意（重要）:
  - fetch_kline: volume(r[7])返回"手"，代码中已×100转"股"
  - fetch_ticker / fetch_batch_quotes: volume 返回"股"（代码中总手×100）
  - 价格字段直接是"元"，不需要÷
  - amount 字段单位为"万元"
  - 复权: 不复权数据通过 TDX 除权除息数据(adjustment模块)转前复权
"""

from __future__ import annotations

import json
import re
import ssl
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

_TZ_CN = timezone(timedelta(hours=8))

from app.data_sources.provider import register, NotSupportedResult
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ================================================================
# 基础配置
# ================================================================

TIMEOUT = 10

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE


# ================================================================
# HTTP 工具
# ================================================================

def _http_get_json(url: str, timeout: int = TIMEOUT) -> Optional[Any]:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
            raw = resp.read()
            for enc in ("utf-8", "gbk", "gb2312", "latin-1"):
                try:
                    text = raw.decode(enc)
                    return json.loads(text)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
            return None
    except Exception:
        return None


def _http_get_text(url: str, timeout: int = TIMEOUT) -> Optional[str]:
    """获取URL文本内容（用于JSONP接口）"""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
            raw = resp.read()
            for enc in ("utf-8", "gbk", "gb2312", "latin-1"):
                try:
                    return raw.decode(enc)
                except UnicodeDecodeError:
                    continue
            return None
    except Exception:
        return None


def _parse_jsonp(text: str) -> Optional[Any]:
    """解析JSONP回调格式: callback({...}) 或 callback([{...}])
    支持单引号格式（搜狐API返回单引号JSONP）
    """
    try:
        start = text.index("(") + 1
        end = text.rindex(")")
        body = text[start:end]
        # 先尝试标准 JSON（双引号）
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            pass
        # 兜底: 单引号 → 双引号（搜狐API返回单引号格式）
        try:
            return json.loads(body.replace("'", '"'))
        except json.JSONDecodeError:
            pass
        # 最终兜底: ast.literal_eval 处理 Python 字面量
        import ast
        try:
            return ast.literal_eval(body)
        except (ValueError, SyntaxError):
            pass
        logger.warning("[sohu] _parse_jsonp: 所有解析方式均失败, body前100字: %s", body[:100])
        return None
    except (ValueError, json.JSONDecodeError):
        return None


# ================================================================
# 代码转换
# ================================================================

def _cn(code: str) -> str:
    """提取纯数字代码"""
    c = code.strip().upper().replace(".", "").replace("SH", "").replace("SZ", "").replace("BJ", "")
    return c


# ================================================================
# 前复权（共享模块）
# ================================================================
from app.data_sources.provider.adjustment import apply_fwd_adjust as _apply_fwd_adjust


# ================================================================
# 数据获取
# ================================================================

def _fetch_sohu_kline(code: str, period: str = "d", limit: int = 200,
                      start_date: str = "", end_date: str = "") -> Optional[List[Dict[str, Any]]]:
    """获取单只股票K线（不复权），支持日线(d)/周线(w)/月线(m)"""
    cn_code = _cn(code)
    # 使用传入的日期范围，缺省用宽范围兜底
    sd = start_date.replace("-", "") if start_date else "19900101"
    ed = end_date.replace("-", "") if end_date else "20261231"
    url = f"https://q.stock.sohu.com/hisHq?code=cn_{cn_code}&start={sd}&end={ed}&period={period}"
    data = _http_get_json(url)
    if not data or not isinstance(data, list):
        return None

    hq = data[0].get("hq") or []
    if not hq:
        return None

    # 搜狐返回: [日期, 开盘, 收盘, 涨跌额, 涨跌幅, 最低, 最高, 成交量, 成交额, 换手率]
    result = []
    for r in hq:
        if len(r) < 6:
            continue
        try:
            dt_str = str(r[0])[:10]
            result.append({
                "time": dt_str,
                "open": round(float(r[1]), 4),
                "high": round(float(r[6]), 4) if len(r) > 6 else round(float(r[1]), 4),
                "low": round(float(r[5]), 4) if len(r) > 5 else round(float(r[1]), 4),
                "close": round(float(r[2]), 4),
                "volume": round(float(r[7]) * 100, 2) if len(r) > 7 else 0,
            })
        except (ValueError, TypeError, IndexError):
            continue

    # 搜狐返回最新在前，反转为最旧在前（与其他源一致），取最近 limit 条
    result.reverse()
    return result[-limit:] if limit and len(result) > limit else result


def _parse_minute_timestamp(ts: str) -> str:
    """搜狐分钟K线时间戳 → 'YYYY-MM-DD HH:MM:00'
    格式: 'YYMMDDHHMM' (10位), 如 '2605071345' → '2026-05-07 13:45:00'
    """
    ts = str(ts).strip()
    if len(ts) >= 10:
        year = "20" + ts[:2]
        month = ts[2:4]
        day = ts[4:6]
        hour = ts[6:8]
        minute = ts[8:10]
        return f"{year}-{month}-{day} {hour}:{minute}:00"
    return ts


def _fetch_sohu_minute_kline(
    code: str, period: str = "5m", limit: int = 200
) -> Optional[List[Dict[str, Any]]]:
    """获取历史分钟K线（5m/15m/30m/60m），不复权。

    接口: hq.stock.sohu.com/{market}/{code_last3}/{biz_code}-9_{period}m.html
    返回JSONP: kline_Xm([[时间戳,开,收,高,低,成交量(手),成交额,涨跌%,涨跌额,换手率], ...])
    """
    cn_code = _cn(code)
    biz_code = f"cn_{cn_code}"
    code_last3 = cn_code[3:] if len(cn_code) > 3 else cn_code
    url = f"https://hq.stock.sohu.com/cn/{code_last3}/{biz_code}-9_{period}.html"
    text = _http_get_text(url)
    if not text:
        return None

    data = _parse_jsonp(text)
    if not data or not isinstance(data, list):
        return None

    result = []
    for r in data:
        if len(r) < 6:
            continue
        try:
            result.append({
                "time": _parse_minute_timestamp(r[0]),
                "open": round(float(r[1]), 4),
                "high": round(float(r[3]), 4),
                "low": round(float(r[4]), 4),
                "close": round(float(r[2]), 4),
                "volume": round(float(r[5]) * 100, 2),  # 手 → 股
            })
        except (ValueError, TypeError, IndexError):
            continue

    # 搜狐分钟K线返回时间升序（最旧在前），取末尾 limit 条（最新）
    return result[-limit:] if limit and len(result) > limit else result


def _fetch_sohu_intraday(code: str) -> Optional[List[Dict[str, Any]]]:
    """获取当日1分钟分时数据（不复权）。

    接口: hq.stock.sohu.com/{market}/{code_last3}/{biz_code}-4.html
    返回JSONP: time_data([[昨收,今开,最高,最低,总额],[时间,价格,均价,成交量(手),成交额], ...])
    """
    cn_code = _cn(code)
    biz_code = f"cn_{cn_code}"
    code_last3 = cn_code[3:] if len(cn_code) > 3 else cn_code
    url = f"https://hq.stock.sohu.com/cn/{code_last3}/{biz_code}-4.html"
    text = _http_get_text(url)
    if not text:
        return None

    data = _parse_jsonp(text)
    if not data or not isinstance(data, list) or len(data) < 2:
        return None

    # 提取日期（从第一个时间条目的时间字段）
    first_bar = data[1]
    if len(first_bar) < 5:
        return None

    time_str = str(first_bar[0])  # e.g. "09:31"
    # 构建日期: 使用今天
    today = datetime.now(_TZ_CN).strftime("%Y-%m-%d")

    result = []
    for r in data[1:]:  # 跳过第一个 header 行
        if len(r) < 5:
            continue
        try:
            bar_time = str(r[0])  # "HH:MM"
            result.append({
                "time": f"{today} {bar_time}:00",
                "open": round(float(r[1]), 4),   # 价格（开盘=价格）
                "high": round(float(r[1]), 4),    # 分时线无独立OHLC，用价格代替
                "low": round(float(r[1]), 4),
                "close": round(float(r[1]), 4),
                "volume": round(float(r[3]) * 100, 2),  # 成交量(手) → 股
            })
        except (ValueError, TypeError, IndexError):
            continue

    return result


# ================================================================
# 实时行情 / 批量快照
# ================================================================

def _parse_sohu_heartbeat(text: str) -> Optional[Dict[str, Any]]:
    """解析 -1.html (fortune_hq) 心跳数据，提取实时行情。

    格式: fortune_hq({...}) — JS对象(单引号、无引号key)
    关键字段:
      price_A1: [code, name, price, change, change%, status, ?, --]
      price_A2: [均价, 昨收, 现手, 今开, 量比, 最高, 换手率, 最低, 总手, 涨停, 市盈, 跌停, 总金额, 当前价, 振幅, ?, 总市值, ?]
    """
    # 提取 fortune_hq({...}) 内的 JS 对象
    try:
        start = text.index("{")
        end = text.rindex("}") + 1
        js_body = text[start:end]
    except ValueError:
        return None

    # 用正则从 JS 对象中提取 price_A1 和 price_A2 数组
    def _extract_array(key: str) -> List[str]:
        m = re.search(r"'" + key + r"':\[([^\]]*)\]", js_body)
        if not m:
            return []
        return re.findall(r"'([^']*)'", m.group(1))

    pa1 = _extract_array("price_A1")
    pa2 = _extract_array("price_A2")
    time_arr = _extract_array("time")

    if len(pa1) < 3 or len(pa2) < 13:
        return None

    def _float(s: str) -> Optional[float]:
        try:
            if not s or s == "--" or s == "-":
                return None
            return float(s.replace(",", "").replace("%", "").replace("万亿", "").replace("亿", ""))
        except (ValueError, TypeError):
            return None

    # price_A2 映射 (通过页面 data-field 验证):
    #   [0]=均价  [1]=昨收  [2]=现手  [3]=今开  [4]=量比
    #   [5]=最高  [6]=换手率  [7]=最低  [8]=总手  [9]=涨停
    #   [10]=市盈(PE)  [11]=跌停  [12]=总金额(万)  [13]=当前价
    #   [14]=振幅  [15]=?  [16]=总市值  [17]=?
    result = {
        "code": pa1[0],
        "name": pa1[1],
        "last": _float(pa1[2]),
        "open": _float(pa2[3]),
        "high": _float(pa2[5]),
        "low": _float(pa2[7]),
        "prev_close": _float(pa2[1]),
        "change": _float(pa1[3]),
        "change_pct": _float(pa1[4]),
        "volume": round(float(pa2[8]) * 100, 2) if _float(pa2[8]) else 0,  # 总手→股
        "amount": _float(pa2[12]),  # 总金额(万)
        "turnover_rate": _float(pa2[6]),
        "PE": _float(pa2[10]),
        "amplitude": _float(pa2[14]),
        "limit_up": _float(pa2[9]),
        "limit_down": _float(pa2[11]),
    }

    if len(time_arr) >= 6:
        result["time"] = f"{time_arr[0]}-{time_arr[1]}-{time_arr[2]} {time_arr[3]}:{time_arr[4]}:{time_arr[5]}"

    return result


def _fetch_sohu_ticker(code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
    """获取单只股票实时行情（通过 -1.html 心跳接口）。

    接口: hq.stock.sohu.com/{market}/{code_last3}/{biz_code}-1.html
    """
    cn_code = _cn(code)
    biz_code = f"cn_{cn_code}"
    code_last3 = cn_code[3:] if len(cn_code) > 3 else cn_code
    url = f"https://hq.stock.sohu.com/cn/{code_last3}/{biz_code}-1.html"
    text = _http_get_text(url, timeout=timeout)
    if not text:
        return None
    return _parse_sohu_heartbeat(text)


def _fetch_sohu_batch_quotes(codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
    """批量获取实时行情快照（通过 hqm getqjson 接口）。

    接口: hqm.stock.sohu.com/getqjson?code=cn_000001,cn_600519,...&cb=xxx
    单次最多 100 只，超出自动分批请求并合并结果。
    返回字段 (已通过 -1.html data-field 交叉验证):
      [0]=code  [1]=name  [2]=last  [3]=change%  [4]=change
      [5]=总手(volume)  [6]=现手  [7]=总金额(万)  [8]=换手率
      [9]=量比  [10]=最高  [11]=最低  [12]=PE
      [13]=昨收  [14]=今开  [15]=url  [16]=总市值(亿)  [17]=time
    """
    if not codes:
        return {}

    _SOHU_BATCH_LIMIT = 40

    # 构建 biz_code → 原始 code 的双向映射
    # 调用方传入 "SH600519" / "600519" 等格式，需映射回来
    biz_to_orig: Dict[str, str] = {}
    for c in codes:
        cn = _cn(c)
        if cn:
            biz_to_orig[f"cn_{cn}"] = c

    if not biz_to_orig:
        return {}

    def _parse_batch(batch_biz: List[str]) -> Dict[str, Dict[str, Any]]:
        code_str = ",".join(batch_biz)
        url = f"https://hqm.stock.sohu.com/getqjson?code={code_str}&cb=_hq_cb"
        text = _http_get_text(url, timeout=timeout)
        if not text:
            return {}
        data = _parse_jsonp(text)
        if not data or not isinstance(data, dict):
            return {}

        def _float(s: Any) -> Optional[float]:
            try:
                if not s or s == "--" or s == "-":
                    return None
                return float(str(s).replace(",", "").replace("%", ""))
            except (ValueError, TypeError):
                return None

        parsed = {}
        for biz_code, arr in data.items():
            if not isinstance(arr, list) or len(arr) < 15:
                continue
            # 映射回原始 code；找不到则用纯数字兜底
            orig_code = biz_to_orig.get(biz_code)
            if not orig_code:
                # 兜底: 从 biz_code 提取数字，尝试还原
                digits = re.sub(r"\D", "", biz_code)
                if digits:
                    orig_code = digits
                else:
                    continue
            try:
                last = _float(arr[2])
                if not last or last <= 0:
                    continue  # 无效价格直接跳过，不返回空壳数据
                parsed[orig_code] = {
                    "last": last,
                    "name": arr[1],
                    "change_pct": _float(arr[3]),
                    "change": _float(arr[4]),
                    "volume": round(float(arr[5]) * 100, 2) if _float(arr[5]) else 0,
                    "amount": _float(arr[7]),
                    "turnover_rate": _float(arr[8]),
                    "high": _float(arr[10]),
                    "low": _float(arr[11]),
                    "PE": _float(arr[12]),
                    "prev_close": _float(arr[13]),
                    "open": _float(arr[14]),
                    "time": arr[17] if len(arr) > 17 else None,
                    "symbol": re.sub(r"\D", "", biz_code),
                }
            except (ValueError, TypeError, IndexError):
                continue
        return parsed

    # 分批并行请求（最多15线程）并合并
    all_biz = list(biz_to_orig.keys())
    batches = [all_biz[i:i + _SOHU_BATCH_LIMIT] for i in range(0, len(all_biz), _SOHU_BATCH_LIMIT)]

    if len(batches) <= 1:
        return _parse_batch(batches[0]) if batches else {}

    result: Dict[str, Dict[str, Any]] = {}
    lock = threading.Lock()
    max_workers = min(len(batches), 15)

    def _fetch_and_merge(batch):
        local = _parse_batch(batch)
        if local:
            with lock:
                result.update(local)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_fetch_and_merge, b) for b in batches]
        for f in futures:
            try:
                f.result(timeout=timeout + 5)
            except Exception:
                pass

    return result


# ================================================================
# Provider 注册
# ================================================================

# [并发常量] 最大并发线程数 — Coordinator.allocate_threads() 据此分配 worker。
# ⚠️ 请勿删除或随意修改: 此常量直接影响调度层线程分配，改错会导致请求过载或资源浪费。
# 选值依据: 搜狐HTTP接口，无限流。
# 同步位置: source_config.py max_workers 需与此值保持一致。
MAX_CONCURRENCY = 8

@register(priority=45)
class SohuDataSource:
    """
    搜狐财经数据源 — A股数据源（priority=45）。

    能力:
      - K线: 日/周/月 + 5m/15m/30m/60m 历史分钟线 + 当日1m分时
      - 实时行情: 单只 (-1.html) + 批量 (getqjson)
      - 全市场批量: 并发获取全市场K线

    线程安全性:
      - 纯标准库 HTTP，线程安全
    """

    name = "sohu"
    priority = 45
    max_concurrency = MAX_CONCURRENCY
    min_interval = 0.0
    jitter_min = 0.0
    jitter_max = 0.0

    capabilities = {
        "kline": True,
        "kline_priority": 45,
        "kline_tf": {"1D", "1W", "1M", "1m", "5m", "15m", "30m", "60m"},
        "kline_batch": True,
        "kline_batch_priority": 45,
        "quote": True,
        "quote_priority": 45,
        "batch_quote": True,
        "batch_quote_priority": 45,
        "hk": False,
        "markets": {"CNStock"},
    }

    _SOHU_PERIOD_MAP = {
        "1D": "d",
        "1W": "w",
        "1M": "m",
    }

    # 分钟级周期: 直接走 hq.stock.sohu.com 的独立接口
    _MINUTE_PERIODS = {"5m", "15m", "30m", "60m"}
    # 当日分时
    _INTRADAY_PERIODS = {"1m"}

    def __init__(self):
        pass

    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 200,
        adj: str = "qfq", timeout: int = 10,
        start_date: str = "", end_date: str = "",
    ) -> Dict[str, Any]:
        """获取单只股票K线。支持日/周/月线 + 5m/15m/30m/60m历史分钟线 + 当日1m分时。"""
        # 当日分时 (1m)
        if timeframe in self._INTRADAY_PERIODS:
            data = _fetch_sohu_intraday(code)
            if not data:
                return {}
            if count and len(data) > count:
                data = data[-count:]  # 取最近 count 条
            return {"bars": data, "count": len(data)}

        # 历史分钟K线 (5m/15m/30m/60m)
        if timeframe in self._MINUTE_PERIODS:
            data = _fetch_sohu_minute_kline(code, timeframe, count)
            if not data:
                return {}
            if adj == "qfq":
                data = _apply_fwd_adjust(data, code)
            return {"bars": data, "count": len(data)}

        # 日/周/月线 (原有逻辑)
        period = self._SOHU_PERIOD_MAP.get(timeframe)
        if not period:
            return NotSupportedResult(self.name, "fetch_kline", f"搜狐API不支持 {timeframe}")

        data = _fetch_sohu_kline(code, period, count, start_date, end_date)
        if not data:
            return {}

        # 前复权处理
        if adj == "qfq":
            data = _apply_fwd_adjust(data, code)

        return {"bars": data, "count": len(data)}

    def fetch_ticker(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        """获取单只股票实时行情快照。

        通过 hq.stock.sohu.com/{market}/{code_last3}/{biz_code}-1.html 心跳接口。
        返回: code, name, last, open, high, low, prev_close, change, change_pct,
              volume(股), amount(万), turnover_rate, PE, amplitude, limit_up/down, time
        """
        return _fetch_sohu_ticker(code, timeout=timeout)

    def fetch_batch_quotes(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        """批量获取实时行情快照。

        通过 hqm.stock.sohu.com/getqjson 批量接口（一次请求支持多只）。
        返回: {biz_code: {code, name, last, open, high, low, prev_close, change, change_pct,
                         volume(股), amount(万), turnover_rate, PE, time}}
        """
        return _fetch_sohu_batch_quotes(codes, timeout=timeout)
