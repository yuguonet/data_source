# -*- coding: utf-8 -*-
"""
雪球数据源 Provider

API来源 & 最新信息:
  - 浏览器F12抓包 https://xueqiu.com/ 观察请求
  - K线: stock.xueqiu.com/v5/stock/chart/kline.json
    参数: symbol(SH600519), period(day/week/1/5/15/30/60), type=before(前复权), count(-200)
  - 行情: stock.xueqiu.com/v5/stock/quote.json?symbol=SH600519&extend=detail
  - 需要cookie: 先访问 xueqiu.com 首页获取，TTL=1小时
  - prepare()方法会预热cookie，失败则该源不可用
  - cookie失效时自动清除缓存重试一次

支持的功能:
  - K线: ✅ 全周期 1m/5m/15m/30m/1H/1D/1W（原生前复权）
  - fetch_ticker: ✅ 单只实时行情（quote.json）
  - fetch_batch_quotes: ❌ 不支持（返回NotSupportedResult）


单位注意（重要）:
  - fetch_kline: r[1]=volume(股), 不需要×100
  - fetch_ticker: quote.volume(股), 不需要×100
  - 价格字段直接是"元"，不需要÷
  - 数据原生前复权(type=before)，不需要额外复权处理
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from app.data_sources.normalizer import normalize_cn_code
from app.data_sources.rate_limiter import get_request_headers, RateLimiter
from app.data_sources.provider import register, NotSupportedResult
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ================================================================
# 限流器
# ================================================================

_xueqiu_limiter = RateLimiter(
    min_interval=0.5,
    jitter_min=0.2,
    jitter_max=0.8,
)

# ================================================================
# Cookie 管理
# ================================================================

_cookie_lock = threading.Lock()
_cookie: Optional[str] = None
_cookie_ts: float = 0
COOKIE_TTL = 3600  # 1小时刷新


def _refresh_cookie() -> Optional[str]:
    """访问雪球首页获取 cookie"""
    global _cookie, _cookie_ts
    with _cookie_lock:
        if _cookie and time.time() - _cookie_ts < COOKIE_TTL:
            return _cookie
    try:
        resp = requests.get("https://xueqiu.com/", timeout=5, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        })
        cookies = resp.cookies.get_dict()
        cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
        with _cookie_lock:
            _cookie = cookie_str
            _cookie_ts = time.time()
        return cookie_str
    except Exception as e:
        logger.warning("[雪球] 获取 cookie 失败: %s", e)
        return None


def _invalidate_cookie():
    """清除缓存的 cookie，下次请求强制重新获取"""
    global _cookie, _cookie_ts
    with _cookie_lock:
        _cookie = None
        _cookie_ts = 0


def _load_config_token() -> str:
    """从 provider/config.json 读取雪球 token，返回 cookie 字符串"""
    try:
        import json as _json
        _cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
        if os.path.exists(_cfg_path):
            with open(_cfg_path, "r", encoding="utf-8") as f:
                xq = (_json.load(f).get("xueqiu") or {})
            token = xq.get("xq_a_token", "")
            uid = xq.get("u", "")
            if token:
                parts = [f"xq_a_token={token}"]
                if uid:
                    parts.append(f"u={uid}")
                return "; ".join(parts)
    except Exception:
        pass
    return ""


def _get_headers() -> dict:
    """获取带 cookie 的请求头，优先使用 config.json 中的 token"""
    # 优先用配置文件中的 token
    config_cookie = _load_config_token()
    # 兜底: 自动获取 cookie
    auto_cookie = _refresh_cookie()
    if not auto_cookie:
        _invalidate_cookie()
        auto_cookie = _refresh_cookie()
    # 合并: config token 在前
    parts = []
    if config_cookie:
        parts.append(config_cookie)
    if auto_cookie:
        parts.append(auto_cookie)
    cookie = "; ".join(parts) if parts else ""
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://xueqiu.com/",
        "Cookie": cookie,
    }


# ================================================================
# 代码转换
# ================================================================

def _to_xueqiu_symbol(code: str) -> str:
    """股票代码 → 雪球格式: SH600519 / SZ000001"""
    nc = normalize_cn_code(code)
    if not nc:
        return ""
    # normalize_cn_code 返回 sh600519 格式
    prefix = nc[:2].upper()
    digits = nc[2:]
    return f"{prefix}{digits}"


# ================================================================
# 数据获取
# ================================================================

# 雪球 API period 参数映射
# 分钟级: "1"=1m, "5"=5m, "15"=15m, "30"=30m, "60"=1H
# 日线级: "day"=1D, "week"=1W, "month"=1M
_XQ_TF_TO_PERIOD = {
    "1m": "1",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1H": "60",
    "1D": "day",
    "1W": "week",
}


def _fetch_xueqiu_kline(code: str, timeframe: str = "15m", limit: int = 200, adj: str = "") -> Optional[List[Dict[str, Any]]]:
    """获取单只股票K线数据，支持多周期。

    支持的周期: 1m, 5m, 15m, 30m, 1H, 1D, 1W
    雪球 API 原生支持这些周期，无需额外聚合。

    adj 复权方式映射:
      ""   → "normal" (不复权)
      "qfq" → "before" (前复权)
      "hfq" → "after"  (后复权)
    """
    symbol = _to_xueqiu_symbol(code)
    if not symbol:
        return None

    period = _XQ_TF_TO_PERIOD.get(timeframe)
    if not period:
        return None  # 不支持的周期

    # adj → 雪球 type 参数
    _adj_map = {"": "normal", "qfq": "before", "hfq": "after"}
    xq_type = _adj_map.get(adj, "normal")

    try:
        url = "https://stock.xueqiu.com/v5/stock/chart/kline.json"
        params = {
            "symbol": symbol,
            "begin": int(time.time() * 1000),
            "period": period,
            "type": xq_type,
            "count": f"-{limit}",
            "indicator": "kline",
        }
        resp = requests.get(url, params=params, headers=_get_headers(), timeout=10)
        data = resp.json()

        items = (data.get("data") or {}).get("item") or []
        if not items:
            return None

        # 雪球返回: [timestamp, volume, open, high, low, close, ...]
        # 日线/周线只保留日期，分钟线保留完整时间
        _daily_tfs = {"1D", "1W", "1M"}
        # 计算分钟级周期对应的分钟数（用于统一为K线结束时间）
        _tf_minutes = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1H": 60}
        _add_min = _tf_minutes.get(timeframe, 0) if timeframe not in _daily_tfs else 0
        result = []
        for r in items:
            if len(r) < 6:
                continue
            try:
                dt = datetime.fromtimestamp(int(r[0]) / 1000)  # 毫秒 → datetime
                # 统一时间格式: 日线以上 YYYY-MM-DD, 分时线 YYYY-MM-DD HH:MM:00
                if timeframe in _daily_tfs:
                    ts = dt.strftime("%Y-%m-%d")
                else:
                    dt_end = dt + timedelta(minutes=_add_min)
                    ts = dt_end.strftime("%Y-%m-%d %H:%M") + ":00"
                result.append({
                    "time": ts,
                    "open": round(float(r[2]), 4),
                    "high": round(float(r[3]), 4),
                    "low": round(float(r[4]), 4),
                    "close": round(float(r[5]), 4),
                    "volume": round(float(r[1]), 2),
                })
            except (ValueError, TypeError, IndexError):
                continue

        return result if result else None
    except Exception as e:
        logger.debug("[雪球] fetch_kline %s %s 失败: %s", code, timeframe, e)
        return None


def _fetch_ticker(code: str) -> Optional[Dict[str, Any]]:
    """获取单只股票实时行情"""
    symbol = _to_xueqiu_symbol(code)
    if not symbol:
        return None

    _xueqiu_limiter.wait()
    try:
        url = "https://stock.xueqiu.com/v5/stock/quote.json"
        params = {"symbol": symbol, "extend": "detail"}
        resp = requests.get(url, params=params, headers=_get_headers(), timeout=8)
        data = resp.json()

        quote = (data.get("data") or {}).get("quote") or {}
        if not quote:
            return None

        last = float(quote.get("current", 0) or 0)
        prev = float(quote.get("last_close", 0) or 0)
        chg = round(last - prev, 4) if prev else 0
        vol = float(quote.get("volume", 0) or 0)

        return {
            "last": last,
            "change": chg,
            "changePercent": round(chg / prev * 100, 2) if prev else 0,
            "high": float(quote.get("high", 0) or last),
            "low": float(quote.get("low", 0) or last),
            "open": float(quote.get("open", 0) or last),
            "previousClose": prev,
            "volume": vol,
            "time": "",
            "name": quote.get("name", ""),
            "symbol": symbol,
        }
    except Exception as e:
        logger.debug("[雪球] fetch_ticker %s 失败: %s", code, e)
        return None


# ================================================================
# Provider 注册
# ================================================================

# [并发常量] 最大并发线程数 — Coordinator.allocate_threads() 据此分配 worker。
# ⚠️ 请勿删除或随意修改: 此常量直接影响调度层线程分配，改错会导致请求过载或资源浪费。
# 选值依据: 需cookie(TTL=1h)，限流 min_interval=0.5s。
# 同步位置: source_config.py max_workers 需与此值保持一致。
MAX_CONCURRENCY = 8

@register(priority=40)
class XueqiuDataSource:
    """
    雪球数据源 — A股数据源（priority=40）。

    能力:
      - K线: 15m（前复权），通过 chart/kline.json API
      - 行情: 单只实时行情（quote.json）
      - 全市场批量: 并发获取全市场K线

    线程安全性:
      - 使用限流器控制并发
      - Cookie 线程安全刷新
    """

    name = "xueqiu"
    priority = 40
    max_concurrency = MAX_CONCURRENCY
    min_interval = 0.5
    jitter_min = 0.2
    jitter_max = 0.8

    capabilities = {
        "kline": True,
        "kline_priority": 40,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D", "1W"},
        "kline_batch": True,
        "kline_batch_priority": 40,
        "quote": True,
        "quote_priority": 40,
        "batch_quote": False,
        "hk": False,
        "markets": {"CNStock"},
    }

    def __init__(self):
        """初始化: 预热 cookie"""
        try:
            _refresh_cookie()
        except Exception:
            pass

    def prepare(self) -> bool:
        """下载前准备: 刷新 cookie，失败则不可用"""
        try:
            cookie = _refresh_cookie()
            if not cookie:
                _invalidate_cookie()
                cookie = _refresh_cookie()
            return bool(cookie)
        except Exception:
            return False

    def fetch_kline(
        self, code: str, timeframe: str = "15m", count: int = 200,
        timeout: int = 10,
        start_date: str = "", end_date: str = "",
    ) -> Dict[str, Any]:
        """获取单只股票K线，支持 1m/5m/15m/30m/1H/1D/1W"""
        if timeframe not in _XQ_TF_TO_PERIOD:
            return NotSupportedResult(self.name, "fetch_kline", f"不支持 {timeframe} 周期")

        # 有日期范围时，取大窗口数据再过滤（雪球支持负数 count 往前取）
        fetch_count = count
        if start_date:
            # 雪球 API 用 count=-N 从当前往前取，要取够覆盖 start_date
            from datetime import datetime, timezone, timedelta
            today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
            from app.data_sources.provider import calc_kline_count
            # 算从 start_date 到今天需要多少根
            fetch_count = calc_kline_count(timeframe, start_date, today)
            fetch_count = min(fetch_count + 50, 5000)

        data = _fetch_xueqiu_kline(code, timeframe, fetch_count, adj="")
        if not data:
            return {}

        # 日期过滤
        from app.data_sources.provider import filter_bars_by_date
        if start_date or end_date:
            data = filter_bars_by_date(data, start_date, end_date)

        return {"bars": data, "count": len(data)} if data else {}

    def fetch_ticker(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        """获取单只股票实时行情"""
        return _fetch_ticker(code)

    def fetch_batch_quotes(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        return NotSupportedResult(self.name, "fetch_batch_quotes")
