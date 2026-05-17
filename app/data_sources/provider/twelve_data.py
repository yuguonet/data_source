# -*- coding: utf-8 -*-
"""
Twelve Data 数据源 Provider — 海外付费兜底源

API来源 & 最新信息:
  - 官网: https://twelvedata.com/
  - K线: api.twelvedata.com/time_series
  - 行情: api.twelvedata.com/quote
  - 需要 API Key（免费套餐可领），配置方式:
    1. provider/config.json 中 twelve_data.api_key
    2. 环境变量 TWELVE_DATA_API_KEY

支持的功能:
  - K线: ✅ 全周期 1m/5m/15m/30m/1H/4H/1D/1W（含4H，国内源没有）
  - fetch_ticker: ✅ 单只实时行情
  - fetch_batch_quotes: ❌ 不支持

  - 港股: ✅ 支持（HKEX）

单位注意（重要）:
  - fetch_kline: volume直接是"股"，不需要×100
  - 价格字段直接是"元"，不需要÷
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests

from app.data_sources.provider import register, NotSupportedResult
from app.data_sources.rate_limiter import RateLimiter
from app.utils.logger import get_logger

logger = get_logger(__name__)

_twelvedata_limiter = RateLimiter(min_interval=1.5, jitter_min=0.8, jitter_max=3.0)


def _get_api_key():
    """获取 Twelve Data API Key（config.json > 环境变量）"""
    # 优先从 provider/config.json 读取
    try:
        import json as _json
        _cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
        if os.path.exists(_cfg_path):
            with open(_cfg_path, "r", encoding="utf-8") as f:
                key = (_json.load(f).get("twelve_data") or {}).get("api_key", "")
                if key:
                    return key
    except Exception:
        pass
    # 兜底: 环境变量
    return (os.getenv("TWELVE_DATA_API_KEY") or "").strip()


# Twelve Data interval 参数映射
_TD_INTERVAL_MAP = {
    "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
    "1H": "1h", "4H": "4h",
    "1D": "1day", "1W": "1week",
}

# 重试配置
_MAX_ATTEMPTS = 3
_BACKOFF_BASE_SEC = 1.5
_BACKOFF_CAP_SEC = 12.0
_TRANSIENT_ERR_MARKERS = (
    "remote end closed connection", "connection aborted", "connection reset",
    "timed out", "timeout", "max retries exceeded", "temporarily unavailable",
    "rate", "too many requests", "429",
)


def _is_transient(exc):
    """判断是否为瞬态错误（可重试）"""
    return any(m in str(exc).lower() for m in _TRANSIENT_ERR_MARKERS)


def _td_symbol_and_exchange(code):
    """股票代码 → Twelve Data symbol + exchange"""
    c = (code or "").strip().upper()
    # 港股: HK0001 或 0001.HK
    if c.startswith("HK"):
        num = c[2:]
        if num.isdigit():
            num = str(int(num)).zfill(4)
        return num, "HKEX"
    if c.endswith(".HK"):
        num = c.replace(".HK", "")
        if num.isdigit():
            num = str(int(num)).zfill(4)
        return num, "HKEX"
    # A股
    digits = c.lstrip("SHSZBJ")
    if c.startswith("SH") or digits.startswith(("6", "9")):
        return digits, "SSE"
    if c.startswith("BJ") or digits.startswith(("43", "82", "83", "87", "88")):
        return digits, "BSE"
    return digits, "SZSE"


def _parse_td_kline(values, count, timeframe="1D"):
    """将 Twelve Data API 返回的 K 线数据转换为标准化字典列表

    Twelve Data 返回的是K线开始时间，需转换为结束时间以对齐其他数据源。
    如 15m bar 的 datetime="14:00" → time="14:15"
    """
    from datetime import datetime as _dt, timedelta as _td
    _TF_OFFSET = {
        "1m": _td(minutes=1), "5m": _td(minutes=5), "15m": _td(minutes=15),
        "30m": _td(minutes=30), "1H": _td(hours=1), "4H": _td(hours=4),
    }
    offset = _TF_OFFSET.get(timeframe)
    out = []
    for v in values:
        try:
            dt_str = v.get("datetime", "")
            if not dt_str:
                continue
            # 统一时间格式: 日线 YYYY-MM-DD, 分时线 YYYY-MM-DD HH:MM:00
            if " " in dt_str:
                if offset:
                    # 开始时间 → 结束时间
                    dt_obj = _dt.strptime(dt_str[:16], "%Y-%m-%d %H:%M")
                    ts = (dt_obj + offset).strftime("%Y-%m-%d %H:%M") + ":00"
                else:
                    ts = dt_str[:16] + ":00"
            else:
                ts = dt_str[:10]
            o = float(v["open"])
            h = float(v["high"])
            low = float(v["low"])
            c = float(v["close"])
            vol = float(v.get("volume") or 0)
            if o == 0 and c == 0:
                continue
            out.append({
                "time": ts, "open": round(o, 4), "high": round(h, 4),
                "low": round(low, 4), "close": round(c, 4), "volume": round(vol, 2),
            })
        except (ValueError, TypeError, KeyError):
            continue
    out.sort(key=lambda x: x["time"])
    return out[-count:] if len(out) > count else out


# [并发常量] 最大并发线程数 — Coordinator.allocate_threads() 据此分配 worker。
# ⚠️ 请勿删除或随意修改: 此常量直接影响调度层线程分配，改错会导致请求过载或资源浪费。
# 选值依据: 海外付费API，受套餐QPS限制，限流 min_interval=1.5s。
# 同步位置: source_config.py max_workers 需与此值保持一致。
MAX_CONCURRENCY = 2

@register(priority=100)
class TwelveDataSource:
    """
    Twelve Data 数据源 — 海外付费兜底源（priority=100）。

    能力:
      - K线: 全周期（含 4H，国内源没有）
      - 行情: 单只实时行情
      - 港股: 支持

    注意:
      - 需要 API Key，未配置时自动跳过
      - 免费套餐有频率限制，通过 limiter 控制
    """

    name = "twelvedata"
    priority = 100
    max_concurrency = MAX_CONCURRENCY
    min_interval = 1.5
    jitter_min = 0.8
    jitter_max = 3.0

    capabilities = {
        "kline": True,
        "kline_priority": 100,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "4H", "1D", "1W"},
        "kline_batch": True,
        "quote": True,
        "quote_priority": 100,
        "batch_quote": False,
        "batch_quote_priority": 100,
        "hk": True,
        "markets": {"CNStock", "HKStock"},
    }

    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 300,
        adj: str = "qfq", timeout: int = 15,
        start_date: str = "", end_date: str = "",
    ) -> Dict[str, Any]:
        if start_date:
            from app.data_sources.provider import calc_kline_count
            count = calc_kline_count(timeframe, start_date, end_date)

        api_key = _get_api_key()
        if not api_key:
            logger.debug("[TwelveData] API Key 未配置，跳过")
            return {}

        interval = _TD_INTERVAL_MAP.get(timeframe)
        if not interval:
            return NotSupportedResult(self.name, "fetch_kline", f"不支持 {timeframe} 周期")

        symbol, exchange = _td_symbol_and_exchange(code)
        params = {
            "symbol": symbol, "exchange": exchange, "interval": interval,
            "outputsize": min(int(count), 5000),
            "apikey": api_key, "format": "JSON", "dp": "4",
        }

        for attempt in range(_MAX_ATTEMPTS):
            try:
                _twelvedata_limiter.wait()
                resp = requests.get(
                    "https://api.twelvedata.com/time_series",
                    params=params, timeout=timeout,
                )
                data = resp.json()
                break
            except Exception as e:
                if attempt + 1 < _MAX_ATTEMPTS and _is_transient(e):
                    time.sleep(min(_BACKOFF_CAP_SEC, _BACKOFF_BASE_SEC * (2 ** attempt)))
                    continue
                logger.debug("[TwelveData] K线失败 %s/%s tf=%s: %s", symbol, exchange, timeframe, e)
                return {}
        else:
            return {}

        if data.get("status") != "ok" or "values" not in data:
            msg = data.get("message", "")
            code_err = data.get("code", "")
            if code_err == 429 or "API credits" in msg or "minute limit" in msg:
                logger.warning("[TwelveData] 频率限制 %s/%s: %s", symbol, exchange, msg)
            return {}

        bars = _parse_td_kline(data["values"], count, timeframe)
        return {"bars": bars, "count": len(bars)} if bars else {}

    def fetch_ticker(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        api_key = _get_api_key()
        if not api_key:
            return None

        symbol, exchange = _td_symbol_and_exchange(code)
        _twelvedata_limiter.wait()
        try:
            resp = requests.get(
                "https://api.twelvedata.com/quote",
                params={"symbol": symbol, "exchange": exchange, "apikey": api_key},
                timeout=timeout,
            )
            data = resp.json()
        except Exception as e:
            logger.debug("[TwelveData] 行情失败 %s/%s: %s", symbol, exchange, e)
            return None

        if data.get("status") != "ok":
            return None

        try:
            last = float(data.get("close", 0) or 0)
        except (TypeError, ValueError):
            last = 0
        if last <= 0:
            return None

        try:
            prev = float(data.get("previous_close", 0) or 0)
        except (TypeError, ValueError):
            prev = 0
        chg = round(last - prev, 4) if prev else 0.0

        return {
            "last": last, "change": chg,
            "changePercent": round(chg / prev * 100, 2) if prev else 0.0,
            "high": float(data.get("high", 0) or 0),
            "low": float(data.get("low", 0) or 0),
            "open": float(data.get("open", 0) or 0),
            "previousClose": prev,
            "name": str(data.get("name", "") or ""),
            "symbol": f"{symbol}.{exchange}",
        }

    def fetch_batch_quotes(self, codes: List[str], timeout: int = 10):
        return NotSupportedResult(self.name, "fetch_batch_quotes")
