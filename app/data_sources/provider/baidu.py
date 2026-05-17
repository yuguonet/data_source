# -*- coding: utf-8 -*-
"""
百度股市通数据源 Provider

API来源 & 最新信息:
  - 浏览器F12抓包 https://gushitong.baidu.com/ 观察请求
  - K线+行情: finance.pae.baidu.com/vapi/v1/getquotation
    参数: code(纯数字), isStock=true, market_type=ab, ktype(min1/min5/min15/min30/min60/day/week/month)
    注意: 分钟级ktype命名是min{N}不是纯数字（5/15/30/60返回空!）
  - 2026-05更新: 旧接口 /selfselect/getstockquotation 已失效，迁移到 /vapi/v1/getquotation
  - 返回Result[0].newMarketData.marketData，格式: "日期,时间,open,close,volume,high,low,...;"
  - 取最后一条即为实时行情

支持的功能:
  - K线: ✅ 全周期 1m/5m/15m/30m/1H/1D/1W/1M
    分钟级ktype命名: min1/min5/min15/min30/min60（不是纯数字!）
    分钟级约1000条: min1≈5日, min5≈1月, min15≈3月, min30≈6月, min60≈1年
    日/周/月线完整历史（day/week/month）
  - fetch_ticker: ✅ 单只实时行情（取日线最后一条）
  - fetch_batch_quotes: ❌ 不支持（返回NotSupportedResult）


单位注意（重要）:
  - fetch_kline: volume(parts[4])直接是"股"，不需要×100
  - 价格字段直接是"元"，不需要÷
  - 不支持前/后复权（API不返回复权因子）
  - prevClose在parts[11]（可能为空）
"""

from __future__ import annotations

import json
import re
import ssl
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, timezone
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


# ================================================================
# 代码转换
# ================================================================

def _cn(code: str) -> str:
    """提取纯数字代码"""
    c = code.strip().upper().replace(".", "").replace("SH", "").replace("SZ", "").replace("BJ", "")
    return c


# ================================================================
# 数据获取
# ================================================================

def _fetch_baidu_kline(code: str, ktype: str = "day", limit: int = 0) -> Optional[List[Dict[str, Any]]]:
    """获取单只股票K线数据。

    Args:
        code: 股票代码
        ktype: 周期 — day(日线), week(周线), month(月线)
        limit: 返回条数，0表示全部
    """
    cn_code = _cn(code)
    url = (
        f"https://finance.pae.baidu.com/vapi/v1/getquotation?"
        f"all=1&code={cn_code}&isStock=true"
        f"&market_type=ab&newFormat=1"
        f"&group=quotation_kline_ab&ktype={ktype}"
    )
    data = _http_get_json(url)
    if not data:
        return None

    r = data.get("Result") or {}
    if isinstance(r, list):
        r = r[0] if r else {}
    if not isinstance(r, dict):
        return None

    nmd = r.get("newMarketData") or {}
    md = nmd.get("marketData") or ""
    if not md:
        return None

    result = []
    for line in md.strip().split(";"):
        parts = line.split(",")
        if len(parts) < 6:
            continue
        try:
            ts = str(parts[1]).strip()  # time: "2026-05-12" 或 "2026-05-13 14:45"
            # 统一时间格式: 日线 YYYY-MM-DD, 分时线 YYYY-MM-DD HH:MM:00
            if len(ts) == 16 and " " in ts:
                ts = ts + ":00"
            o = float(parts[2])  # open
            c = float(parts[3])  # close
            v = float(parts[4])  # volume
            h = float(parts[5])  # high
            low = float(parts[6]) if len(parts) > 6 else o  # low
            if o == 0 and c == 0:
                continue
            result.append({
                "time": ts, "open": round(o, 4), "high": round(h, 4),
                "low": round(low, 4), "close": round(c, 4), "volume": round(v, 2),
            })
        except (ValueError, TypeError, IndexError):
            continue

    if limit and len(result) > limit:
        return result[-limit:]
    return result


def _fetch_baidu_quote(code: str) -> Optional[Dict[str, Any]]:
    """获取单只股票实时行情"""
    cn_code = _cn(code)
    url = (
        f"https://finance.pae.baidu.com/vapi/v1/getquotation?"
        f"all=1&code={cn_code}&isStock=true"
        f"&market_type=ab&newFormat=1"
        f"&group=quotation_kline_ab&ktype=day"
    )
    data = _http_get_json(url)
    if not data:
        return None

    r = data.get("Result") or {}
    if isinstance(r, list):
        r = r[0] if r else {}
    if not isinstance(r, dict):
        return None

    nmd = r.get("newMarketData") or {}
    md = nmd.get("marketData") or ""
    if not md:
        return None

    # 取最后一条
    last_line = md.strip().rstrip(";").split(";")[-1]
    parts = last_line.split(",")
    if len(parts) < 6:
        return None

    try:
        last = float(parts[3])  # close
        open_p = float(parts[2])  # open
        vol = float(parts[4])  # volume
        high = float(parts[5])  # high
        low = float(parts[6]) if len(parts) > 6 else open_p
    except (ValueError, IndexError):
        return None

    if last <= 0:
        return None

    prev = float(parts[11]) if len(parts) > 11 and parts[11] else 0  # prevClose
    chg = round(last - prev, 4) if prev else 0

    return {
        "last": last,
        "change": chg,
        "changePercent": round(chg / prev * 100, 2) if prev else 0,
        "high": high,
        "low": low,
        "open": open_p,
        "previousClose": prev,
        "volume": vol,
        "time": str(parts[1])[:10] if len(parts) > 1 else "",
        "name": "",
        "symbol": cn_code,
    }


# ================================================================
# Provider 注册
# ================================================================

# [并发常量] 最大并发线程数 — Coordinator.allocate_threads() 据此分配 worker。
# ⚠️ 请勿删除或随意修改: 此常量直接影响调度层线程分配，改错会导致请求过载或资源浪费。
# 选值依据: 百度HTTP接口，无限流。
# 同步位置: source_config.py max_workers 需与此值保持一致。
MAX_CONCURRENCY = 8

@register(priority=50)
class BaiduDataSource:
    """
    百度股市通数据源 — A股数据源（priority=50）。

    能力:
      - K线: 日线/周线/月线（完整历史）
      - 行情: 单只/批量实时行情
      - 全市场批量: 并发获取全市场K线

    API端点:
      /vapi/v1/getquotation（2026-05 更新，旧 /selfselect/getstockquotation 已失效）

    线程安全性:
      - 纯标准库 HTTP，线程安全
    """

    name = "baidu"
    priority = 50
    max_concurrency = MAX_CONCURRENCY
    min_interval = 0.0
    jitter_min = 0.0
    jitter_max = 0.0

    capabilities = {
        "kline": True,
        "kline_priority": 50,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D", "1W", "1M"},
        "kline_batch": True,
        "kline_batch_priority": 50,
        "quote": True,
        "quote_priority": 50,
        "batch_quote": False,
        "hk": False,
        "markets": {"CNStock"},
    }

    _TF_MAP = {
        "1m": "min1",    # 1分钟线（约1000条，最近5个交易日）
        "5m": "min5",    # 5分钟线（约1000条，约1个月）
        "15m": "min15",  # 15分钟线（约1000条，约3个月）
        "30m": "min30",  # 30分钟线（约1000条，约6个月）
        "1H": "min60",   # 60分钟线（约1000条，约1年）
        "1D": "day",
        "1W": "week",
        "1M": "month",
    }

    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 200,
        adj: str = "qfq", timeout: int = 10,
        start_date: str = "", end_date: str = "",
    ) -> Dict[str, Any]:
        """获取单只股票K线。支持 1D/1W/1M。"""
        ktype = self._TF_MAP.get(timeframe)
        if not ktype:
            return NotSupportedResult(self.name, "fetch_kline", f"百度API不支持 {timeframe}，仅支持 {set(self._TF_MAP.keys())}")

        data = _fetch_baidu_kline(code, ktype=ktype, limit=count)
        if not data:
            return {}
        return {"bars": data, "count": len(data)}

    def fetch_ticker(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        """获取单只股票实时行情"""
        return _fetch_baidu_quote(code)

    def fetch_batch_quotes(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        return NotSupportedResult(self.name, "fetch_batch_quotes")
