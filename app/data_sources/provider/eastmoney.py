# -*- coding: utf-8 -*-
"""
东方财富数据源 Provider

API来源 & 最新信息:
  - 浏览器F12抓包 https://quote.eastmoney.com/ 观察请求
  - K线: push2his.eastmoney.com/api/qt/stock/kline/get
  - 单只行情: push2.eastmoney.com/api/qt/stock/get
  - 批量行情: push2.eastmoney.com/api/qt/clist/get
  - 关注f字段编号变化（f43=最新价, f47=成交量, f60=昨收 等）
  - klt参数: 1=1m, 5=5m, 15=15m, 30=30m, 60=1H, 101=1D, 102=1W
  - fqt参数: 0=不复权, 1=前复权, 2=后复权

支持的功能:
  - K线: ✅ 全周期 1m/5m/15m/30m/1H/1D/1W（per-symbol，非原生批量）
  - fetch_ticker: ✅ 单只实时行情
  - fetch_batch_quotes: ✅ 原生全市场批量（clist API 一次6000只）


单位注意（重要）:
  - fetch_ticker: stock/get API 的价格字段(f43/f44/f45/f46/f60)返回"分"，需÷100
  - fetch_ticker: 成交量f47返回"手"，需×100转"股"
  - fetch_batch_quotes: clist API 的价格字段(f2/f15/f16/f17/f18)返回"元"，不需÷
  - fetch_batch_quotes: 成交量f5返回"手"，需×100转"股"
  - fetch_kline: kline API 的OHLC返回"元"，不需÷；volume返回"股"，不需×
"""

from __future__ import annotations

import itertools
import json
import logging
import random
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from curl_cffi import requests as cffi_requests

from app.data_sources.normalizer import to_raw_digits, detect_market
from app.data_sources.rate_limiter import (
    get_request_headers,
)
from app.data_sources.provider import register
from app.utils.logger import get_logger

logger = get_logger(__name__)


def _to_eastmoney_secid(symbol: str) -> str:
    """股票代码 → 东财 secid（沪1.xxx / 深北0.xxx）"""
    market, digits = detect_market(symbol)
    if not market or not digits:
        return ""
    return f"1.{digits}" if market == "SH" else f"0.{digits}"


# ================================================================
# 请求头 — 随机 UA + Referer 轮换池
# ================================================================
# 不做 CDN 节点探测（太慢），靠 Referer 轮换 + 随机 UA 规避反爬。
# 失败交给 Coordinator 熔断器和多源 fallback 处理。

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 OPR/103.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


def _make_headers(referer: str = "https://quote.eastmoney.com/") -> dict:
    """随机 UA + 指定 Referer + 完整浏览器指纹"""
    return {
        "User-Agent": _random_ua(), "Referer": referer,
        "Accept": "*/*", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br", "Connection": "keep-alive",
    }


def _strip_jsonp(text: str) -> str:
    """
    去除 JSONP 回调包装，提取纯 JSON。

    东财 API 可能返回以下格式:
      - 纯 JSON: {"data": {...}}
      - JSONP:   jQuery123456({"data": {...}})
      - JSONP:   callback({"data": {...}})
    """
    text = text.strip()
    if not text:
        return text
    # 匹配 callback({...}) 或 callback([...]) 格式
    m = re.match(r'^[a-zA-Z_$][\w$]*\s*\((.+)\)\s*;?\s*$', text, re.DOTALL)
    if m:
        return m.group(1).strip()
    return text


class _RefererPool:
    """线程安全的 Referer 轮换池"""
    def __init__(self, referers: List[str]):
        self._cycle = itertools.cycle(referers)
        self._lock = threading.Lock()
    def next(self) -> str:
        with self._lock: return next(self._cycle)


_em_referers = _RefererPool([
    # 东财主站
    "https://quote.eastmoney.com/",
    "https://www.eastmoney.com/",
    "https://stock.eastmoney.com/",
    "https://data.eastmoney.com/",
    "https://push2.eastmoney.com/",
    # 东财 push CDN 节点
    "https://82.push2.eastmoney.com/",
    "https://83.push2.eastmoney.com/",
    "https://84.push2.eastmoney.com/",
    "https://85.push2.eastmoney.com/",
    # 东财子站 / 频道
    "https://futures.eastmoney.com/",
    "https://fund.eastmoney.com/",
    "https://bond.eastmoney.com/",
    "https://forex.eastmoney.com/",
    "https://hk.eastmoney.com/",
    "https://guba.eastmoney.com/",
    "https://so.eastmoney.com/",
    "https://newsapi.eastmoney.com/",
    "https://emweb.eastmoney.com/",
    "https://pdf.eastmoney.com/",
    "https://jspdf.eastmoney.com/",
    "https://cx.eastmoney.com/",
    "https://appshare.eastmoney.com/",
    "https://zlcndc.eastmoney.com/",
    "https://choice.eastmoney.com/",
    # 东财行情 push 域名
    "https://push2his.eastmoney.com/",
    # 东财其他产品
    "https://eastmoney.com/",
    "https://caifuhao.eastmoney.com/",
    "https://mp.eastmoney.com/",
    "https://search-api-web.eastmoney.com/",
    # 行情页 Referer（模拟从股票详情页发起请求）
    "https://quote.eastmoney.com/concept/sh600519.html",
    "https://quote.eastmoney.com/concept/sz000001.html",
    "https://quote.eastmoney.com/center/gridlist.html",
    "https://quote.eastmoney.com/center/boardlist.html",
])


# 东财固定域名（不做 CDN 探测，直接用默认域名）
_EM_KLINE_HOST = "push2his.eastmoney.com"
_EM_QUOTE_HOST = "push2.eastmoney.com"

# ================================================================
# curl_cffi 会话 — 绕过东财 TLS 指纹检测 (JA3)
# ================================================================
# 东财 push2 CDN 对 urllib/requests 的 TLS 指纹做了封锁，
# curl_cffi 的 edge101 指纹实测可通过（需少量重试建立连接）。

_EM_IMPERSONATE = "edge101"
_em_session: Optional[cffi_requests.Session] = None
_em_session_lock = threading.Lock()


def _get_em_session() -> cffi_requests.Session:
    """获取/创建东财专用 curl_cffi Session（单例，线程安全）"""
    global _em_session
    if _em_session is not None:
        return _em_session
    with _em_session_lock:
        if _em_session is None:
            _em_session = cffi_requests.Session()
    return _em_session


def _em_get(
    url: str,
    params: Optional[dict] = None,
    referer: str = "https://quote.eastmoney.com/",
    timeout: int = 10,
    retries: int = 3,
    retry_delay: float = 0.5,
) -> Optional[str]:
    """东财专用 GET：curl_cffi edge101 指纹 + Referer 轮换 + 自动重试。

    返回响应文本，失败返回 None。
    """
    session = _get_em_session()
    headers = _make_headers(referer=referer)
    last_err = None
    for attempt in range(retries):
        try:
            resp = session.get(
                url, params=params, headers=headers,
                impersonate=_EM_IMPERSONATE, timeout=timeout,
            )
            if resp.status_code == 200 and resp.text:
                return resp.text
            last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = str(e).split(".")[0][:80]
        if attempt < retries - 1:
            time.sleep(retry_delay * (attempt + 1))
    logger.debug("[东财] %s 请求失败 (%d次重试): %s", url.split("?")[0], retries, last_err)
    return None



# 东财K线周期映射: 内部周期 → 东财 klt 参数
# klt (K Line Type): 1=1分钟, 5=5分钟, ..., 101=日线, 102=周线
_EM_KLT = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1H": 60, "1D": 101, "1W": 102}

# 东财复权类型映射: 内部复权方式 → 东财 fqt 参数
# fqt (Forward/Backward Adjust): 0=不复权, 1=前复权, 2=后复权
_EM_FQT = {"": 0, "qfq": 1, "hfq": 2}


# [并发常量] 最大并发线程数 — Coordinator.allocate_threads() 据此分配 worker。
# ⚠️ 请勿删除或随意修改: 此常量直接影响调度层线程分配，改错会导致请求过载或资源浪费。
# 选值依据: 东财datacenter无限流，6并发稳健。
# 同步位置: source_config.py max_workers 需与此值保持一致。
MAX_CONCURRENCY = 6

@register(priority=30)
class EastMoneyDataSource:
    """
    东方财富数据源 — 国内最稳定的免费数据源之一（priority=30）。

    能力:
      - K线: 全周期（分钟/日/周），通过 kline/get API
      - 行情: 单只实时行情（stock/get API）
      - 批量行情: 全市场行情（clist/get API，一次HTTP最多6000只）
      - 市场数据: 龙虎榜/热度/涨停池/跌停池/炸板池（独立函数）

    线程安全性:
      - 实例方法无状态，线程安全

    API参数说明:
      - secid: 证券ID，格式为 "市场代码.股票代码"（如 "1.600519"）
      - ut: 用户令牌（固定值，东财API要求）
      - fields1: 基础字段（f1=代码, f2=名称, f3=最新价）
      - fields2: K线字段（f51=日期, f52=开盘, f53=收盘, f54=最高, f55=最低, f56=成交量...）
      - klt: K线周期类型
      - fqt: 复权类型
    """

    name = "eastmoney"
    priority = 25
    max_concurrency = MAX_CONCURRENCY
    min_interval = 0.0
    jitter_min = 0.0
    jitter_max = 0.0

    capabilities = {
        "kline": True,
        "kline_priority": 25,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D", "1W"},
        "kline_batch": True,
        "quote": True,
        "quote_priority": 20,
        "batch_quote": True,
        "batch_quote_priority": 5,
        "hk": False,
        "markets": {"CNStock"},
    }

    def fetch_kline(
        self, code: str, timeframe: str = "1D", count: int = 300,
        timeout: int = 10,
        start_date: str = "", end_date: str = "",
    ) -> Dict[str, Any]:
        if start_date:
            from app.data_sources.provider import calc_kline_count
            count = calc_kline_count(timeframe, start_date, end_date)
        from datetime import date as _date
        em_end = end_date.replace("-", "") if end_date else _date.today().strftime("%Y%m%d")
        em_beg = start_date.replace("-", "") if start_date else "19900101"

        secid = _to_eastmoney_secid(code)
        if not secid:
            return {}
        klt = _EM_KLT.get(timeframe)
        if klt is None:
            return {}

        url = f"https://{_EM_KLINE_HOST}/api/qt/stock/kline/get"

        text = _em_get(
            url,
            params={
                "cb": "jQuery",
                "secid": secid,
                "ut": "fa5fd1943c7b386f172d6893dbbd1835",
                "fields1": "f1,f2,f3",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": klt,
                "fqt": 0,  # 0=不复权（固定不复权，不对外暴露复权参数）
                "beg": em_beg,
                "end": em_end,
                "lmt": min(int(count), 5000),
            },
            referer=_em_referers.next(),
            timeout=timeout,
        )
        if not text:
            return {}
        body = _strip_jsonp(text)
        if not body:
            return {}
        try:
            data = json.loads(body)
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}
        klines_data = (data.get("data") or {}).get("klines")
        if not isinstance(klines_data, list):
            return {}

        out = []
        for line in klines_data:
            parts = line.split(",")
            if len(parts) < 7:
                continue
            try:
                dt_str = parts[0].strip()
                if not dt_str:
                    continue
                # 标准化为字符串格式
                ts_str = dt_str
                # 统一时间格式: 日线 YYYY-MM-DD, 分时线 YYYY-MM-DD HH:MM:00
                if len(ts_str) == 16 and " " in ts_str:
                    ts_str = ts_str + ":00"
                o, c, h, low, v = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5])
                if o == 0 and c == 0:
                    continue
                if h > 0 and low > 0 and h < low:
                    h, low = low, h
                out.append({
                    "time": ts_str, "open": round(o, 4), "high": round(h, 4),
                    "low": round(low, 4), "close": round(c, 4), "volume": round(v, 2),
                })
            except (ValueError, TypeError, IndexError):
                continue
        out.sort(key=lambda x: x["time"])
        out = out[-count:] if len(out) > count else out
        return {"bars": out, "count": len(out)} if out else {}

    def fetch_ticker(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        secid = _to_eastmoney_secid(code)
        if not secid:
            return None

        url = f"http://{_EM_QUOTE_HOST}/api/qt/stock/get"

        text = _em_get(
            url,
            params={
                "cb": "jQuery",
                "secid": secid,
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f60,f170,f171",
            },
            referer=_em_referers.next(),
            timeout=timeout,
        )
        if not text:
            return None
        body = _strip_jsonp(text)
        if not body:
            return None
        try:
            data = json.loads(body)
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        d = data.get("data")
        if not isinstance(d, dict):
            return None

        def _f(key: str, default: float = 0.0) -> float:
            v = d.get(key)
            if v is None or v == "-" or v == "":
                return default
            try:
                return float(v)
            except (TypeError, ValueError):
                return default

        last = _f("f43") / 100
        prev = _f("f60") / 100
        if last == 0 and prev == 0:
            return None
        chg = round(last - prev, 4) if prev else 0.0
        vol = _f("f47") * 100  # f47返回"手"，×100转"股"
        now = datetime.now(timezone(timedelta(hours=8)))
        time_str = now.strftime("%Y-%m-%d %H:%M:%S")
        return {
            "last": last,
            "change": chg,
            "changePercent": round(chg / prev * 100, 2) if prev else 0.0,
            "high": _f("f44") / 100,
            "low": _f("f45") / 100,
            "open": _f("f46") / 100,
            "previousClose": prev,
            "volume": vol, "time": time_str,
            "name": str(d.get("f58", "")).strip(),
            "symbol": secid,
        }

    def fetch_batch_quotes(self, codes: List[str], timeout: int = 15) -> Dict[str, Dict[str, Any]]:
        if not codes:
            return {}
        code_set: Dict[str, str] = {}
        for sym in codes:
            raw = to_raw_digits(sym)
            if raw and raw.isdigit() and len(raw) == 6:
                code_set[raw] = sym
        if not code_set:
            return {}

        # ⚠️ 必须用 HTTP: push2.eastmoney.com 的 HTTPS 对非浏览器 TLS 指纹返回空响应
        # curl_cffi edge101 模拟已失效(2025年起东财收紧JA3检测)，HTTP 不受影响
        url = f"http://{_EM_QUOTE_HOST}/api/qt/clist/get"

        try:
            text = _em_get(
                url,
                params={
                    "cb": "jQuery",
                    "pn": 1, "pz": 6000, "po": 1, "np": 1,
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": 2, "invt": 2, "fid": "f3",
                    "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
                    "fields": "f2,f5,f6,f12,f15,f16,f17,f18",
                },
                referer=_em_referers.next(),
                timeout=timeout,
            )
            if not text:
                logger.warning("[东财批量行情] clist 响应为空")
                return {}
            body = _strip_jsonp(text)
            if not body:
                logger.warning("[东财批量行情] clist 响应为空")
                return {}
            data = json.loads(body)
            diff = ((data.get("data") or {}).get("diff")) or []
        except Exception as e:
            logger.warning("[东财批量行情] clist 请求失败: %s", e)
            return {}

        now = datetime.now(timezone(timedelta(hours=8)))
        today_str = now.strftime("%Y-%m-%d %H:%M:%S")
        result: Dict[str, Dict[str, Any]] = {}
        for item in diff:
            code = str(item.get("f12", "")).strip()
            sym = code_set.get(code)
            if not sym:
                continue
            try:
                last = float(item.get("f2", 0))
                if last <= 0:
                    continue
                prev = float(item.get("f18", 0))
                chg = round(last - prev, 4) if prev else 0.0
                vol = float(item.get("f5", 0) or 0) * 100  # f5返回"手"，×100转"股"
                result[sym] = {
                    "last": last,
                    "change": chg,
                    "changePercent": round(chg / prev * 100, 2) if prev else 0.0,
                    "high": round(float(item.get("f15", 0)), 4),
                    "low": round(float(item.get("f16", 0)), 4),
                    "open": round(float(item.get("f17", 0)), 4),
                    "previousClose": prev,
                    "volume": vol,
                    "amount": round(float(item.get("f6", 0) or 0), 2),  # 成交额(元)
                    "time": today_str,
                    "name": "",
                    "symbol": sym,
                }
            except (ValueError, TypeError):
                continue
        return result

