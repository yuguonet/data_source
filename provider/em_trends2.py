# -*- coding: utf-8 -*-
"""
东方财富 trends2 极速数据源 Provider

API来源 & 最新信息:
  - 浏览器F12抓包 https://quote.eastmoney.com/ 找 trends2 请求
  - 接口: push2.eastmoney.com/api/qt/stock/trends2/get
  - 参数: secid(市场.代码), fields1, fields2, ndays=1
  - 返回当天1分钟K线原始数据，需自行聚合为更大周期
  - JSONP格式: jQuery({...});

支持的功能:
  - K线: ✅ 1m/5m/15m/30m/1H（1m数据聚合，仅当天数据）
  - K线 1D/1W: ❌ 不支持（API只返回当天数据，不够聚合）
  - fetch_ticker: ✅ 用全天1m最新bar的close作为当前价
  - fetch_batch_quotes: ⚠️ 逐只并发调_fetch_em_trends2_quote（非真批量）
  - fetch_market_kline: ✅ 并发获取全市场K线

单位注意（重要）:
  - _em_trends2_raw: volume 返回的是原始值，代码中已×100转"股"
  - 价格字段直接是"元"，不需要÷
  - 复权: 不复权数据通过 TDX 除权除息数据(adjustment模块)转前复权
"""

from __future__ import annotations

import json
import re
import ssl
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

_TZ_CN = timezone(timedelta(hours=8))
from typing import Any, Dict, List, Optional

from app.data_sources.provider import register, NotSupportedResult
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ================================================================
# 基础配置
# ================================================================

TIMEOUT = 10
PER_DOMAIN_CONCURRENT = 30
PER_DOMAIN_INTERVAL = 0.01

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# 极速源用持久 opener（连接复用）
_fast_opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=SSL_CTX))


# ================================================================
# CDN 节点探测 + 锁定
# ================================================================
# 东财 push2 CDN 有多个节点（82~85.push2.eastmoney.com），但真正稳定的少。
# 策略: 启动时探测，锁定最快的节点一直用，失败时自动切换到下一个可用节点。
# 不做轮换 — 增加复杂性且收益不大。

_CDN_CANDIDATES = [
    "84.push2.eastmoney.com",
    "85.push2.eastmoney.com",
    "push2.eastmoney.com",
    "82.push2.eastmoney.com",
    "83.push2.eastmoney.com",
]

_cdn_host: str = "84.push2.eastmoney.com"  # 当前锁定的节点（84/85 支持 HTTP）
_cdn_lock = threading.Lock()
_cdn_discovered = False


def _probe_cdn() -> str:
    """探测 CDN 节点，返回实际 HTTP 请求能拿到数据的最快节点。

    东财 push2 CDN 各节点对 HTTP/HTTPS 支持不一致:
    - 82/83.push2: HTTP 返回空（仅 HTTPS 可用，但被 TLS 指纹封锁）
    - 84/85.push2: HTTP 正常
    - push2 主域: 不稳定
    必须用真实 HTTP 请求验证，不能只测 TCP 连接。
    """
    import socket
    # 先用 TCP 快速过滤不可达节点
    reachable = []
    for host in _CDN_CANDIDATES:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            t0 = time.time()
            s.connect((host, 80))
            latency = time.time() - t0
            s.close()
            reachable.append((host, latency))
        except Exception:
            pass

    # 再用真实 HTTP 请求验证能拿到数据的节点
    test_url_suffix = "/api/qt/stock/trends2/get?cb=jQuery&secid=1.600519&fields1=f1,f2,f3&fields2=f51&iscr=0&ndays=1"
    working = []
    for host, latency in reachable:
        try:
            resp = urllib.request.urlopen(
                urllib.request.Request(
                    f"http://{host}{test_url_suffix}",
                    headers=HEADERS,
                ),
                timeout=3,
            )
            body = resp.read().decode("utf-8", "ignore")
            if "jQuery" in body and '"rc":0' in body:
                working.append((host, latency))
        except Exception:
            pass

    if working:
        working.sort(key=lambda x: x[1])
        return working[0][0]
    # 全部探测失败时返回首选节点
    return "84.push2.eastmoney.com"


def _get_cdn_host() -> str:
    """获取当前锁定的 CDN 节点（首次调用时探测）。"""
    global _cdn_host, _cdn_discovered
    if _cdn_discovered:
        return _cdn_host
    with _cdn_lock:
        if _cdn_discovered:
            return _cdn_host
        _cdn_host = _probe_cdn()
        _cdn_discovered = True
        logger.info("[em_trends2] CDN 节点锁定: %s", _cdn_host)
        return _cdn_host


def _switch_cdn():
    """当前节点失败时，切换到下一个可用节点。"""
    global _cdn_host, _cdn_discovered
    with _cdn_lock:
        try:
            idx = _CDN_CANDIDATES.index(_cdn_host)
            next_idx = (idx + 1) % len(_CDN_CANDIDATES)
        except ValueError:
            next_idx = 0
        _cdn_host = _CDN_CANDIDATES[next_idx]
        _cdn_discovered = True
        logger.warning("[em_trends2] CDN 切换: → %s", _cdn_host)


# ================================================================
# 域名限流
# ================================================================

class _DomainThrottler:
    """线程安全的域名级限流器"""

    def __init__(self, max_c: int = 50, interval: float = 0.01):
        self._sems: Dict[str, threading.Semaphore] = {}
        self._last: Dict[str, float] = {}
        self._max = max_c
        self._interval = interval
        self._lock = threading.Lock()

    def _domain(self, url: str) -> str:
        m = re.search(r'https?://([^/]+)', url)
        return m.group(1) if m else url

    def _sem(self, d: str) -> threading.Semaphore:
        with self._lock:
            if d not in self._sems:
                self._sems[d] = threading.Semaphore(self._max)
            return self._sems[d]

    def acquire(self, url: str):
        d = self._domain(url)
        self._sem(d).acquire()
        wait = 0.0
        with self._lock:
            wait = max(0, self._interval - (time.time() - self._last.get(d, 0)))
            self._last[d] = time.time() + wait
        if wait > 0:
            time.sleep(wait)

    def release(self, url: str):
        self._sem(self._domain(url)).release()


_throttler = _DomainThrottler(PER_DOMAIN_CONCURRENT, PER_DOMAIN_INTERVAL)


# ================================================================
# HTTP 工具
# ================================================================

def _http_get(url: str, headers: dict = None, timeout: int = TIMEOUT) -> Optional[str]:
    h = {**HEADERS, **(headers or {})}
    _throttler.acquire(url)
    try:
        req = urllib.request.Request(url, headers=h)
        with _fast_opener.open(req, timeout=timeout) as resp:
            raw = resp.read()
            for enc in ("utf-8", "gbk", "gb2312", "latin-1"):
                try:
                    return raw.decode(enc)
                except UnicodeDecodeError:
                    continue
            return raw.decode("utf-8", errors="ignore")
    except Exception:
        return None
    finally:
        _throttler.release(url)


def _http_get_json(url: str, headers: dict = None, timeout: int = TIMEOUT) -> Optional[dict]:
    t = _http_get(url, headers, timeout)
    if not t:
        return None
    try:
        m = re.search(r'[=(]\s*(\{[\s\S]*\})\s*[);]*$', t)
        if m:
            return json.loads(m.group(1))
        return json.loads(t)
    except Exception:
        return None


# ================================================================
# 代码工具
# ================================================================

def _normalize(code: str) -> str:
    """标准化为 sh/sz/bj 前缀 + 6位数字"""
    c = code.strip().upper().replace(".", "").replace("SH", "").replace("SZ", "").replace("BJ", "")
    if c.startswith("6"):
        return f"sh{c}"
    elif c.startswith(("0", "3")):
        return f"sz{c}"
    elif c.startswith(("8", "4")):
        return f"bj{c}"
    return c


def _to_em(code: str) -> str:
    """转东财 secid 格式: 1.600519 / 0.000001"""
    nc = _normalize(code)
    return f"1.{nc[2:]}" if nc.startswith("sh") else f"0.{nc[2:]}"


def _cn(code: str) -> str:
    """提取纯数字代码"""
    return _normalize(code)[2:]


def _k(t, o, h, l, c, v, a=0) -> Dict[str, Any]:
    """构建标准化K线字典"""
    return {
        "time": str(t), "open": float(o), "high": float(h),
        "low": float(l), "close": float(c),
        "volume": float(v), "amount": float(a),
    }


BAR_LIMIT = 64


def _last_n_bars(klines: list, n: int = BAR_LIMIT) -> Optional[list]:
    return klines[-n:] if klines and len(klines) > 0 else None


# ================================================================
# 前复权（共享模块）
# ================================================================
from app.data_sources.provider.adjustment import apply_fwd_adjust


# ================================================================
# 核心数据获取
# ================================================================

def _em_trends2_raw(code: str) -> Optional[list]:
    """push2.eastmoney.com trends2: 获取今天1分钟原始数据（JSONP）"""
    secid = _to_em(code)
    try:
        host = _get_cdn_host()
        # ⚠️ 必须用 HTTP: push2.eastmoney.com 的 HTTPS 对非浏览器 TLS 指纹返回空响应
        url = (
            f"http://{host}/api/qt/stock/trends2/get?"
            f"cb=jQuery&secid={secid}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13"
            f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58&iscr=0&ndays=1"
        )
        req = urllib.request.Request(url, headers=HEADERS)
        with _fast_opener.open(req, timeout=TIMEOUT) as resp:
            raw = resp.read().decode("utf-8", "ignore")
        # JSONP: jQuery({...}); → 提取 JSON
        m = re.search(r'[=(]\s*(\{[\s\S]*\})\s*[);]*$', raw)
        if m:
            d = json.loads(m.group(1))
        else:
            d = json.loads(raw)
        trends = (d.get("data") or {}).get("trends") or []
        if not trends:
            return None

        bars = []
        for t in trends:
            p = t.split(",")
            if len(p) < 7:
                continue
            bars.append({
                "time": p[0], "open": float(p[1]), "close": float(p[2]),
                "high": float(p[3]), "low": float(p[4]),
                "volume": float(p[5]) * 100, "amount": float(p[6]),
            })
        return bars if bars else None
    except Exception:
        _switch_cdn()  # 请求失败，切换到下一个节点
        return None


# 聚合周期映射: timeframe → 每根bar包含的1min bar数
_EM_AGG_STEPS = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1H": 60,
}


def _aggregate_bars(raw_bars: list, timeframe: str) -> Optional[list]:
    """将1分钟原始数据聚合为指定周期的K线。

    支持: 1m(不聚合), 5m, 15m, 30m, 1H。
    1D 不支持（API只返回当天数据，不够聚合出日线）。
    """
    step = _EM_AGG_STEPS.get(timeframe)
    if step is None:
        return None  # 不支持的周期（如 1D）

    if step == 1:
        return raw_bars  # 1m 直接返回

    result = []
    for i in range(0, len(raw_bars) - step + 1, step):
        chunk = raw_bars[i:i + step]
        # bar结束时间 = 最后一根1min bar的时间 + 1分钟
        last_t = chunk[-1]["time"]
        try:
            _dt = datetime.strptime(str(last_t)[:16], "%Y-%m-%d %H:%M")
            end_t = (_dt + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")
        except (ValueError, OverflowError, TypeError):
            end_t = last_t
        result.append(_k(
            end_t,
            chunk[0]["open"],
            max(b["high"] for b in chunk),
            min(b["low"] for b in chunk),
            chunk[-1]["close"],
            sum(b["volume"] for b in chunk),
            sum(b["amount"] for b in chunk),
        ))
    return result if result else None


def _em_trends2_kline(code: str, timeframe: str = "15m", limit: int = 200) -> Optional[list]:
    """获取单只股票K线数据，支持 1m/5m/15m/30m/1H。

    流程: 获取全天1min数据 → 聚合为目标周期 → 截取 limit 条。
    """
    raw = _em_trends2_raw(code)
    if not raw:
        return None
    return _aggregate_bars(raw, timeframe)


# ================================================================
# 实时行情 — 用当天1min数据最新bar的close作为当前价
# ================================================================

def _fetch_em_trends2_quote(code: str) -> Optional[Dict[str, Any]]:
    """获取单只股票实时行情 — 从全天1min数据提取最新bar"""
    raw = _em_trends2_raw(code)
    if not raw:
        return None

    last_bar = raw[-1]
    last = float(last_bar.get("close", 0) or 0)
    if last <= 0:
        return None

    highs = [float(b.get("high", 0)) for b in raw if float(b.get("high", 0)) > 0]
    lows = [float(b.get("low", 0)) for b in raw if float(b.get("low", 0)) > 0]
    open_p = float(raw[0].get("open", 0) or last)
    vol = sum(float(b.get("volume", 0) or 0) for b in raw)

    return {
        "last": last,
        "close": last,
        "change": 0,
        "changePercent": 0,
        "high": max(highs) if highs else last,
        "low": min(lows) if lows else last,
        "open": open_p,
        "previousClose": 0,
        "volume": vol,
        "amount": 0,
        "time": "",
        "name": "",
        "symbol": code,
    }


# ================================================================
# Provider 注册
# ================================================================

# [并发常量] 最大并发线程数 — Coordinator.allocate_threads() 据此分配 worker。
# ⚠️ 请勿删除或随意修改: 此常量直接影响调度层线程分配，改错会导致请求过载或资源浪费。
# 选值依据: trends2极速API，响应极快，15并发可充分利用。
# 同步位置: source_config.py max_workers 需与此值保持一致。
MAX_CONCURRENCY = 15

@register(priority=5)
class EmTrends2DataSource:
    """
    东方财富 trends2 极速数据源 — 最快的A股免费源（priority=5）。

    能力:
      - K线: 1m/5m/15m/30m/1H（1min数据聚合），今天的数据
      - 不支持 1D（API只返回当天数据）
      - 行情: 用当天1min数据最新bar作为实时行情
      - 全市场批量: 并发获取全市场K线（30线程）
      - 不支持批量行情接口

    线程安全性:
      - 使用域名限流器控制并发
      - TDX 连接池线程本地
    """

    name = "em_trends2"
    priority = 5
    max_concurrency = MAX_CONCURRENCY
    min_interval = 0.0
    jitter_min = 0.0
    jitter_max = 0.0

    capabilities = {
        "kline": True,
        "kline_priority": 5,
        "kline_tf": {"1m", "5m", "15m", "30m", "1H"},
        "kline_batch": True,
        "kline_batch_priority": 5,
        "quote": True,
        "quote_priority": 5,
        "batch_quote": True,
        "batch_quote_priority": 5,
        "hk": False,
        "markets": {"CNStock"},
    }

    def __init__(self):
        pass

    def fetch_kline(
        self, code: str, timeframe: str = "15m", count: int = 200,
        adj: str = "qfq", timeout: int = 10,
        start_date: str = "", end_date: str = "",
    ) -> List[Dict[str, Any]]:
        """
        获取单只股票K线，支持 1m/5m/15m/30m/1H。
        数据来源: 全天1min数据聚合。
        不支持 1D（API只返回当天数据）。
        不复权数据通过 TDX 除权除息数据转前复权。
        """
        if timeframe not in _EM_AGG_STEPS:
            return NotSupportedResult(self.name, "fetch_kline", f"不支持 {timeframe} 周期")

        data = _em_trends2_kline(code, timeframe, count)
        if not data:
            return []

        # 统一时间格式: "YYYY-MM-DD HH:MM" → "YYYY-MM-DD HH:MM:00"
        result = []
        for bar in data:
            try:
                ts_str = str(bar.get("time", ""))
                # 统一为 YYYY-MM-DD HH:MM:00 字符串格式
                if "-" in ts_str and ":" in ts_str:
                    ts = ts_str[:16] + ":00"
                else:
                    ts = ts_str
                result.append({
                    "time": ts,
                    "open": round(float(bar["open"]), 4),
                    "high": round(float(bar["high"]), 4),
                    "low": round(float(bar["low"]), 4),
                    "close": round(float(bar["close"]), 4),
                    "volume": round(float(bar["volume"]), 2),
                })
            except (ValueError, TypeError, KeyError):
                continue

        # 前复权处理
        if adj == "qfq" and result:
            result = apply_fwd_adjust(result, code)

        return result[-count:] if len(result) > count else result

    def fetch_ticker(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        """获取单只股票实时行情 — 用当天1min数据最新bar的close作为当前价"""
        return _fetch_em_trends2_quote(code)

    def fetch_batch_quotes(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        """批量实时行情 — 并发直接调 _fetch_em_trends2_quote"""
        result: Dict[str, Dict[str, Any]] = {}
        lock = threading.Lock()

        def _fetch(code):
            q = _fetch_em_trends2_quote(code)
            if q:
                # key 统一用纯数字代码（去掉 sh/sz 前缀）
                digits = code.strip().upper().replace("SH", "").replace("SZ", "").replace("BJ", "")
                with lock:
                    result[digits] = q

        max_workers = min(len(codes), 30)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futs = [pool.submit(_fetch, c) for c in codes]
            for f in futs:
                try:
                    f.result()
                except Exception:
                    pass

        return result

    def _get_stock_list(self) -> list:
        """获取A股股票列表（通过东财 clist API）"""
        try:
            stocks, page = [], 1
            while True:
                host = _get_cdn_host()
                # ⚠️ 必须用 HTTP（同 _em_trends2_raw 原因）
                data = _http_get_json(
                    f"http://{host}/api/qt/clist/get?pn={page}&pz=5000&po=1&np=1"
                    f"&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3"
                    f"&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14,f13"
                )
                if not data:
                    break
                items = (data.get("data") or {}).get("diff") or []
                if not items:
                    break
                for i in items:
                    c, n, m = i.get("f12", ""), i.get("f14", ""), i.get("f13", 0)
                    if c:
                        stocks.append({"code": f"{'sh' if m == 1 else 'sz'}{c}", "name": n})
                if len(stocks) >= ((data.get("data") or {}).get("total", 0)):
                    break
                page += 1
            return stocks
        except Exception as e:
            logger.error("[EmTrends2] 获取股票列表失败: %s", e)
            return []
