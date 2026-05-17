# -*- coding: utf-8 -*-
"""
同花顺(10jqka)数据源 Provider

API来源 & 最新信息:
  - 浏览器F12抓包 https://stockpage.10jqka.com/ 观察请求
  - K线（日/周）: d.10jqka.com.cn/v2/line/hs_{code}/{period}/last{count}.js
    period: 01=日线, 10=周线, 20=月线
  - 分时（当天1分钟tick）: d.10jqka.com.cn/v2/time/hs_{code}/{period}.js
    period参数被忽略，始终返回当天1分钟tick数据
    格式: "HHMM,price,cumulative_amount,avg_price,volume;..."
  - 行情: 同K线接口取最后一条 /01/last1.js
  - K线返回JSONP: quotebridge_v2_line_hs_xxx({...})
  - 分时返回JSONP: quotebridge_v2_time_hs_xxx({..,"data":"HHMM,price,...;..."})

时间约定（重要）:
  - /v2/time/ tick时间即为该分钟bar的结束时间（如14:58的tick = 14:57~14:58这根bar）
  - 1m: 直接返回tick时间（结束时间）
  - 聚合为5m/15m/30m/1H时，按时间分组（(m+step-1)//step），bar时间 = 最后一根tick时间（结束时间）
  - 时间格式统一为 "YYYY-MM-DD HH:MM:SS"（tick的HHMM补:00秒）

支持的功能:
  - K线 分钟级: ✅ 1m/5m/15m/30m/1H（当天数据，/v2/time/ 接口 + 聚合）
  - K线 日/周: ✅ 1D/1W（/v2/line/ 接口，历史数据）
  - fetch_ticker: ✅ 单只实时行情（取K线最后一条）
  - fetch_batch_quotes: ❌ 不支持（返回NotSupportedResult）


单位注意（重要）:
  - /v2/line/ K线: volume(parts[5])直接是"股"，价格"元"
  - /v2/time/ 分时: price=现价, cum_amount=累计额, avg_price=均价, volume=成交量(股)
  - fetch_ticker: 无change/changePercent字段（从K线推的，只能返回0）
  - 复权: 不复权数据通过 除权除息数据(adjustment模块)转前复权

溢出修正:
  - /v2/time/ 接口在盘后(15:01~15:30)仍返回收盘价平移+极小成交量的无效tick
  - 午休时段(12:01~13:00)也会返回溢出tick（如13:00与13:01累计额重复）
  - 在 _fetch_ths_time_data 解析时统一过滤: "1201" <= hhmm <= "1300" 或 hhmm > "1500"
  - 聚合函数 _aggregate_tick_bars 保留二次过滤作为安全兜底
"""
from __future__ import annotations
import json, re, threading
from datetime import datetime, timezone, timedelta

_TZ_CN = timezone(timedelta(hours=8))
from typing import Any, Dict, List, Optional
import requests
from app.data_sources.normalizer import to_raw_digits, detect_market
from app.data_sources.rate_limiter import get_request_headers, RateLimiter, get_shared_session
from app.data_sources.provider import register, NotSupportedResult
from app.utils.logger import get_logger
logger = get_logger(__name__)

# 行情限流器 — 仅 fetch_ticker 使用，K线限流已移至 Coordinator
_ths_quote_limiter = RateLimiter(min_interval=0.6, jitter_min=0.2, jitter_max=1.0)

_THS_MARKET = {"SH": 1, "SZ": 0, "BJ": 0}
# 日线/周线走 /v2/line/ 接口（分钟线走 /v2/time/ 专用逻辑，不在此映射）
_THS_PERIOD = {"1D": 1, "1W": 10}

_THS_MIN_TFS = {"1m", "5m", "15m", "30m", "1H"}

def _to_ths_params(code):
    market, digits = detect_market(code)
    # detect_market 无法识别无前缀代码(如 "999999")，尝试加前缀后再检测
    if not market:
        from app.data_sources.normalizer import add_market_prefix
        prefixed = add_market_prefix(code, "CNStock")
        if prefixed != code:
            market, digits = detect_market(prefixed)
    if not market or not digits: return None
    mkt = _THS_MARKET.get(market)
    if mkt is None: return None
    return (mkt, digits)


# ═══════════════ 分时数据 (/v2/time/) ════════════════

_TF_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1H": 60}


def _fetch_ths_time_data(digits: str, timeout: int = 10) -> List[Dict[str, Any]]:
    """从 /v2/time/ 获取当天分时tick数据，返回1分钟bar列表。

    接口: d.10jqka.com.cn/v2/time/hs_{code}/{period}.js
    period参数被忽略，始终返回当天1分钟tick。
    数据格式: "HHMM,price,cumulative_amount,avg_price,volume;..."

    时间说明: tick的HHMM即为该分钟bar的结束时间（如"0931" = 09:30~09:31这根bar）。
    输出格式: "YYYY-MM-DD HH:MM:SS"（HHMM补:00秒）。

    溢出修正: 解析时过滤午休溢出(12:01~13:00)和盘后溢出(>15:00)的无效tick。

    注意: 仅返回当天数据，非交易时段为空。
    """
    url = f"https://d.10jqka.com.cn/v2/time/hs_{digits}/15.js"
    try:
        resp = get_shared_session().get(
            url,
            headers=get_request_headers(referer="https://stockpage.10jqka.com/"),
            timeout=timeout,
        )
        resp.encoding = "utf-8"
        text = resp.text or ""
    except Exception as e:
        logger.warning("[同花顺分时] 请求失败 %s: %s", digits, e)
        return []

    # 提取 "data":"..."
    m = re.search(r'"data"\s*:\s*"([^"]*)"', text)
    if not m or not m.group(1):
        return []

    today = datetime.now(_TZ_CN).strftime("%Y-%m-%d")
    bars: List[Dict[str, Any]] = []

    for seg in m.group(1).split(";"):
        seg = seg.strip()
        if not seg:
            continue
        parts = seg.split(",")
        if len(parts) < 5:
            continue
        try:
            hhmm = parts[0].strip()           # "0930", "1015" ...
            # 溢出修正: 过滤午休(12:01~13:00)和盘后(>15:00)的无效tick
            # 10jqka盘后仍返回收盘价平移+极小成交量的数据，午休边界(13:00)与13:01累计额重复
            if "1201" <= hhmm <= "1300" or hhmm > "1500":
                continue
            price = float(parts[1])            # 现价
            # parts[2] = 累计成交额（不是本bar的）
            # parts[3] = 均价
            vol = float(parts[4])              # 本bar成交量（股）
            if price <= 0 or vol < 0:
                continue
            bars.append({
                "time": f"{today} {hhmm[:2]}:{hhmm[2:]}:00",
                "open": price, "high": price, "low": price, "close": price,
                "volume": vol,
            })
        except (ValueError, TypeError, IndexError):
            continue

    return bars


def _aggregate_tick_bars(tick_bars: List[Dict[str, Any]], timeframe: str) -> List[Dict[str, Any]]:
    """将1分钟tick数据聚合为指定周期K线（按时间分组）。

    tick_bars: _fetch_ths_time_data 返回的列表（open=high=low=close=price）。
    支持: 1m(直返), 5m, 15m, 30m, 1H。

    算法: 过滤午休tick(12:01~13:00)后，按tick时间分组（(m+step-1)//step对齐到结束时间边界），
    bar时间 = 该组最后一根tick时间（结束时间）。

    注: 午休溢出已在 _fetch_ths_time_data 源端过滤，此处为双重保险。
    """
    if not tick_bars:
        return []

    step = _TF_MINUTES.get(timeframe, 1)
    if step <= 1:
        return tick_bars

    # 过滤午休时段(12:00~12:59)的tick，按时间分组
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for bar in tick_bars:
        try:
            dt = datetime.strptime(bar["time"][:19], "%Y-%m-%d %H:%M:%S")
        except (ValueError, OverflowError, TypeError):
            continue
        # 午休溢出: 12:01~13:00的tick无效（源端已过滤，此处为双重保险）
        m = dt.hour * 60 + dt.minute
        if 12 * 60 < m <= 13 * 60:
            continue
        # tick时间是结束时间，用 (m + step - 1) // step 对齐到bar结束时间边界
        # 如30m: 13:01~13:30 → slot=26, 13:31~14:00 → slot=27
        slot = (m + step - 1) // step
        group_key = (bar["time"][:10], slot)
        groups.setdefault(group_key, []).append(bar)

    result: List[Dict[str, Any]] = []
    for key in sorted(groups):
        chunk = groups[key]
        # bar时间 = 最后一根tick时间（结束时间）
        bar_time = chunk[-1]["time"]
        result.append({
            "time": bar_time,
            "open": chunk[0]["open"],
            "high": max(b["high"] for b in chunk),
            "low": min(b["low"] for b in chunk),
            "close": chunk[-1]["close"],
            "volume": sum(b["volume"] for b in chunk),
        })
    return result


# ═══════════════ 前复权（共享模块）═══════════════
from app.data_sources.provider.adjustment import apply_fwd_adjust as _apply_fwd_adjust


# [并发常量] 最大并发线程数 — Coordinator.allocate_threads() 据此分配 worker。
# ⚠️ 请勿删除或随意修改: 此常量直接影响调度层线程分配，改错会导致请求过载或资源浪费。
# 选值依据: 同花顺HTTP接口，限流 min_interval=1.0s。
# 同步位置: source_config.py max_workers 需与此值保持一致。
MAX_CONCURRENCY = 4

@register(priority=25)
class ThsDataSource:
    """同花顺(10jqka)数据源 — HTTP接口，无需额外依赖。"""
    name = "10jqka"; priority = 20
    max_concurrency = MAX_CONCURRENCY
    min_interval = 1.0
    jitter_min = 0.5
    jitter_max = 1.5
    capabilities = {"kline": True, "kline_priority": 20, "kline_tf": {"1m", "5m", "15m", "30m", "1H", "1D", "1W"},
                    "kline_batch": True, "quote": True, "quote_priority": 25,
                    "batch_quote": False, "batch_quote_priority": 30, "hk": False, "markets": {"CNStock"}}

    def fetch_kline(self, code, timeframe="1D", count=300, adj="qfq", timeout=10, start_date="", end_date="") -> Dict[str, Any]:
        if start_date:
            from app.data_sources.provider import calc_kline_count; count = calc_kline_count(timeframe, start_date, end_date)
        params = _to_ths_params(code)
        if not params: return {}
        mkt, digits = params

        # ── 分钟级: /v2/time/ 分时接口 + 聚合 ──
        if timeframe in _THS_MIN_TFS:
            tick_bars = _fetch_ths_time_data(digits, timeout=timeout)
            if not tick_bars: return {}
            bars = _aggregate_tick_bars(tick_bars, timeframe)
            if not bars: return {}
            # 前复权
            if adj in ("qfq", "hfq"):
                bars = _apply_fwd_adjust(bars, code)
            result = bars[-count:] if len(bars) > count else bars
            return {"bars": result, "count": len(result)}

        # ── 日/周/月: /v2/line/ K线接口 ──
        period = _THS_PERIOD.get(timeframe)
        if period is None: return {}
        p_str = str(period).zfill(2) if period < 10 else str(period)
        url = "https://d.10jqka.com.cn/v2/line/hs_{}/{}/last{}.js".format(digits, p_str, min(int(count), 800))
        try:
            resp = get_shared_session().get(url, headers=get_request_headers(referer="https://stockpage.10jqka.com/"), timeout=timeout)
            resp.encoding = "utf-8"; text = resp.text or ""
        except Exception as e: logger.warning("[同花顺K线] 请求失败 %s: %s", code, e); return {}
        m = re.search(r'"data"\s*:\s*"([^"]+)"', text)
        if not m: m = re.search(r'"([^"]*\d{8}[^"]*)"', text)
        if not m: return {}
        raw = m.group(1); out = []
        for seg in raw.split(";"):
            seg = seg.strip()
            if not seg: continue
            parts = seg.split(";")
            if len(parts) < 5: parts = seg.split(",")
            if len(parts) < 5: continue
            try:
                dt_str = parts[0].strip()
                if len(dt_str) == 8 and dt_str.isdigit(): ts = f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]}"
                elif len(dt_str) >= 10: ts = dt_str[:10] if " " not in dt_str else (dt_str[:16] + ":00" if len(dt_str) >= 16 else dt_str[:10])
                else: continue
                o, h, l, c = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4])
                v = float(parts[5]) if len(parts) > 5 else 0
                if o > 10000 and c < 100: o, h, l, c = o/100, h/100, l/100, c/100
                if o == 0 and c == 0: continue
                if c <= 0 or o <= 0: continue  # 跳过极端负数/零价
                out.append({"time": ts, "open": round(o, 4), "high": round(h, 4), "low": round(l, 4), "close": round(c, 4), "volume": round(v, 2)})
            except (ValueError, TypeError, IndexError): continue
        out.sort(key=lambda x: x["time"])
        if len(out) > count:
            out = out[-count:]
        if adj in ("qfq", "hfq"):
            out = _apply_fwd_adjust(out, code)
        return {"bars": out, "count": len(out)} if out else {}

    def fetch_ticker(self, code, timeout=8):
        params = _to_ths_params(code)
        if not params: return None
        mkt, digits = params
        _ths_quote_limiter.wait()
        try:
            resp = get_shared_session().get("https://d.10jqka.com.cn/v2/line/hs_{}/01/last1.js".format(digits),
                headers=get_request_headers(referer="https://stockpage.10jqka.com/"), timeout=timeout)
            resp.encoding = "utf-8"; text = resp.text or ""
        except Exception as e: logger.warning("[同花顺行情] 请求失败 %s: %s", code, e); return None
        # 解析 JSONP: quotebridge_v2_line_hs_xxx_01_last1({...})
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if not m: return None
        try: data = json.loads(m.group())
        except (json.JSONDecodeError, ValueError): return None
        # v2/line 返回 data 字段: "date,open,high,low,close,volume,amount,..."
        raw = str(data.get("data", ""))
        if not raw: return None
        last_seg = raw.rstrip(";").split(";")[-1]
        parts = last_seg.split(",")
        if len(parts) < 5: return None
        try:
            last = float(parts[4])  # close
            open_p = float(parts[1])
            high = float(parts[2])
            low = float(parts[3])
            vol = float(parts[5]) if len(parts) > 5 else 0
        except (ValueError, IndexError): return None
        if last <= 0: return None
        return {"last": last, "change": 0, "changePercent": 0,
                "high": high, "low": low, "open": open_p, "previousClose": 0,
                "volume": vol, "name": data.get("name", ""), "symbol": f"{digits}"}

    def fetch_batch_quotes(self, codes, timeout=10):
        return NotSupportedResult(self.name, "fetch_batch_quotes")
