# -*- coding: utf-8 -*-
"""
除权除息因子模块 — 独立模块，不依赖项目其他文件，仅支持不复权

因子来源: 新浪财经 qfq.js / hfq.js
  - qfq (前复权): fwd_price = unadj_price / qfq_factor
  - hfq (后复权): hfq_price = unadj_price * hfq_factor

对外暴露:
  - fetch_qfq_factors(code) — 获取前复权因子 [(date, factor), ...]
  - reverse_fwd_adjust(klines, code) — 将前复权K线还原为不复权

缓存: 内存缓存，进程生命周期有效
"""

from __future__ import annotations

import json
import re
import ssl as _ssl
import threading
import urllib.request as _urllib
from typing import Dict, List, Optional, Tuple

# ================================================================
# HTTP 工具
# ================================================================

_SSL_CTX = _ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = _ssl.CERT_NONE

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
}

def _http_get(url: str, timeout: int = 6) -> Optional[str]:
    try:
        req = _urllib.Request(url, headers=_HEADERS)
        with _urllib.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None


# ================================================================
# 代码转换
# ================================================================

def _to_sina_code(code: str) -> Optional[str]:
    """将任意格式股票代码转为新浪格式 (sz000001 / sh600519)。"""
    c = code.strip().upper().replace(".", "").replace("SH", "").replace("SZ", "").replace("BJ", "")
    if not c.isdigit() or len(c) != 6:
        return None
    prefix = code.strip()[:2].upper()
    if prefix == "SH":
        return "sh" + c
    elif prefix == "SZ":
        return "sz" + c
    elif prefix == "BJ":
        return "bj" + c
    # 无前缀时按号段推断
    if c.startswith(("6", "9")):
        return "sh" + c
    return "sz" + c


# ================================================================
# 因子获取
# ================================================================

_qfq_cache: Dict[str, List[Tuple[str, float]]] = {}
_cache_lock = threading.Lock()


def _parse_sina_factor(text: str, var_suffix: str) -> Optional[List[Tuple[str, float]]]:
    """解析新浪因子 JS 返回。

    格式: var sz301128qfq={"total":N,"data":[{"d":"2026-05-19","f":"1.0000000000000000"},...]}
    """
    m = re.search(r'"total":\s*(\d+),\s*"data":\s*\[(.*?)\]', text)
    if not m:
        return None
    items = re.findall(r'\{"d":"([\d-]+)",\s*"f":"([\d.]+)"\}', m.group(2))
    if not items:
        return None
    factors = [(d, float(f)) for d, f in items if d > "1900-01-01"]
    return factors if factors else None


def fetch_qfq_factors(code: str) -> Optional[List[Tuple[str, float]]]:
    """获取前复权因子 (从新浪 qfq.js)。

    因子含义:
      fwd_price = unadj_price / qfq_factor
      unadj_price = fwd_price * qfq_factor
      最新除权日 factor=1.0，越早的日期 factor 越大。

    Returns:
        [(date_str, factor), ...] 按日期降序，失败返回 None
    """
    sina_code = _to_sina_code(code)
    if not sina_code:
        return None

    with _cache_lock:
        if sina_code in _qfq_cache:
            return _qfq_cache[sina_code]

    text = _http_get(f"https://finance.sina.com.cn/realstock/company/{sina_code}/qfq.js")
    if not text:
        return None

    factors = _parse_sina_factor(text, "qfq")
    if factors:
        with _cache_lock:
            _qfq_cache[sina_code] = factors
    return factors




# ================================================================
# 因子查找
# ================================================================

def _find_factor(sorted_dates: List[str], factor_map: Dict[str, float],
                 bar_date: str, latest_ex: Optional[str]) -> float:
    """查找 bar_date 对应的因子。

    - bar_date >= latest_ex → 返回 1.0 (无除权，无需调整)
    - bar_date < latest_ex  → 返回 <= bar_date 的最大日期的因子
    """
    if latest_ex and bar_date >= latest_ex:
        return 1.0
    for d in reversed(sorted_dates):
        if d <= bar_date:
            return factor_map[d]
    return 1.0


def _build_factor_lookup(factors: Optional[List[Tuple[str, float]]]):
    """构建因子查找结构。"""
    if not factors:
        return None, None, None
    factor_map = {d: f for d, f in factors}
    sorted_dates = sorted(factor_map.keys())
    latest_ex = sorted_dates[-1] if sorted_dates else None
    return factor_map, sorted_dates, latest_ex


# ================================================================
# K线时间提取
# ================================================================

def _extract_date(bar_time) -> str:
    """从 bar['time'] 提取 YYYY-MM-DD。"""
    t = str(bar_time or "")
    return t[:10]


# ================================================================
# 复权计算
# ================================================================





def reverse_fwd_adjust(klines: list, code: str) -> list:
    """将前复权K线还原为不复权。

    公式: unadj_price = fwd_price * qfq_factor

    Args:
        klines: 前复权K线列表
        code:   股票代码

    Returns:
        不复权K线列表（新列表），无因子时返回原列表
    """
    if not klines:
        return klines

    factors = fetch_qfq_factors(code)
    factor_map, sorted_dates, latest_ex = _build_factor_lookup(factors)
    if not factor_map:
        return klines

    result = []
    for bar in klines:
        bar_date = _extract_date(bar.get("time", ""))
        factor = _find_factor(sorted_dates, factor_map, bar_date, latest_ex)

        if factor != 1.0:
            result.append({
                "time": bar["time"],
                "open": round(bar["open"] * factor, 4),
                "high": round(bar["high"] * factor, 4),
                "low": round(bar["low"] * factor, 4),
                "close": round(bar["close"] * factor, 4),
                "volume": bar["volume"],
            })
        else:
            result.append(bar)
    return result
