# -*- coding: utf-8 -*-
"""
前复权共享模块 — 统一 TDX 除权除息数据获取与前复权因子计算

使用此模块的 Provider:
  tdx_ex, sina, em_trends2, sohu, 10jqka（通过 from adjustment import apply_fwd_adjust）
  注意: xueqiu 不需要（API原生返回前复权数据），baidu 不支持复权

API来源:
  - 除权除息数据来自 pytdx 的 get_xdxr_info(market, symbol)
  - 需要 pytdx 库 + 可用TDX服务器（自动探测）
  - 无pytdx时跳过复权，返回原始不复权数据

对外暴露 3 个公开函数:
  - fetch_xdxr(code)             — 获取 TDX 除权除息原始数据
  - build_fwd_factor(code, klines) — 构建前复权因子（内存缓存 + JSON 文件缓存）
  - apply_fwd_adjust(klines, code) — 对 K 线施加前复权

前复权公式 (标准通达信算法):
  对每次除权除息事件:
    factor = (除权前收盘价 - 每股分红) / (除权前收盘价 * (1 + 送转比 + 配股比))
  累乘所有事件的因子得到累积因子，用于乘以历史价格。
  TDX原始数据: fenhong=每10股分红(元), songzhuangu=每10股送转(股), peigu=每10股配股(股)

缓存策略:
  - 内存缓存: {code: [(date_str, cum_factor), ...]}，进程生命周期有效
  - 文件缓存: data/xdxr.json（单文件，全量），TTL 7 天
  - 线程安全: threading.Lock 保护缓存读写
"""

from __future__ import annotations

import json
import os
import threading
import time as _time
from typing import Any, Dict, List, Optional, Tuple

from app.data_sources.normalizer import normalize_cn_code
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ================================================================
# 配置
# ================================================================

_CACHE_DIR = os.environ.get("XDXR_CACHE_DIR", "data")
_CACHE_FILE = os.path.join(_CACHE_DIR, "xdxr.json")
_CACHE_TTL = 7 * 24 * 3600  # 7 天
_cache_loaded = False

# ================================================================
# TDX 服务器探测 & 连接
# ================================================================

HAS_TDX = False
try:
    from pytdx.hq import TdxHq_API
    HAS_TDX = True
except ImportError:
    pass

_TDX_CANDIDATE_SERVERS = [
    ("180.153.18.170", 7709), ("60.191.117.167", 7709), ("60.12.136.251", 7709),
    ("60.12.136.250", 7709), ("115.238.90.165", 7709), ("218.75.126.9", 7709),
    ("115.238.56.198", 7709), ("119.147.212.81", 7709), ("112.74.214.43", 7709),
    ("221.231.141.60", 7709), ("101.227.73.20", 7709), ("101.227.77.254", 7709),
]

_tdx_live_servers: List[tuple] = []
_tdx_discovered = False
_tdx_discover_lock = threading.Lock()


def _tdx_discover():
    """并行探测 TDX 服务器，仅执行一次。"""
    global _tdx_live_servers, _tdx_discovered
    with _tdx_discover_lock:
        if _tdx_discovered:
            return
        _tdx_discovered = True

    if not HAS_TDX:
        return

    import socket
    results = []

    def _probe(host, port):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            t0 = _time.time()
            s.connect((host, port))
            lat = _time.time() - t0
            s.close()
            try:
                api = TdxHq_API()
                api.connect(host, port, time_out=3)
                api.get_security_bars(1, 0, '000001', 0, 1)
                api.disconnect()
                results.append((host, port, lat))
            except Exception:
                pass
        except Exception:
            pass

    threads = [
        threading.Thread(target=_probe, args=(h, p), daemon=True)
        for h, p in _TDX_CANDIDATE_SERVERS
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    results.sort(key=lambda x: x[2])
    _tdx_live_servers = [(h, p) for h, p, _ in results]
    logger.info("[adjustment] TDX 服务器探测完成: %d 个可用", len(_tdx_live_servers))


def _ensure_tdx():
    if not _tdx_discovered:
        _tdx_discover()


# ================================================================
# 代码转换
# ================================================================

def _to_tdx_market_symbol(code: str) -> Optional[Tuple[int, str]]:
    nc = normalize_cn_code(code)
    if not nc or len(nc) < 3:
        return None
    prefix = nc[:2].upper()
    symbol = nc[2:]
    if not symbol.isdigit() or len(symbol) != 6:
        return None
    market = 1 if prefix == "SH" else 0
    return (market, symbol)


# ================================================================
# 文件缓存（单文件 data/xdxr.json）
# ================================================================

_xdxr_file_cache: Dict[str, List[Tuple[str, float]]] = {}
_xdxr_file_dirty = False
_xdxr_file_lock = threading.Lock()

# 写入策略: _put_file_cache 只更新内存+标记脏，不写磁盘。
# 写入时机: 进程退出时 atexit 兜底写入一次。
# 复权数据变化极低频（一年几次分红/送转），无需运行中频繁写入。


def _load_cache_file():
    """从 data/xdxr.json 加载全部缓存到内存，仅执行一次。

    文件格式: {"updated_at": 1715641234.5, "data": {code: [[date, factor], ...]}}
    兼容旧格式: {code: [[date, factor], ...]}（无 updated_at 时按 mtime 判断过期）
    """
    global _cache_loaded, _xdxr_file_cache
    if _cache_loaded:
        return
    with _xdxr_file_lock:
        if _cache_loaded:
            return
        _cache_loaded = True
        if not os.path.exists(_CACHE_FILE):
            return
        try:
            with open(_CACHE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)

            # 判断过期: 优先用文件内 updated_at，兜底用 mtime
            file_mtime = os.path.getmtime(_CACHE_FILE)
            updated_at = raw.get("updated_at") if isinstance(raw, dict) else None
            ts_to_check = updated_at if isinstance(updated_at, (int, float)) else file_mtime
            if _time.time() - ts_to_check > _CACHE_TTL:
                logger.info("[adjustment] 缓存文件过期(%.1f天)，忽略",
                            (_time.time() - ts_to_check) / 86400)
                return

            # 解析数据: 兼容新旧格式
            if isinstance(raw, dict) and "data" in raw:
                data = raw["data"]
            else:
                data = raw  # 旧格式: 顶层就是 {code: factors}

            if isinstance(data, dict):
                for code, factors in data.items():
                    if isinstance(factors, list):
                        _xdxr_file_cache[code] = [
                            (str(item[0]), float(item[1])) for item in factors if len(item) >= 2
                        ]
                logger.info("[adjustment] 从缓存加载 %d 只股票复权因子", len(_xdxr_file_cache))
        except Exception as e:
            logger.debug("[adjustment] 缓存文件读取失败: %s", e)


def _flush_cache_file():
    """将内存缓存写入 data/xdxr.json（仅脏数据时写入）。

    正常情况下不主动调用 — 脏数据由 atexit 在进程退出时写入。
    保留此函数供需要立即持久化的场景手动调用。
    """
    with _xdxr_file_lock:
        if _xdxr_file_dirty:
            _do_write()


def _do_write():
    """实际写入文件（调用时必须持有 _xdxr_file_lock）。"""
    global _xdxr_file_dirty
    try:
        os.makedirs(os.path.dirname(_CACHE_FILE), exist_ok=True)
        out = {
            "updated_at": _time.time(),
            "data": {code: [[d, c] for d, c in factors]
                     for code, factors in _xdxr_file_cache.items()},
        }
        with open(_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
        _xdxr_file_dirty = False
        logger.debug("[adjustment] 缓存已写入 %d 只", len(_xdxr_file_cache))
    except Exception as e:
        logger.debug("[adjustment] 缓存文件写入失败: %s", e)


def _flush_on_exit():
    """进程退出时兜底写入脏数据（atexit 注册）。"""
    with _xdxr_file_lock:
        if _xdxr_file_dirty:
            _do_write()
            if _xdxr_file_dirty:
                # _do_write 失败（如磁盘满），最后一次尝试 stderr 提示
                import sys
                print("[adjustment] 警告: 复权因子缓存写入失败，下次启动将重新从 TDX 拉取",
                      file=sys.stderr)


import atexit as _atexit
_atexit.register(_flush_on_exit)


def _get_file_cache(code: str) -> Optional[List[Tuple[str, float]]]:
    """从单文件缓存读取指定股票的复权因子。"""
    _load_cache_file()
    return _xdxr_file_cache.get(code)


def _put_file_cache(code: str, factors: List[Tuple[str, float]]):
    """写入指定股票的复权因子到内存，标记脏数据。

    不主动写磁盘 — 复权数据变化极低频（一年几次分红/送转），
    批量加载时每只都写磁盘毫无意义。写入时机:
      1. 进程退出时 atexit 兜底写入
      2. 下次冷启动 _load_cache_file 时发现过期会忽略旧文件
    """
    global _xdxr_file_dirty
    _load_cache_file()
    _xdxr_file_cache[code] = factors
    _xdxr_file_dirty = True


# ================================================================
# 核心函数
# ================================================================

_xdxr_cache: Dict[str, List[Tuple[str, float]]] = {}
_xdxr_lock = threading.Lock()


def fetch_xdxr(code: str) -> list:
    """
    从 TDX 获取除权除息原始数据。

    Args:
        code: 股票代码（任意格式）

    Returns:
        TDX 除权除息原始数据列表，失败返回空列表
    """
    _ensure_tdx()
    if not HAS_TDX or not _tdx_live_servers:
        return []

    ms = _to_tdx_market_symbol(code)
    if not ms:
        return []
    market, symbol = ms

    for host, port in _tdx_live_servers[:3]:
        try:
            api = TdxHq_API()
            api.connect(host, port, time_out=3)
            xdxr = api.get_xdxr_info(market, symbol)
            api.disconnect()
            if xdxr:
                return xdxr
            return []
        except Exception:
            continue
    return []


def _extract_date_str(bar_time) -> str:
    """从 bar["time"] 提取 YYYY-MM-DD 字符串，兼容 int 时间戳和 str。"""
    t = bar_time
    if isinstance(t, (int, float)):
        from datetime import datetime
        return datetime.fromtimestamp(int(t)).strftime("%Y-%m-%d")
    return str(t)[:10]


def build_fwd_factor(code: str, klines: list = None) -> List[Tuple[str, float]]:
    """
    构建前复权因子: [(date_str, cum_factor), ...] 按日期升序。

    使用标准通达信算法:
      对每次除权除息事件:
        factor = (除权前收盘价 - 每股分红) / (除权前收盘价 * (1 + 送转比 + 配股比))
      累乘所有事件的因子得到累积因子。

    需要传入 klines 来获取除权前收盘价。若 klines 为 None 或无法获取
    除权前收盘价，则跳过该事件（不使用错误的近似公式）。

    三级缓存:
      1. 内存缓存（最快）
      2. JSON 文件缓存（data/xdxr.json 单文件，TTL 7 天）
      3. TDX 网络请求（最慢）

    Args:
        code:   股票代码
        klines: 不复权 K 线数据（用于获取除权前收盘价）

    Returns:
        前复权因子列表，无除权数据返回空列表
    """
    nc = normalize_cn_code(code) or code

    # 1. 内存缓存
    with _xdxr_lock:
        if nc in _xdxr_cache:
            return _xdxr_cache[nc]

    # 2. 文件缓存（单文件）
    file_factors = _get_file_cache(nc)
    if file_factors is not None:
        with _xdxr_lock:
            _xdxr_cache[nc] = file_factors
        return file_factors

    # 3. TDX 网络请求
    xdxr = fetch_xdxr(nc)
    if not xdxr:
        with _xdxr_lock:
            _xdxr_cache[nc] = []
        return []

    # 解析除权除息事件
    events = []
    for r in xdxr:
        try:
            if int(r.get('category', 0)) != 1:
                continue
            y = int(r.get('year', 0))
            m = int(r.get('month', 0))
            d = int(r.get('day', 0))
            if y < 2000:
                continue
            date_str = f"{y:04d}-{m:02d}-{d:02d}"
            fenhong = float(r.get('fenhong', 0) or 0)         # 每10股分红(元)
            songzhuangu = float(r.get('songzhuangu', 0) or 0) # 每10股送转(股)
            peigujia = float(r.get('peigujia', 0) or 0)       # 配股价(元)
            peigu = float(r.get('peigu', 0) or 0)             # 每10股配股(股)
            if fenhong == 0 and songzhuangu == 0 and peigu == 0:
                continue
            events.append((date_str, fenhong, songzhuangu / 10.0, peigujia, peigu / 10.0))
        except Exception:
            continue

    if not events:
        with _xdxr_lock:
            _xdxr_cache[nc] = []
        return []

    events.sort(key=lambda x: x[0])

    # 构建除权日前收盘价索引（从 klines 提取）
    price_map: Dict[str, float] = {}
    if klines:
        for bar in klines:
            d = _extract_date_str(bar.get("time", ""))
            c = bar.get("close", 0)
            if d and c and c > 0:
                price_map[d] = float(c)

    # 标准通达信前复权因子算法
    result: List[Tuple[str, float]] = []
    cum = 1.0
    for date_str, fenhong, sg_ratio, pgj, pg_ratio in events:
        prev_close = price_map.get(date_str)
        if prev_close and prev_close > 0:
            dividend_per_share = fenhong / 10.0
            factor = (prev_close - dividend_per_share) / (prev_close * (1.0 + sg_ratio + pg_ratio))
        else:
            # 无除权前收盘价时，仅处理送转和配股，忽略分红
            divisor = 1.0 + sg_ratio + pg_ratio
            if divisor <= 0:
                continue
            factor = 1.0 / divisor
        if factor <= 0 or factor > 2:
            continue
        cum *= factor
        result.append((date_str, cum))

    with _xdxr_lock:
        _xdxr_cache[nc] = result
    _put_file_cache(nc, result)

    return result


def apply_fwd_adjust(klines: list, code: str) -> list:
    """
    对不复权 K 线数据施加前复权。

    Args:
        klines: K 线列表（需包含 time/open/high/low/close/volume）
        code:   股票代码

    Returns:
        前复权后的 K 线列表（新列表）
    """
    if not klines:
        return klines

    # 传入 klines 以便获取除权前收盘价
    factors = build_fwd_factor(code, klines=klines)
    if not factors:
        return klines

    adjusted = []
    factor_idx = 0
    current_factor = 1.0

    # 前复权: 历史价格 × (末尾累积因子 / 当前累积因子)
    # 这样最末尾的价格不变，历史价格被调整到与末尾连续
    last_factor = factors[-1][1] if factors else 1.0

    for bar in klines:
        bar_date = _extract_date_str(bar.get("time", ""))

        # 推进因子: factor_date <= bar_date 时累积（除权日当天就切换）
        while factor_idx < len(factors) and factors[factor_idx][0] <= bar_date:
            current_factor = factors[factor_idx][1]
            factor_idx += 1

        # 调整因子 = 末尾因子 / 当前因子
        # 除权日之前: current_factor=1.0, adj=last_factor (<1, 调整)
        # 除权日当天: current_factor=last_factor, adj=1.0 (不调整)
        if current_factor > 0:
            adj_factor = last_factor / current_factor
        else:
            adj_factor = 1.0

        if adj_factor < 1.0:
            adjusted.append({
                "time": bar["time"],
                "open": round(bar["open"] * adj_factor, 4),
                "high": round(bar["high"] * adj_factor, 4),
                "low": round(bar["low"] * adj_factor, 4),
                "close": round(bar["close"] * adj_factor, 4),
                "volume": bar["volume"],
            })
        else:
            adjusted.append(bar)

    return adjusted
