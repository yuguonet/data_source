# -*- coding: utf-8 -*-
"""
Coordinator 五大接口测试

测试接口:
  1. coordinate_kline        — 单股K线
  2. coordinate_ticker       — 单股实时行情 (Race)
  3. coordinate_tickers      — 批量实时行情
  4. coordinate_batch_quotes — 批量行情 (底层)
  5. coordinate_market_kline — 全市场批量K线

用法:
  python3 test_coordinator.py                    # 全部测试
  python3 test_coordinator.py kline              # 只测 K线
  python3 test_coordinator.py ticker             # 只测单股行情
  python3 test_coordinator.py tickers            # 只测批量行情
  python3 test_coordinator.py batch              # 只测批量行情(底层)
  python3 test_coordinator.py market             # 全市场K线 默认5200只
  python3 test_coordinator.py market --count 100 --tf 5m  # 100只 5分钟
  python3 test_coordinator.py kline ticker       # 测多个

  python3 test_coordinator.py kline --tf 5m      # K线用5分钟周期
  python3 test_coordinator.py kline --count 1    # K线只跑1个用例
  python3 test_coordinator.py batch --count 20   # 批量行情 20只
"""

from __future__ import annotations

import sys
import os
import time
import argparse
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

# 确保项目根目录在 sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.data_sources.coordinator import get_coordinator
from app.data_sources.source_config import get_source_config, SOURCE_CONFIGS
from test_utils import get_active_stock_codes, get_test_codes

# ================================================================
# 常量
# ================================================================

TZ_CN = timezone(timedelta(hours=8))
NOW = datetime.now(TZ_CN)
TODAY = NOW.strftime("%Y-%m-%d")

# 测试用股票代码（大中小盘各选几只）
TEST_CODES = ["600519", "000001", "601318", "000858", "600036"]

# ================================================================
# 工具函数
# ================================================================

def _divider(title: str):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def _sub(text: str):
    print(f"\n--- {text} ---")


def _show_result(data: Any, max_items: int = 3):
    """展示结果摘要"""
    if isinstance(data, list):
        print(f"  返回 {len(data)} 项")
        for i, item in enumerate(data[:max_items]):
            if isinstance(item, dict):
                sym = item.get("symbol", "?")
                keys = [k for k in item.keys() if k != "symbol"][:6]
                print(f"  [{i}] {sym}: {keys}")
            else:
                print(f"  [{i}] {item}")
        if len(data) > max_items:
            print(f"  ... 还有 {len(data) - max_items} 项")
    elif isinstance(data, dict):
        print(f"  返回 {len(data)} 项")
        for i, (k, v) in enumerate(data.items()):
            if i >= max_items:
                print(f"  ... 还有 {len(data) - max_items} 项")
                break
            if isinstance(v, dict):
                keys = list(v.keys())[:8]
                print(f"  {k}: {keys}")
            else:
                print(f"  {k}: {v}")
    else:
        print(f"  返回: {data}")


def _validate_quote(q: dict) -> bool:
    """校验行情数据格式"""
    if not q or not isinstance(q, dict):
        return False
    has_price = any(k in q for k in ("last", "price", "currentPrice", "current"))
    return has_price


def _analyze_kline_list(bars: list, label: str = ""):
    """OHLCV 统计校验 — List[dict] 格式，每条含 symbol 字段"""
    if not bars:
        print(f"  ⚠ {label} 返回结果为空")
        return

    # 按 symbol 分组
    grouped: Dict[str, list] = {}
    for bar in bars:
        sym = bar.get("symbol", "?")
        grouped.setdefault(sym, []).append(bar)

    total_syms = len(grouped)
    total_bars = len(bars)
    empty = 0
    zero_vol = 0
    flat_ohlc = 0
    valid = 0
    sample_shown = False

    for sym, sym_bars in grouped.items():
        has_nonzero_vol = False
        sym_flat = 0
        for bar in sym_bars:
            o, h, l, c = bar.get("open"), bar.get("high"), bar.get("low"), bar.get("close")
            if o is not None and o == h == l == c:
                sym_flat += 1
            vol = bar.get("volume") or bar.get("vol") or 0
            if vol != 0:
                has_nonzero_vol = True

        if sym_flat == len(sym_bars):
            flat_ohlc += 1
        if has_nonzero_vol:
            valid += 1
        else:
            zero_vol += 1

        if not sample_shown:
            first = sym_bars[0]
            o, h, l, c = first.get("open"), first.get("high"), first.get("low"), first.get("close")
            vol = first.get("volume") or first.get("vol") or 0
            print(f"  📊 样本 [{sym}] bars={len(sym_bars)} | O={o} H={h} L={l} C={c} V={vol}")
            sample_shown = True

    print(f"  📈 OHLCV 统计: 股票={total_syms} K线={total_bars} | 有效={valid} | 全零成交量={zero_vol} | 全持平={flat_ohlc}")
    avg = total_bars / total_syms if total_syms > 0 else 0
    print(f"     平均每只: {avg:.1f} 条")
    if flat_ohlc > total_syms * 0.5:
        print(f"  ⚠ 超过50%的股票OHLC全部持平，可能存在假数据!")


def _analyze_quote_list(quotes: list, label: str = ""):
    """行情统计 — List[dict] 格式"""
    if not quotes:
        print(f"  ⚠ {label} 返回结果为空")
        return

    total = len(quotes)
    empty_price = sum(1 for q in quotes if not (q.get("last") or q.get("price") or q.get("currentPrice")))
    zero_price = sum(1 for q in quotes if (q.get("last") or q.get("price") or 0) == 0)
    print(f"  📈 行情统计: 总数={total} | 无价格={empty_price} | 零价格={zero_price}")
    if empty_price > 0:
        print(f"  ⚠ 有 {empty_price} 只股票无价格数据!")


# ================================================================
# 测试 1: coordinate_kline — 单股K线
# ================================================================

def test_coordinate_kline(tf: str = "1D", count: int = 0):
    _divider(f"测试 1: coordinate_kline — 单股K线 (tf={tf})")
    coord = get_coordinator()

    all_cases = [
        {"name": f"{tf} 茅台", "symbol": "600519", "timeframe": tf, "limit": 10},
        {"name": f"{tf} 平安", "symbol": "000001", "timeframe": tf, "limit": 5},
        {"name": f"{tf} 五粮液", "symbol": "000858", "timeframe": tf, "limit": 20},
    ]
    cases = all_cases[:count] if count > 0 else all_cases

    for case in cases:
        _sub(case["name"])
        t0 = time.perf_counter()
        result = coord.coordinate_kline(
            symbol=case["symbol"],
            timeframe=case["timeframe"],
            limit=case["limit"],
            market="CNStock",
            timeout=15.0,
        )
        elapsed = time.perf_counter() - t0

        bars = result.get("bars", [])
        source = result.get("source", "")
        ok = len(bars) > 0
        print(f"  耗时: {elapsed:.2f}s | 条数: {len(bars)} | 源: {source} | {'✅ 成功' if ok else '❌ 失败'}")

        if bars:
            first = bars[0]
            keys = list(first.keys())[:8]
            print(f"  字段: {keys}")

    # preferred_source 测试
    _sub(f"preferred_source 优先源测试 (tencent) tf={tf}")
    t0 = time.perf_counter()
    result = coord.coordinate_kline(
        symbol="600519",
        timeframe=tf,
        limit=5,
        market="CNStock",
        preferred_source="tencent",
        timeout=15.0,
    )
    elapsed = time.perf_counter() - t0
    bars = result.get("bars", [])
    print(f"  耗时: {elapsed:.2f}s | 条数: {len(bars)} | 源: {result.get('source', '')} | {'✅' if bars else '❌'}")


# ================================================================
# 测试 2: coordinate_ticker — 单股实时行情
# ================================================================

def test_coordinate_ticker():
    _divider("测试 2: coordinate_ticker — 单股实时行情 (Race)")
    coord = get_coordinator()

    for code, name in [("600519", "贵州茅台"), ("000001", "平安银行")]:
        _sub(f"Race {code} {name}")
        t0 = time.perf_counter()
        result = coord.coordinate_ticker(
            symbol=code,
            market="CNStock",
            timeout=8.0,
        )
        elapsed = time.perf_counter() - t0
        print(f"  耗时: {elapsed:.2f}s")
        if result:
            valid = _validate_quote(result)
            status = "✅ 格式正确" if valid else "⚠ 格式异常"
            last = result.get("last") or result.get("price") or result.get("currentPrice") or "N/A"
            print(f"  最新价={last}  {status}")
            for k in ("last", "price", "change", "changePercent", "high", "low", "open", "previousClose", "name"):
                if k in result:
                    print(f"    {k}: {result[k]}")
        else:
            print(f"  ❌ 未获取到数据")


# ================================================================
# 测试 3: coordinate_tickers — 批量实时行情
# ================================================================

def test_coordinate_tickers(count: int = 5):
    _divider(f"测试 3: coordinate_tickers — 批量实时行情 ({count}只)")
    coord = get_coordinator()

    codes = TEST_CODES[:count]
    _sub(f"批量行情 ({len(codes)}只)")
    t0 = time.perf_counter()
    result = coord.coordinate_tickers(
        symbols=codes,
        market="CNStock",
        timeout=30.0,
    )
    elapsed = time.perf_counter() - t0
    print(f"  耗时: {elapsed:.2f}s | 成功: {len(result)}/{len(codes)}")

    _analyze_quote_list(result, label="tickers")
    _show_result(result)


# ================================================================
# 测试 4: coordinate_batch_quotes — 批量行情 (底层)
# ================================================================

def test_coordinate_batch_quotes(count: int = 5200):
    _divider(f"测试 4: coordinate_batch_quotes — 批量行情 ({count}只)")
    coord = get_coordinator()

    codes = get_active_stock_codes(max_codes=count)
    _sub(f"批量行情 ({len(codes)}只)")
    t0 = time.perf_counter()
    result = coord.coordinate_batch_quotes(
        symbols=codes,
        market="CNStock",
        timeout=120.0,
    )
    elapsed = time.perf_counter() - t0
    print(f"  耗时: {elapsed:.2f}s | 成功: {len(result)}/{len(codes)}")

    _analyze_quote_list(result, label="batch_quotes")
    for q in result[:3]:
        sym = q.get("symbol", "?")
        last = q.get("last") or q.get("price") or "N/A"
        name = q.get("name", "")
        print(f"  {sym} {name}: {last}")


# ================================================================
# 测试 5: coordinate_market_kline — 全市场批量K线
# ================================================================

def test_coordinate_market_kline(tf: str = "1D", count: int = 5200):
    _divider(f"测试 5: coordinate_market_kline — 全市场批量K线 ({count}只, tf={tf})")

    from app.data_sources.provider import get_providers
    providers = get_providers(capability="kline_batch", timeframe=tf, market="CNStock")
    print(f"  可用 kline_batch 源: {[p.name for p in providers]}")

    codes = get_active_stock_codes(max_codes=count)
    coord = get_coordinator()

    _sub(f"全市场K线 ({len(codes)}只)")
    t0 = time.perf_counter()
    result = coord.coordinate_market_kline(
        market="CNStock",
        timeframe=tf,
        count=500,
        timeout=120.0,
        symbols=codes,
    )
    elapsed = time.perf_counter() - t0
    print(f"  耗时: {elapsed:.2f}s | K线总数: {len(result)}")

    _analyze_kline_list(result, label=f"全市场K线 {len(codes)}只")
    _show_result(result)


# ================================================================
# 并发效率对比测试
# ================================================================

def test_efficiency():
    _divider("效率对比: 单线程 vs Coordinator 并发")
    coord = get_coordinator()
    code = "600519"
    timeframe = "1D"
    limit = 5

    # 单线程串行
    _sub("单线程串行 (3次)")
    from app.data_sources.provider import get_providers
    from app.data_sources.coordinator import _make_provider_fetch_fn

    providers = get_providers(capability="kline", timeframe=timeframe, market="CNStock")
    if not providers:
        print("  无可用源，跳过")
        return

    p = providers[0]
    fetch_fn = _make_provider_fetch_fn(p)
    print(f"  使用源: {p.name}")

    t0 = time.perf_counter()
    for _ in range(3):
        bars = fetch_fn(code, timeframe, limit)
    serial_time = time.perf_counter() - t0
    print(f"  串行耗时: {serial_time:.2f}s | 条数: {len(bars) if bars else 0}")

    # Coordinator 单股
    _sub("Coordinator 单股 (3次)")
    t0 = time.perf_counter()
    for _ in range(3):
        result = coord.coordinate_kline(
            symbol=code, timeframe=timeframe, limit=limit,
            market="CNStock", timeout=15.0,
        )
    parallel_time = time.perf_counter() - t0
    bars = result.get("bars", []) if result else []
    print(f"  并发耗时: {parallel_time:.2f}s | 条数: {len(bars) if bars else 0}")

    if serial_time > 0:
        speedup = serial_time / parallel_time if parallel_time > 0 else float('inf')
        print(f"\n  📊 加速比: {speedup:.2f}x (串行 {serial_time:.2f}s → 并发 {parallel_time:.2f}s)")


# ================================================================
# 源状态报告
# ================================================================

def show_source_status():
    _divider("数据源状态报告")
    from app.data_sources.provider import get_providers
    from app.data_sources.coordinator import get_realtime_circuit_breaker

    cb = get_realtime_circuit_breaker()

    for cap in ("kline", "quote", "batch_quote"):
        providers = get_providers(capability=cap, market="CNStock")
        names = []
        for p in providers:
            cfg = get_source_config(p.name)
            available = cb.is_available(p.name)
            status = "✅" if available else "🔴熔断"
            names.append(f"{p.name}({status} w={cfg.max_workers})")
        print(f"  [{cap}]: {', '.join(names) if names else '无'}")

    for cap in ("kline", "quote"):
        providers = get_providers(capability=cap, market="HKStock")
        names = [p.name for p in providers]
        print(f"  [{cap} HK]: {', '.join(names) if names else '无'}")


# ================================================================
# Main
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="Coordinator 接口测试")
    parser.add_argument("tests", nargs="*", default=[],
                        help="要运行的测试: kline / ticker / tickers / batch / market / efficiency / status / all")
    parser.add_argument("--tf", default="1D",
                        help="K线周期 (默认 1D, 可选: 1m/5m/15m/30m/60m/1W 等)")
    parser.add_argument("--count", type=int, default=None,
                        help="测试数量 (kline: 用例数, batch/tickers: 股票数)")
    args = parser.parse_args()

    test_map = {
        "kline": lambda: test_coordinate_kline(tf=args.tf, count=args.count or 0),
        "ticker": test_coordinate_ticker,
        "tickers": lambda: test_coordinate_tickers(count=args.count or 5),
        "batch": lambda: test_coordinate_batch_quotes(count=args.count or 5200),
        "market": lambda: test_coordinate_market_kline(tf=args.tf, count=args.count or 5200),
        "efficiency": test_efficiency,
        "status": show_source_status,
    }

    if not args.tests or "all" in args.tests:
        show_source_status()
        for fn in test_map.values():
            if fn != show_source_status:
                try:
                    fn()
                except Exception as e:
                    print(f"\n  ❌ 测试异常: {e}")
    else:
        for name in args.tests:
            fn = test_map.get(name)
            if fn:
                try:
                    fn()
                except Exception as e:
                    print(f"\n  ❌ {name} 测试异常: {e}")
            else:
                print(f"未知测试: {name}. 可选: {', '.join(test_map.keys())}")

    print(f"\n{'='*70}")
    print(f"  测试完成 @ {datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
