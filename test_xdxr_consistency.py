#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
除权股票数据源一致性测试 — 直接调用 provider.fetch_kline

从 xdxr.json 获取最近发生除权的股票，用各数据源的 fetch_kline 获取不复权 K 线，
横向对比除权日前后的数据一致性和偏差率。

用法:
  python3 test_xdxr_consistency.py              # 默认取最近30只
  python3 test_xdxr_consistency.py --count 50   # 取最近50只
  python3 test_xdxr_consistency.py --days 30    # 除权日期范围30天
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# ================================================================
# 路径设置
# ================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

# 导入 provider 注册表
from app.data_sources.provider import get_provider, get_providers

_TZ_CN = timezone(timedelta(hours=8))
_NOW = datetime.now(_TZ_CN)
TODAY = _NOW.strftime("%Y-%m-%d")


# ================================================================
# 加载最近除权股票
# ================================================================

def load_recent_xdxr_codes(days: int = 90, max_count: int = 30) -> List[Tuple[str, str]]:
    """
    从 xdxr.json 加载最近有除权记录的股票。

    Returns:
        [(code, xdxr_date), ...] 按除权日降序
    """
    xdxr_file = os.path.join(SCRIPT_DIR, "data", "xdxr.json")
    if not os.path.exists(xdxr_file):
        print("  ❌ xdxr.json 不存在")
        return []

    with open(xdxr_file) as f:
        raw = json.load(f)

    data = raw.get("data", {})
    cutoff = (_NOW - timedelta(days=days)).strftime("%Y-%m-%d")

    recent = []
    for code, factors in data.items():
        if not factors:
            continue
        last_date = factors[-1][0]
        if last_date >= cutoff:
            recent.append((code, last_date))

    recent.sort(key=lambda x: x[1], reverse=True)
    return recent[:max_count]


# ================================================================
# 直接调用 provider.fetch_kline
# ================================================================

def fetch_from_provider(provider_name: str, code: str, count: int = 20) -> Optional[List[Dict]]:
    """直接调用 provider 的 fetch_kline 方法"""
    provider = get_provider(provider_name)
    if not provider:
        return None
    try:
        result = provider.fetch_kline(code, "1D", count=count, adj="", timeout=15)
        if not result or not isinstance(result, dict):
            return None
        bars = result.get("bars") or []
        return bars if bars else None
    except Exception as e:
        return None


def fetch_all_providers(code: str, count: int = 20) -> Dict[str, List[Dict]]:
    """并发从所有可用 provider 获取 K 线数据"""
    # 获取所有支持 kline 的 provider
    providers = get_providers(capability="kline")
    provider_names = [p.name for p in providers]

    results = {}
    lock = threading.Lock()

    def _fetch(name):
        data = fetch_from_provider(name, code, count)
        if data:
            with lock:
                results[name] = data

    with ThreadPoolExecutor(max_workers=len(provider_names)) as pool:
        futures = [pool.submit(_fetch, name) for name in provider_names]
        for f in as_completed(futures, timeout=30):
            pass

    return results


# ================================================================
# 数据对比
# ================================================================

def compare_klines(all_data: Dict[str, List[Dict]], xdxr_date: str) -> Dict:
    """
    对比各源 K 线数据一致性。

    Returns:
        {
            "sources": [...],
            "by_date": {date: {source: {close, volume}, ...}},
            "pair_stats": {(s1,s2): {match, total, ohlc_match, vol_match}},
            "deviation": {source: {avg_dev, max_dev}},
        }
    """
    sources = sorted(all_data.keys())
    if len(sources) < 2:
        return {"sources": sources, "by_date": {}, "pair_stats": {}, "deviation": {}}

    # 按日期整理数据
    by_date: Dict[str, Dict[str, Dict]] = defaultdict(dict)
    for src, bars in all_data.items():
        for bar in bars:
            dt = str(bar.get("time", ""))[:10]
            if dt:
                by_date[dt][src] = bar

    # 关键日期: 除权日及前后各2天
    xdxr_dt = datetime.strptime(xdxr_date, "%Y-%m-%d")
    key_dates = []
    for delta in range(-3, 4):
        d = (xdxr_dt + timedelta(days=delta)).strftime("%Y-%m-%d")
        if d in by_date and len(by_date[d]) >= 2:
            key_dates.append(d)

    # 逐日对比
    pair_stats = defaultdict(lambda: {"match": 0, "total": 0, "ohlc_match": 0,
                                       "vol_match": 0, "close_devs": []})
    deviation = defaultdict(lambda: {"devs": [], "max_dev": 0})

    for dt in sorted(by_date.keys()):
        day_data = by_date[dt]
        available = [s for s in sources if s in day_data]
        if len(available) < 2:
            continue

        # 取基准 (第一个源)
        base_src = available[0]
        base = day_data[base_src]

        for other_src in available[1:]:
            other = day_data[other_src]
            pair = tuple(sorted([base_src, other_src]))
            pair_stats[pair]["total"] += 1

            # OHLC 一致检查 (允许 0.5% 偏差)
            ohlc_match = True
            for field in ["open", "high", "low", "close"]:
                bv, ov = base.get(field, 0), other.get(field, 0)
                if bv and ov and bv > 0:
                    dev = abs(bv - ov) / bv * 100
                    if dev > 0.5:
                        ohlc_match = False
                    deviation[base_src]["devs"].append(dev)
                    deviation[other_src]["devs"].append(dev)
                    pair_stats[pair]["close_devs"].append(dev)

            if ohlc_match:
                pair_stats[pair]["ohlc_match"] += 1

            # 成交量一致检查 (允许 5% 偏差)
            bv = base.get("volume", 0) or 0
            ov = other.get("volume", 0) or 0
            if bv > 0 and ov > 0:
                vol_dev = abs(bv - ov) / max(bv, ov) * 100
                if vol_dev <= 5:
                    pair_stats[pair]["vol_match"] += 1

    # 计算偏差统计
    for src in sources:
        devs = deviation[src]["devs"]
        if devs:
            deviation[src]["avg_dev"] = sum(devs) / len(devs)
            deviation[src]["max_dev"] = max(devs)
        else:
            deviation[src]["avg_dev"] = 0
            deviation[src]["max_dev"] = 0

    return {
        "sources": sources,
        "by_date": dict(by_date),
        "pair_stats": dict(pair_stats),
        "deviation": dict(deviation),
        "key_dates": key_dates,
    }


# ================================================================
# 主测试流程
# ================================================================

def run_test(codes_with_dates: List[Tuple[str, str]], kline_count: int = 20):
    """运行测试"""
    print(f"\n{'='*70}")
    print(f"  除权股票数据源一致性测试 (直接调用 provider.fetch_kline)")
    print(f"  测试股票: {len(codes_with_dates)} 只  |  K线数量: {kline_count} 根  |  复权: 不复权")
    print(f"{'='*70}\n")

    # 汇总统计
    total_pairs = defaultdict(lambda: {"match": 0, "total": 0, "ohlc_match": 0,
                                        "vol_match": 0, "close_devs": []})
    source_availability = defaultdict(int)

    for idx, (code, xdxr_date) in enumerate(codes_with_dates):
        print(f"  [{idx+1}/{len(codes_with_dates)}] {code} (除权日: {xdxr_date})")

        # 直接调用 provider.fetch_kline
        all_data = fetch_all_providers(code, kline_count)
        if not all_data:
            print(f"    ⚠️  所有源均无数据，跳过")
            continue

        available = sorted(all_data.keys())
        for src in available:
            source_availability[src] += 1
        print(f"    可用源: {', '.join(available)} ({len(available)}个)")

        # 对比分析
        result = compare_klines(all_data, xdxr_date)

        # 打印除权日附近数据
        if result.get("key_dates"):
            print(f"    除权日附近数据:")
            for dt in result["key_dates"][:5]:
                day_data = result["by_date"].get(dt, {})
                if not day_data:
                    continue
                marker = " ⬅ 除权日" if dt == xdxr_date else ""
                closes = {src: f"{d['close']:.2f}" for src, d in day_data.items()}
                print(f"      {dt}: {closes}{marker}")

        # 打印偏差
        for src in available:
            devs = result["deviation"].get(src, {})
            avg_dev = devs.get("avg_dev", 0)
            max_dev = devs.get("max_dev", 0)
            if avg_dev > 0:
                print(f"    {src}: 平均偏差 {avg_dev:.2f}%  最大偏差 {max_dev:.2f}%")

        # 汇总
        for pair, stats in result["pair_stats"].items():
            total_pairs[pair]["match"] += stats.get("ohlc_match", 0)
            total_pairs[pair]["total"] += stats.get("total", 0)
            total_pairs[pair]["ohlc_match"] += stats.get("ohlc_match", 0)
            total_pairs[pair]["vol_match"] += stats.get("vol_match", 0)
            total_pairs[pair]["close_devs"].extend(stats.get("close_devs", []))

        print()

    # 打印汇总报告
    print(f"\n{'='*70}")
    print(f"  📊 汇总报告")
    print(f"{'='*70}\n")

    # 数据源可用性
    print(f"  数据源可用性:")
    for src, count in sorted(source_availability.items(), key=lambda x: -x[1]):
        rate = count / len(codes_with_dates) * 100
        print(f"    {src:<12s}  {count}/{len(codes_with_dates)}  ({rate:.0f}%)")

    # 两两对比
    if total_pairs:
        print(f"\n  两两对比 (OHLC一致率 / 成交量一致率 / 平均价格偏差):")
        print(f"  {'对比对':<24s} {'OHLC一致':>10s} {'Vol一致':>10s} {'平均偏差':>10s} {'样本数':>8s}")
        print(f"  {'-'*64}")
        for pair, stats in sorted(total_pairs.items()):
            if stats["total"] == 0:
                continue
            pair_name = f"{pair[0]} vs {pair[1]}"
            ohlc_rate = stats["ohlc_match"] / stats["total"] * 100
            vol_rate = stats["vol_match"] / stats["total"] * 100
            avg_dev = sum(stats["close_devs"]) / len(stats["close_devs"]) if stats["close_devs"] else 0
            print(f"  {pair_name:<24s} {ohlc_rate:>9.1f}% {vol_rate:>9.1f}% {avg_dev:>9.2f}% {stats['total']:>8d}")

    # 各源偏差排名
    print(f"\n  各源偏差排名 (与其他源的平均价格偏差):")
    all_devs = {}
    for pair, stats in total_pairs.items():
        for src in pair:
            if src not in all_devs:
                all_devs[src] = []
            all_devs[src].extend(stats["close_devs"])

    for src, devs in sorted(all_devs.items()):
        if devs:
            avg = sum(devs) / len(devs)
            mx = max(devs)
            print(f"    {src:<12s}  平均: {avg:.2f}%  最大: {mx:.2f}%")


# ================================================================
# CLI
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="除权股票数据源一致性测试")
    parser.add_argument("--count", type=int, default=30,
                        help="测试股票数量 (默认30)")
    parser.add_argument("--days", type=int, default=90,
                        help="除权日期范围 (默认90天)")
    parser.add_argument("--kline-count", type=int, default=20,
                        help="每只股票获取K线数量 (默认20)")
    args = parser.parse_args()

    # 加载除权股票
    codes = load_recent_xdxr_codes(days=args.days, max_count=args.count)
    if not codes:
        print("  ❌ 无除权股票数据")
        return

    print(f"  📋 加载 {len(codes)} 只最近除权股票 (最近{args.days}天)")
    for code, dt in codes[:5]:
        print(f"    {code} ({dt})")
    if len(codes) > 5:
        print(f"    ... 等 {len(codes)} 只")

    run_test(codes, kline_count=args.kline_count)


if __name__ == "__main__":
    main()
