#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_batch_quotes 全市场拉取耗时分析

测试维度:
  1. 全市场一次性拉取 (所有活跃股票)
  2. 分批拉取对比 (不同 batch_size)
  3. 各 Provider 耗时分解
  4. 吞吐量统计 (只/秒)
"""

import sys, os, time, json, statistics
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.data_sources.coordinator import get_coordinator
from app.data_sources.source_config import get_source_config, SOURCE_CONFIGS
from test_utils import get_active_stock_codes

TZ_CN = timezone(timedelta(hours=8))
LINE = "─" * 68


def section(title: str):
    print(f"\n{'='*68}")
    print(f"  {title}")
    print(f"{'='*68}")


def test_full_market(exclude=None):
    """全市场 fetch_batch_quotes 耗时分析"""
    section("全市场 fetch_batch_quotes 耗时分析")

    # 1. 加载股票代码
    print("\n📋 加载活跃股票代码...")
    t0 = time.perf_counter()
    all_codes = get_active_stock_codes()
    load_time = time.perf_counter() - t0
    print(f"  共 {len(all_codes)} 只 | 加载耗时: {load_time:.2f}s")

    if not all_codes:
        print("  ❌ 无法获取股票代码列表")
        return

    coord = get_coordinator()

    # 2. 小批量预热 (5只)
    print("\n🔥 预热 (5只)...")
    warmup_codes = all_codes[:5]
    t0 = time.perf_counter()
    warmup_result = coord.coordinate_batch_quotes(
        symbols=warmup_codes, market="CNStock", timeout=15.0,
    )
    warmup_time = time.perf_counter() - t0
    print(f"  预热耗时: {warmup_time:.2f}s | 成功: {len(warmup_result)}/{len(warmup_codes)}")

    # 3. 全市场一次性拉取
    section("全市场一次性拉取")
    print(f"  股票数: {len(all_codes)}")
    print(f"  开始时间: {datetime.now(TZ_CN).strftime('%H:%M:%S')}")

    t_start = time.perf_counter()
    result = coord.coordinate_batch_quotes(
        symbols=all_codes,
        market="CNStock",
        timeout=120.0,
    )
    total_time = time.perf_counter() - t_start

    success_count = len(result)
    fail_count = len(all_codes) - success_count
    throughput = success_count / total_time if total_time > 0 else 0

    print(f"\n  ┌─ 结果汇总 {'─'*54}")
    print(f"  │ 总股票数:    {len(all_codes)}")
    print(f"  │ 成功获取:    {success_count}")
    print(f"  │ 失败/缺失:   {fail_count}")
    print(f"  │ 成功率:      {success_count/len(all_codes)*100:.1f}%")
    print(f"  │ 总耗时:      {total_time:.2f}s")
    print(f"  │ 吞吐量:      {throughput:.1f} 只/秒")
    print(f"  │ 平均每只:    {total_time/len(all_codes)*1000:.2f}ms")
    print(f"  └{'─'*66}")

    # 4. 样本数据展示
    print(f"\n  📊 样本数据 (前5只):")
    for q in result[:5]:
        sym = q.get("symbol", "?")
        last = q.get("last") or q.get("price") or q.get("close") or "N/A"
        name = q.get("name", "")
        chg = q.get("changePercent") or q.get("change") or ""
        print(f"  {sym} {name:>8} | 最新: {last:>10} | 涨跌%: {chg}")

    return {
        "total_codes": len(all_codes),
        "success": success_count,
        "fail": fail_count,
        "total_time": total_time,
        "throughput": throughput,
    }


def test_batch_size_comparison():
    """不同 batch_size 的耗时对比"""
    section("不同 batch_size 耗时对比")

    all_codes = get_active_stock_codes()
    if not all_codes or len(all_codes) < 50:
        print("  ⚠ 股票数量不足，跳过")
        return

    coord = get_coordinator()
    batch_sizes = [20, 50, 100, 200, 500]
    results = {}

    for bs in batch_sizes:
        test_codes = all_codes[:min(bs * 2, len(all_codes))]  # 取 batch_size*2 只来测
        print(f"\n  batch_size={bs} | 测试 {len(test_codes)} 只...")

        t0 = time.perf_counter()
        r = coord.coordinate_batch_quotes(
            symbols=test_codes, market="CNStock", timeout=60.0,
        )
        elapsed = time.perf_counter() - t0

        results[bs] = {
            "tested": len(test_codes),
            "success": len(r),
            "time": elapsed,
            "throughput": len(r) / elapsed if elapsed > 0 else 0,
        }
        print(f"    耗时: {elapsed:.2f}s | 成功: {len(r)}/{len(test_codes)} | {results[bs]['throughput']:.1f} 只/秒")

    # 汇总表
    print(f"\n  ┌─ batch_size 对比 {'─'*48}")
    print(f"  │ {'batch_size':>10} {'测试数':>8} {'成功':>8} {'耗时':>10} {'吞吐(只/s)':>12}")
    print(f"  │ {'─'*10} {'─'*8} {'─'*8} {'─'*10} {'─'*12}")
    for bs, r in results.items():
        print(f"  │ {bs:>10} {r['tested']:>8} {r['success']:>8} {r['time']:>9.2f}s {r['throughput']:>11.1f}")
    print(f"  └{'─'*66}")


def test_provider_breakdown(exclude=None):
    """各 Provider 单独耗时分解"""
    section("各 Provider 单独 fetch_batch_quotes 耗时")

    from app.data_sources.provider import get_providers

    providers = get_providers(capability="batch_quote", market="CNStock")
    if exclude:
        providers = [p for p in providers if p.name not in exclude]
    if not providers:
        print("  ⚠ 无可用 batch_quote provider")
        return

    print(f"  可用 Provider: {[p.name for p in providers]}")

    test_codes = get_active_stock_codes(max_codes=30)
    if not test_codes:
        test_codes = ["sh600519", "sz000001", "sh601318", "sz000858", "sh600036"]

    print(f"  测试代码: {len(test_codes)} 只\n")

    results = []
    for p in providers:
        print(f"  测试 {p.name}...", end=" ", flush=True)
        t0 = time.perf_counter()
        try:
            r = p.fetch_batch_quotes(test_codes)
            elapsed = time.perf_counter() - t0
            if isinstance(r, dict) and "data" in r:
                ok = r.get("ok", False)
                data = r["data"] if ok else {}
                count = len(data) if isinstance(data, dict) else 0
                err = r.get("error", "") if not ok else ""
            elif isinstance(r, dict):
                count = len(r)
                ok = count > 0
                err = ""
            else:
                count = 0
                ok = False
                err = "unexpected format"

            results.append({
                "name": p.name,
                "priority": p.priority,
                "time": elapsed,
                "success": count,
                "total": len(test_codes),
                "ok": ok,
                "error": err,
            })
            status = f"✅ {count}/{len(test_codes)}" if ok else f"⚠ {err[:30]}"
            print(f"{elapsed:.2f}s | {status}")
        except Exception as e:
            elapsed = time.perf_counter() - t0
            results.append({
                "name": p.name, "priority": p.priority,
                "time": elapsed, "success": 0, "total": len(test_codes),
                "ok": False, "error": str(e)[:40],
            })
            print(f"{elapsed:.2f}s | ❌ {str(e)[:40]}")

    # 汇总
    results.sort(key=lambda x: x["time"])
    print(f"\n  ┌─ Provider 耗时排名 {'─'*46}")
    print(f"  │ {'排名':>4} {'Provider':<16} {'优先级':>6} {'耗时':>10} {'成功':>8} {'状态':>6}")
    print(f"  │ {'─'*4} {'─'*16} {'─'*6} {'─'*10} {'─'*8} {'─'*6}")
    for i, r in enumerate(results, 1):
        icon = "✅" if r["ok"] else "❌"
        print(f"  │ {i:>4} {r['name']:<16} {r['priority']:>6} {r['time']:>9.2f}s {r['success']:>5}/{r['total']:<3} {icon}")
    print(f"  └{'─'*66}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="fetch_batch_quotes 全市场耗时分析")
    parser.add_argument("--full", action="store_true", help="全市场拉取")
    parser.add_argument("--batch", action="store_true", help="不同 batch_size 对比")
    parser.add_argument("--provider", action="store_true", help="各 Provider 单独耗时")
    parser.add_argument("--all", action="store_true", help="全部测试")
    parser.add_argument("--exclude", nargs="+", default=[], help="排除的 Provider")
    args = parser.parse_args()

    if not any([args.full, args.batch, args.provider, args.all]):
        args.all = True

    exclude = set(args.exclude) if args.exclude else None

    # 如果有 exclude，从注册表中临时移除
    if exclude:
        from app.data_sources.provider import _registry
        removed = {}
        for name in list(_registry.keys()):
            if name in exclude:
                removed[name] = _registry.pop(name)
        print(f"  ⚠ 已排除 Provider: {', '.join(exclude)}")

    overall_start = time.perf_counter()

    if args.all or args.provider:
        test_provider_breakdown(exclude)

    if args.all or args.batch:
        test_batch_size_comparison()

    if args.all or args.full:
        test_full_market()

    overall_time = time.perf_counter() - overall_start
    print(f"\n⏱  总测试耗时: {overall_time:.2f}s")

    # 恢复
    if exclude:
        _registry.update(removed)
