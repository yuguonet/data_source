#!/usr/bin/env python3
"""新旧 _batch_quotes_queue 对比测试"""
import sys, os, time, threading, concurrent.futures
from collections import deque
from typing import Dict, List, Set, Tuple, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.data_sources.provider import get_providers, _registry
from app.data_sources.source_config import get_source_config
from app.data_sources.normalizer import strip_market_prefix
from test_utils import get_active_stock_codes

# ================================================================
# 旧版: 队列分配 + 分阶段启动
# ================================================================
def old_batch_quotes(symbols, available, group_size=500, timeout=120):
    from queue import Queue, Empty

    groups = [symbols[i:i+group_size] for i in range(0, len(symbols), group_size)]
    task_queue = Queue()
    for g in groups:
        task_queue.put((g,))

    result_list = []
    completed = set()
    data_lock = threading.Lock()

    available_names = [p.name for p in available]
    symbol_tried = {}
    symbol_tried_lock = threading.Lock()
    failed = []
    failed_lock = threading.Lock()
    failed_set = set()

    global_stop = threading.Event()
    per_task_timeout = 8.0
    stats = {p.name: {"ok": 0, "fail": 0, "groups": 0} for p in available}

    def _put_back_missing(missing, source_name):
        if not missing:
            return
        to_retry, newly_failed = [], []
        with symbol_tried_lock, failed_lock:
            for sym in missing:
                if sym in failed_set:
                    continue
                tried = symbol_tried.setdefault(sym, set())
                tried.add(source_name)
                if len(tried) < len(available_names):
                    to_retry.append(sym)
                else:
                    newly_failed.append(sym)
        if newly_failed:
            with failed_lock:
                for sym in newly_failed:
                    if sym not in failed_set:
                        failed_set.add(sym)
                        failed.append(sym)
        if to_retry:
            with data_lock:
                to_retry = [s for s in to_retry if strip_market_prefix(s) not in completed]
            if to_retry:
                for i in range(0, len(to_retry), group_size):
                    task_queue.put((to_retry[i:i+group_size],))

    def _merge_quotes(raw_dict):
        added = []
        with data_lock:
            for psym, quote in raw_dict.items():
                d = strip_market_prefix(psym)
                if d in completed or d in failed_set:
                    continue
                quote["symbol"] = d
                result_list.append(quote)
                completed.add(d)
                added.append(d)
        return added

    def _process_group(provider, group_codes):
        with data_lock:
            remaining = [c for c in group_codes if strip_market_prefix(c) not in completed]
        with failed_lock:
            remaining = [c for c in remaining if strip_market_prefix(c) not in failed_set]
        if not remaining:
            return
        requested_set = set(remaining)
        try:
            task_result = provider.fetch_batch_quotes(remaining, timeout=int(per_task_timeout))
            if task_result:
                added = _merge_quotes(task_result)
                returned_digits = {strip_market_prefix(s) for s in task_result.keys()}
                missing = [s for s in requested_set if strip_market_prefix(s) not in returned_digits]
                if missing and not global_stop.is_set():
                    _put_back_missing(missing, provider.name)
                stats[provider.name]["ok"] += len(added)
                stats[provider.name]["groups"] += 1
            else:
                stats[provider.name]["fail"] += 1
                if not global_stop.is_set():
                    _put_back_missing(remaining, provider.name)
        except Exception:
            stats[provider.name]["fail"] += 1
            if not global_stop.is_set():
                _put_back_missing(remaining, provider.name)

    def _worker(provider, priority=0):
        while not global_stop.is_set():
            try:
                (group_codes,) = task_queue.get(timeout=1.0)
            except:
                return
            _process_group(provider, group_codes)

    # 分阶段启动
    total_threads = 0
    thread_plan = []
    for p in available:
        cfg = get_source_config(p.name)
        n = min(cfg.max_workers, len(groups))
        thread_plan.append((p, n))
        total_threads += n

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=total_threads, thread_name_prefix="old")
    all_futures = []
    for stage_idx, (provider, n) in enumerate(thread_plan):
        if global_stop.is_set():
            break
        stage_futures = []
        for _ in range(n):
            f = pool.submit(_worker, provider, stage_idx)
            stage_futures.append(f)
            all_futures.append(f)
        if stage_idx == 0:
            done, _ = concurrent.futures.wait(stage_futures, timeout=timeout, return_when=concurrent.futures.ALL_COMPLETED)
            if len(done) == len(stage_futures) and not global_stop.is_set():
                continue
        if stage_idx > 0:
            time.sleep(0.1)

    concurrent.futures.wait(all_futures, timeout=timeout)
    global_stop.set()
    pool.shutdown(wait=False)

    # drain
    drain_deadline = time.time() + min(timeout, 30.0)
    while time.time() < drain_deadline:
        try:
            (group_codes,) = task_queue.get_nowait()
        except:
            break
        with data_lock:
            remaining = [c for c in group_codes if strip_market_prefix(c) not in completed]
        with failed_lock:
            remaining = [c for c in remaining if strip_market_prefix(c) not in failed_set]
        if not remaining:
            continue
        for provider in available:
            if not remaining or time.time() >= drain_deadline:
                break
            try:
                task_result = provider.fetch_batch_quotes(remaining, timeout=int(per_task_timeout))
                if task_result:
                    _merge_quotes(task_result)
                    with data_lock:
                        remaining = [c for s in remaining if strip_market_prefix(s) not in completed]
            except:
                pass
        if remaining:
            with failed_lock:
                for sym in remaining:
                    if sym not in failed_set and sym not in completed:
                        failed_set.add(sym)
                        failed.append(sym)

    return {q["symbol"]: q for q in result_list}, failed, stats


# ================================================================
# 新版: deque 工作队列
# ================================================================
def new_batch_quotes(symbols, available, group_size=500, timeout=120):
    num_sources = len(available)
    queue = deque()
    for s in symbols:
        queue.append((strip_market_prefix(s), set()))
    queue_lock = threading.Lock()

    result_list = []
    completed = set()
    failed_set = set()
    failed = []
    result_lock = threading.Lock()

    global_stop = threading.Event()
    per_task_timeout = 8.0
    stats = {p.name: {"ok": 0, "fail": 0, "batches": 0} for p in available}
    stats_lock = threading.Lock()

    def _grab(provider_name, n):
        with queue_lock:
            codes, tried_list, deferred = [], [], []
            while queue and len(codes) < n:
                code, tried = queue.popleft()
                if code in completed or code in failed_set:
                    continue
                if provider_name in tried:
                    deferred.append((code, tried))
                    continue
                codes.append(code)
                tried_list.append(tried)
            for item in deferred:
                queue.append(item)
            return codes, tried_list

    def _submit(provider_name, requested, tried_list, task_result):
        ok_syms = []
        if task_result:
            with result_lock:
                for psym, quote in task_result.items():
                    d = strip_market_prefix(psym)
                    if d in completed or d in failed_set:
                        continue
                    quote["symbol"] = d
                    result_list.append(quote)
                    completed.add(d)
                    ok_syms.append(d)
        returned_digits = {strip_market_prefix(s) for s in (task_result or {}).keys()}
        fail_items = []
        for i, sym in enumerate(requested):
            d = strip_market_prefix(sym) if not sym.isdigit() else sym
            if d not in returned_digits and d not in completed:
                tried = tried_list[i] if i < len(tried_list) else set()
                tried.add(provider_name)
                if len(tried) >= num_sources:
                    with result_lock:
                        if d not in failed_set and d not in completed:
                            failed_set.add(d)
                            failed.append(d)
                else:
                    fail_items.append((d, tried))
        if fail_items:
            with queue_lock:
                for code, tried in fail_items:
                    queue.append((code, tried))
        return ok_syms

    def _worker(provider):
        my_ok = 0
        while not global_stop.is_set():
            batch, tried_list = _grab(provider.name, group_size)
            if not batch:
                return
            try:
                task_result = provider.fetch_batch_quotes(batch, timeout=int(per_task_timeout))
                ok = _submit(provider.name, batch, tried_list, task_result)
                my_ok += len(ok)
                with stats_lock:
                    stats[provider.name]["ok"] += len(ok)
                    stats[provider.name]["batches"] += 1
                if not task_result:
                    stats[provider.name]["fail"] += 1
                    if my_ok == 0:
                        return
            except Exception as e:
                _submit(provider.name, batch, tried_list, None)
                with stats_lock:
                    stats[provider.name]["fail"] += 1
                if my_ok == 0:
                    return

    total_threads = 0
    for p in available:
        cfg = get_source_config(p.name)
        n = min(cfg.max_workers, max(1, len(symbols) // group_size + 1))
        total_threads += n

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=total_threads, thread_name_prefix="new")
    all_futures = []
    for provider in available:
        cfg = get_source_config(provider.name)
        n = min(cfg.max_workers, max(1, len(symbols) // group_size + 1))
        for _ in range(n):
            all_futures.append(pool.submit(_worker, provider))

    concurrent.futures.wait(all_futures, timeout=timeout)
    global_stop.set()
    pool.shutdown(wait=False)

    return {q["symbol"]: q for q in result_list}, failed, stats


# ================================================================
# 对比
# ================================================================
def run_comparison():
    all_codes = get_active_stock_codes()
    if not all_codes:
        print("无法获取股票代码")
        return

    providers = get_providers(capability="batch_quote", market="CNStock")
    available = [p for p in providers if p.name in ("tencent", "sohu")]
    print(f"测试源: {[p.name for p in available]}")
    print(f"股票数: {len(all_codes)}")

    # 预热
    print("\n预热中...")
    coord_new = available[0]
    coord_new.fetch_batch_quotes(all_codes[:5], timeout=8)

    # 旧版
    print("\n" + "="*60)
    print("旧版 (队列分配 + 分阶段启动)")
    print("="*60)
    t0 = time.perf_counter()
    r1, f1, s1 = old_batch_quotes(all_codes, available)
    t1 = time.perf_counter() - t0
    print(f"  耗时: {t1:.2f}s | 成功: {len(r1)}/{len(all_codes)} | 失败: {len(f1)}")
    for name, s in s1.items():
        if s["ok"] > 0 or s["fail"] > 0:
            print(f"  {name}: {s['ok']}只/{s['groups']}批 fail={s['fail']}")

    # 新版
    print("\n" + "="*60)
    print("新版 (deque 工作队列)")
    print("="*60)
    t0 = time.perf_counter()
    r2, f2, s2 = new_batch_quotes(all_codes, available)
    t2 = time.perf_counter() - t0
    print(f"  耗时: {t2:.2f}s | 成功: {len(r2)}/{len(all_codes)} | 失败: {len(f2)}")
    for name, s in s2.items():
        if s["ok"] > 0 or s["fail"] > 0:
            print(f"  {name}: {s['ok']}只/{s['batches']}批 fail={s['fail']}")

    # 对比
    print("\n" + "="*60)
    print("对比")
    print("="*60)
    speedup = t1 / t2 if t2 > 0 else 0
    diff = t1 - t2
    winner = "新版" if t2 < t1 else "旧版"
    print(f"  旧版: {t1:.2f}s")
    print(f"  新版: {t2:.2f}s")
    print(f"  差值: {diff:+.2f}s ({winner}快 {abs(diff):.2f}s)")
    print(f"  加速比: {speedup:.2f}x")


if __name__ == "__main__":
    run_comparison()
