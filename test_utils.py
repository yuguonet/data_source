# -*- coding: utf-8 -*-
"""
测试工具模块 — 共享的股票代码缓存 + 通用函数

所有 test_*.py 共用此模块，避免重复扫描代码范围。
首次运行从腾讯接口探测，结果缓存到 data/active_codes.json（24h 有效）。
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional

import requests

# ================================================================
# 常量
# ================================================================

_TZ_CN = timezone(timedelta(hours=8))
_CACHE_DIR = Path(__file__).parent / "data"
_CACHE_FILE = _CACHE_DIR / "active_codes.json"
_CACHE_TTL_HOURS = 24

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


# ================================================================
# 通用工具
# ================================================================

def progress(current: int, total: int, prefix: str = ""):
    """进度条"""
    pct = current / total * 100 if total > 0 else 0
    bar = "█" * int(pct / 2) + "░" * (50 - int(pct / 2))
    print(f"\r  {prefix} {bar} {pct:.0f}% ({current}/{total})", end="", flush=True)


def http_get(url: str, params: dict = None, referer: str = "", encoding: str = "",
             timeout: int = 10, retries: int = 2) -> Optional[str]:
    """带重试的 HTTP GET"""
    headers = {"User-Agent": UA}
    if referer:
        headers["Referer"] = referer
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if encoding:
                resp.encoding = encoding
            if resp.status_code == 200 and resp.text:
                return resp.text
        except Exception:
            if attempt < retries - 1:
                time.sleep(0.5)
    return None


# ================================================================
# 股票代码缓存
# ================================================================

def _probe_active_codes() -> List[str]:
    """
    从腾讯行情接口探测活跃 A 股代码。
    扫描沪深主板+科创板+创业板代码范围，返回有有效行情的代码。
    """
    print("\n📋 探测活跃股票代码列表（首次需要 1-2 分钟）...")

    code_ranges = []
    for i in range(600000, 606000):       # 沪市主板
        code_ranges.append(f"sh{i}")
    for i in range(688000, 690000):       # 科创板
        code_ranges.append(f"sh{i}")
    for i in range(1, 4000):              # 深市主板
        code_ranges.append(f"sz{i:06d}")
    for i in range(300000, 302000):       # 创业板
        code_ranges.append(f"sz{i}")

    print(f"  探测范围: {len(code_ranges)} 个候选代码")

    batch_size = 500
    all_active: List[str] = []
    total_batches = (len(code_ranges) + batch_size - 1) // batch_size

    for batch_idx in range(0, len(code_ranges), batch_size):
        batch = code_ranges[batch_idx:batch_idx + batch_size]
        codes_str = ",".join(batch)
        progress(batch_idx // batch_size, total_batches, "探测中")

        text = http_get(
            f"https://qt.gtimg.cn/q={codes_str}",
            referer="https://gu.qq.com/",
            encoding="gbk",
            timeout=15,
        )
        if not text:
            continue

        for line in text.strip().split("\n"):
            line = line.strip().rstrip(";")
            if "=" not in line or '""' in line:
                continue
            try:
                var_name, data = line.split("=", 1)
                parts = data.strip('"').split("~")
                if len(parts) >= 6 and parts[1] and parts[3]:
                    last = float(parts[3]) if parts[3] else 0
                    if last > 0:
                        m = re.search(r'(sh|sz)(\d+)', var_name)
                        if m:
                            all_active.append(f"{m.group(1)}{m.group(2)}")
            except (ValueError, IndexError):
                continue

        time.sleep(0.3)

    progress(total_batches, total_batches, "探测中")
    print(f"\n  ✅ 找到 {len(all_active)} 只活跃股票")
    return all_active


def _load_cache() -> Optional[dict]:
    """加载缓存文件，返回 None 如果不存在或过期"""
    if not _CACHE_FILE.exists():
        return None
    try:
        with open(_CACHE_FILE, "r") as f:
            data = json.load(f)
        cached_at = datetime.fromisoformat(data["updated_at"])
        if datetime.now(_TZ_CN) - cached_at > timedelta(hours=_CACHE_TTL_HOURS):
            print(f"  ⏰ 缓存已过期（{data['updated_at']}），重新探测")
            return None
        return data
    except (json.JSONDecodeError, KeyError, ValueError):
        return None


def _save_cache(codes: List[str]):
    """保存到缓存文件"""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "updated_at": datetime.now(_TZ_CN).isoformat(),
        "count": len(codes),
        "codes": codes,
    }
    with open(_CACHE_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  💾 已缓存到 {_CACHE_FILE}")


def get_active_stock_codes(max_codes: int = 0, force_refresh: bool = False) -> List[str]:
    """
    获取活跃 A 股代码列表 — 文件缓存版。

    首次调用或缓存过期时从腾讯接口探测，结果缓存 24 小时。
    后续调用直接读文件，毫秒级返回。

    Args:
        max_codes:    最大返回数量，0=不限制
        force_refresh: 强制刷新缓存

    Returns:
        带市场前缀的代码列表，如 ["sh600519", "sz000001", ...]
    """
    codes = None

    if not force_refresh:
        cache = _load_cache()
        if cache:
            codes = cache["codes"]
            print(f"  📂 从缓存加载 {len(codes)} 只股票代码 ({cache['updated_at']})")

    if codes is None:
        codes = _probe_active_codes()
        if codes:
            _save_cache(codes)

    if max_codes > 0 and len(codes) > max_codes:
        import random
        random.seed(42)
        codes = random.sample(codes, max_codes)

    return codes


def get_test_codes(n: int = 5) -> List[str]:
    """获取少量测试代码（不走探测，用固定列表）"""
    return ["sh600519", "sz000001", "sh601318", "sz000858", "sh600036",
            "sz002415", "sh601166", "sz000725", "sh600900", "sz000333",
            "sh601888", "sz002714", "sh600276", "sz000651", "sh601398",
            "sz002304", "sh600030", "sz000568", "sh601012", "sz002594"][:n]


# ================================================================
# CLI 入口 — 独立运行可刷新缓存
# ================================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="股票代码缓存管理")
    parser.add_argument("--refresh", action="store_true", help="强制刷新缓存")
    parser.add_argument("--show", action="store_true", help="显示当前缓存内容")
    parser.add_argument("--max", type=int, default=0, help="最大返回数量")
    args = parser.parse_args()

    if args.show:
        cache = _load_cache()
        if cache:
            print(f"缓存时间: {cache['updated_at']}")
            print(f"代码数量: {cache['count']}")
            print(f"前10个: {cache['codes'][:10]}")
        else:
            print("无有效缓存")
    else:
        codes = get_active_stock_codes(max_codes=args.max, force_refresh=args.refresh)
        print(f"返回 {len(codes)} 只股票代码")
