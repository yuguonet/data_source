#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据源 Provider 全接口连通性测试

输出格式统一:
  - 每只股票一个表格
  - 行 = 数据源
  - 列 = 指标（ticker/batch）或 日期（kline）
  - 方便横向比对各源数据差异

用法:
  python test_providers.py                  # 默认 600519 000001 300750
  python test_providers.py 000001           # 指定代码
  python test_providers.py --only kline     # 只测某个
  python test_providers.py --debug          # 完整堆栈
"""

from __future__ import annotations
import sys, os, types, time, importlib, argparse, json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# ================================================================
# Mock app
# ================================================================

def _build_mock_app():
    app = types.ModuleType("app"); app.__path__ = []; sys.modules["app"] = app
    au = types.ModuleType("app.utils"); au.__path__ = []; sys.modules["app.utils"] = au

    def get_logger(n):
        import logging; return logging.getLogger(n)
    ul = types.ModuleType("app.utils.logger"); ul.get_logger = get_logger; sys.modules["app.utils.logger"] = ul

    ds = types.ModuleType("app.data_sources"); ds.__path__ = []; sys.modules["app.data_sources"] = ds

    def normalize_cn_code(code):
        code = (code or "").strip()
        d = "".join(c for c in code if c.isdigit())
        if not d: return ""
        d = d.zfill(6)
        return f"sh{d}" if d.startswith(("6", "9")) else f"sz{d}"

    def normalize_hk_code(code):
        d = "".join(c for c in (code or "") if c.isdigit())
        return f"hk{d.zfill(5)}" if d else ""

    def detect_market(symbol):
        s = (symbol or "").strip().upper()
        d = "".join(c for c in s if c.isdigit())
        if not d: return ("", "")
        d = d.zfill(6)
        if s.startswith("SH") or d.startswith(("6", "9")): return ("SH", d)
        if s.startswith("SZ") or d.startswith(("0", "3")): return ("SZ", d)
        if d.startswith(("4", "8")): return ("BJ", d)
        return ("SZ", d)

    def to_raw_digits(symbol):
        return "".join(c for c in (symbol or "") if c.isdigit()).zfill(6)

    nm = types.ModuleType("app.data_sources.normalizer")
    nm.normalize_cn_code = normalize_cn_code
    nm.normalize_hk_code = normalize_hk_code
    nm.detect_market = detect_market
    nm.to_raw_digits = to_raw_digits
    sys.modules["app.data_sources.normalizer"] = nm

    sys.path.insert(0, os.path.dirname(__file__))
    import rate_limiter as _rl
    arl = types.ModuleType("app.data_sources.rate_limiter")
    for a in dir(_rl):
        if not a.startswith("_"): setattr(arl, a, getattr(_rl, a))
    for fn_name in ("get_tencent_limiter", "get_eastmoney_limiter", "get_sina_limiter"):
        if not hasattr(arl, fn_name):
            setattr(arl, fn_name, lambda: _rl.RateLimiter(5, 1.0))
    if not hasattr(arl, "get_shared_session"):
        import requests as _req; arl.get_shared_session = lambda: _req.Session()
    sys.modules["app.data_sources.rate_limiter"] = arl

    for mod_name, attrs in [
        ("app.utils.trading_calendar", {"trading_days_count": lambda s, e: max((datetime.strptime(e, "%Y-%m-%d") - datetime.strptime(s, "%Y-%m-%d")).days, 1)}),
        ("app.utils.basicinfo_db", {"get_stock_basic_db": lambda: {}}),
        ("app.utils.config_loader", {"load_addon_config": lambda: {}}),
    ]:
        m = types.ModuleType(mod_name)
        for k, v in attrs.items(): setattr(m, k, v)
        sys.modules[mod_name] = m

_build_mock_app()

# ================================================================
# 加载 providers
# ================================================================

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PROVIDER_DIR = os.path.join(PROJECT_ROOT, "provider")
sys.path.insert(0, PROJECT_ROOT)

import logging
logging.disable(logging.CRITICAL)
import provider as _lp; sys.modules["app.data_sources.provider"] = _lp
for f in os.listdir(PROVIDER_DIR):
    if f.endswith(".py"):
        try:
            m = importlib.import_module(f"provider.{f[:-3]}")
            sys.modules[f"app.data_sources.provider.{f[:-3]}"] = m
        except Exception: pass
from provider import _registry  # noqa: E402
for f in sorted(os.listdir(PROVIDER_DIR)):
    if f.endswith(".py") and f != "__init__.py":
        n = f"provider.{f[:-3]}"
        if n not in sys.modules:
            try: importlib.import_module(n)
            except Exception: pass
logging.disable(logging.NOTSET)

# ================================================================
# 读取 provider/config.json 中的 token 配置
# ================================================================

_config_path = os.path.join(PROJECT_ROOT, "provider", "config.json")
if os.path.exists(_config_path):
    with open(_config_path) as _f:
        _cfg = json.load(_f)
    # xueqiu token — 已内置到 xueqiu.py 的 _load_config_token()，无需额外注入
    _xq = _cfg.get("xueqiu", {})
    if _xq.get("xq_a_token"):
        print(f"  ✓ 雪球 token 已配置 (xq_a_token={_xq['xq_a_token'][:8]}...)")
    # twelve_data api_key
    _td = _cfg.get("twelve_data", {})
    if _td.get("api_key"):
        print(f"  ✓ TwelveData API Key 已配置")
    else:
        print(f"  ⚠ TwelveData API Key 未配置，跳过该源")

# ================================================================
# 工具
# ================================================================

W = 120
LINE = "─" * W

def _run(fn, *a, **kw):
    t0 = time.time()
    try:
        r = fn(*a, **kw)
    except Exception as e:
        return {"ok": False, "data": None, "error": str(e), "t": round(time.time() - t0, 2)}
    t = round(time.time() - t0, 2)
    if r is None:
        return {"ok": False, "data": None, "error": "返回 None", "t": t}
    if isinstance(r, dict) and r.get("not_supported"):
        return {"ok": False, "data": None, "error": "不支持", "t": t}
    if hasattr(r, "reason") and hasattr(r, "source") and hasattr(r, "interface"):
        return {"ok": False, "data": None, "error": f"不支持: {r.reason}", "t": t}
    return {"ok": True, "data": r, "error": "", "t": t}


def _fmt(v, decimals=2):
    """格式化数字，大数自动缩写"""
    if v is None or v == "" or v == "—":
        return "—"
    try:
        n = float(v)
    except (ValueError, TypeError):
        return str(v)
    if n == 0:
        return "0"
    if abs(n) >= 1e8:
        return f"{n/1e8:.{decimals}f}亿"
    if abs(n) >= 1e4:
        return f"{n/1e4:.{decimals}f}万"
    return f"{n:.{decimals}f}"


def _bar_field(bar, *keys, default="—"):
    if not isinstance(bar, dict):
        return default
    for k in keys:
        v = bar.get(k)
        if v is not None:
            return v
    return default


def _match_code(data, code):
    """在 dict 中查找 code 对应的值，支持多种 key 格式"""
    if not data or not isinstance(data, dict):
        return None
    # 精确匹配
    if code in data:
        return data[code]
    # 纯数字匹配
    digits = "".join(c for c in str(code) if c.isdigit())
    if digits in data:
        return data[digits]
    # 模糊匹配
    for k, v in data.items():
        k_digits = "".join(c for c in str(k) if c.isdigit())
        if k_digits == digits:
            return v
    return None


# ================================================================
# 测试 1: 单只实时行情 — 每只股票一个表，行=源，列=指标
# ================================================================

def test_ticker(providers, codes):
    print(f"\n  📊 [1/4] fetch_ticker — 单只实时行情\n  {LINE}")

    for code in codes:
        # 收集各源数据
        rows = []
        for p in providers:
            r = _run(p.fetch_ticker, code)
            rows.append((p.name, r, p.priority))

        # 表头
        print(f"\n  ┌─ {code} ─ 实时行情 ─────────────────────────────────────────")
        print(f"  │ {'源':<14} {'优先级':>4} {'最新':>10} {'涨跌':>8} {'涨跌%':>8} {'开盘':>10} {'最高':>10} {'最低':>10} {'昨收':>10} {'成交量':>12} {'耗时':>6}  状态")
        print(f"  │ {'─'*14} {'─'*4} {'─'*10} {'─'*8} {'─'*8} {'─'*10} {'─'*10} {'─'*10} {'─'*10} {'─'*12} {'─'*6}  ─────")

        for name, r, pri in rows:
            if r["ok"]:
                d = r["data"]
                last = _fmt(_bar_field(d, "last", "close", "price"))
                chg = _fmt(_bar_field(d, "change", "涨跌额"))
                chg_pct = _fmt(_bar_field(d, "changePercent", "涨跌幅"))
                opn = _fmt(_bar_field(d, "open", "开盘"))
                high = _fmt(_bar_field(d, "high", "最高"))
                low = _fmt(_bar_field(d, "low", "最低"))
                prev = _fmt(_bar_field(d, "previousClose", "昨收"))
                vol = _fmt(_bar_field(d, "volume", "成交量"), 0)
                ts = f"{r['t']:.2f}s"
                print(f"  │ {name:<14} {pri:>4} {last:>10} {chg:>8} {chg_pct:>8} {opn:>10} {high:>10} {low:>10} {prev:>10} {vol:>12} {ts:>6}  ✅")
            else:
                ts = f"{r['t']:.2f}s"
                err = r["error"][:24]
                print(f"  │ {name:<14} {pri:>4} {'—':>10} {'—':>8} {'—':>8} {'—':>10} {'—':>10} {'—':>10} {'—':>10} {'—':>12} {ts:>6}  ❌ {err}")

        print(f"  └{'─' * (W - 2)}")


# ================================================================
# 测试 2: 批量实时行情 — 每只股票一个表，行=源，列=指标
# ================================================================

def test_batch_quotes(providers, codes):
    print(f"\n  📊 [2/4] fetch_batch_quotes — 批量实时行情\n  {LINE}")

    # 先收集各源的批量结果
    source_results = {}
    for p in providers:
        r = _run(p.fetch_batch_quotes, codes)
        source_results[p.name] = (r, p.priority)

    # 按股票出表
    for code in codes:
        print(f"\n  ┌─ {code} ─ 批量行情 ─────────────────────────────────────────")
        print(f"  │ {'源':<14} {'优先级':>4} {'最新':>10} {'涨跌':>8} {'涨跌%':>8} {'开盘':>10} {'最高':>10} {'最低':>10} {'成交量':>12} {'耗时':>6}  状态")
        print(f"  │ {'─'*14} {'─'*4} {'─'*10} {'─'*8} {'─'*8} {'─'*10} {'─'*10} {'─'*10} {'─'*12} {'─'*6}  ─────")

        for name, (r, pri) in source_results.items():
            ts = f"{r['t']:.2f}s"
            if not r["ok"]:
                err = r["error"][:24]
                print(f"  │ {name:<14} {pri:>4} {'—':>10} {'—':>8} {'—':>8} {'—':>10} {'—':>10} {'—':>10} {'—':>12} {ts:>6}  ❌ {err}")
                continue

            d = _match_code(r["data"], code)
            if d:
                last = _fmt(_bar_field(d, "last", "close", "price"))
                chg = _fmt(_bar_field(d, "change"))
                chg_pct = _fmt(_bar_field(d, "changePercent"))
                opn = _fmt(_bar_field(d, "open"))
                high = _fmt(_bar_field(d, "high"))
                low = _fmt(_bar_field(d, "low"))
                vol = _fmt(_bar_field(d, "volume"), 0)
                print(f"  │ {name:<14} {pri:>4} {last:>10} {chg:>8} {chg_pct:>8} {opn:>10} {high:>10} {low:>10} {vol:>12} {ts:>6}  ✅")
            else:
                print(f"  │ {name:<14} {pri:>4} {'—':>10} {'—':>8} {'—':>8} {'—':>10} {'—':>10} {'—':>10} {'—':>12} {ts:>6}  ⚠️ 无数据")

        print(f"  └{'─' * (W - 2)}")


# ================================================================
# 测试 3: 单只K线 — 每只股票×每个日期一个表，行=源，列=OHLCV
# ================================================================

def test_kline(providers, codes, timeframe="1D", count=5):
    tf_name = {"1D": "日线", "1W": "周线", "1M": "月线", "15m": "15分钟", "5m": "5分钟"}.get(timeframe, timeframe)
    print(f"\n  📊 [3/4] fetch_kline — {tf_name} (最近{count}条)\n  {LINE}")

    for code in codes:
        # 收集各源数据
        results = {}
        for p in providers:
            r = _run(p.fetch_kline, code, timeframe, count, "qfq", 15)
            results[p.name] = r

        # 提取有效数据
        has_data = {}
        for name, r in results.items():
            if r["ok"] and isinstance(r["data"], list) and r["data"]:
                has_data[name] = r["data"]

        if not has_data:
            print(f"\n  ┌─ {code} ─ {tf_name} ─ 无任何源返回数据 ─────────────────")
            for name, r in results.items():
                icon = "❌" if not r["ok"] else "⚠️"
                print(f"  │ {name:<14} {icon} {r['error'] or '空数据'}")
            print(f"  └{'─' * (W - 2)}")
            continue

        # 收集所有日期
        all_dates = set()
        for bars in has_data.values():
            for b in bars:
                d = _bar_field(b, "time", "date", "datetime")
                if d != "—":
                    all_dates.add(str(d))
        sorted_dates = sorted(all_dates)[-count:]

        # 构建 源名->日期->bar 映射
        date_maps = {}
        for name, bars in has_data.items():
            dm = {}
            for b in bars:
                d = str(_bar_field(b, "time", "date", "datetime"))
                dm[d] = b
            date_maps[name] = dm

        src_names = sorted(has_data.keys())

        # ── 状态行 ──
        print(f"\n  ┌─ {code} ─ {tf_name} ─────────────────────────────────────")
        status_parts = []
        for p in providers:
            r = results[p.name]
            if r["ok"]:
                n = len(r["data"]) if isinstance(r["data"], list) else "?"
                status_parts.append(f"{p.name}={n}条({r['t']:.1f}s)")
            else:
                status_parts.append(f"{p.name}=❌")
        print(f"  │ {' | '.join(status_parts)}")

        # ── 每个日期一个小表: 行=源, 列=OHLCV ──
        for dt in sorted_dates:
            print(f"  │")
            print(f"  │  📅 {dt}")
            print(f"  │  {'源':<14} {'开盘':>10} {'最高':>10} {'最低':>10} {'收盘':>10} {'成交量':>12}")
            print(f"  │  {'─'*14} {'─'*10} {'─'*10} {'─'*10} {'─'*10} {'─'*12}")

            for name in src_names:
                bar = date_maps[name].get(dt)
                if bar:
                    o = _fmt(_bar_field(bar, "open"), 2)
                    h = _fmt(_bar_field(bar, "high"), 2)
                    l = _fmt(_bar_field(bar, "low"), 2)
                    c = _fmt(_bar_field(bar, "close"), 2)
                    v = _fmt(_bar_field(bar, "volume"), 0)
                    print(f"  │  {name:<14} {o:>10} {h:>10} {l:>10} {c:>10} {v:>12}")
                else:
                    print(f"  │  {name:<14} {'·':>10} {'·':>10} {'·':>10} {'·':>10} {'·':>12}")

        # ── 差异汇总: 最新日期各源收盘价对比 ──
        if len(src_names) > 1:
            latest = sorted_dates[-1]
            closes = {}
            for name in src_names:
                bar = date_maps[name].get(latest)
                if bar:
                    cv = _bar_field(bar, "close")
                    if cv != "—":
                        try:
                            closes[name] = float(cv)
                        except (ValueError, TypeError):
                            pass
            if len(closes) > 1:
                vals = list(closes.values())
                mx, mn = max(vals), min(vals)
                diff = mx - mn
                diff_pct = diff / mn * 100 if mn else 0
                print(f"  │")
                parts = [f"{n}={v}" for n, v in closes.items()]
                print(f"  │  ⚡ {latest} 收盘差: {diff:.4f} ({diff_pct:.3f}%) → {' vs '.join(parts)}")

        print(f"  └{'─' * (W - 2)}")


# ================================================================
# 测试 4: 批量K线 — 每只股票一个表，行=源，列=指标
# ================================================================

def test_market_kline(providers, codes):
    symbols = codes[:3]
    print(f"\n  📊 [4/4] fetch_market_kline — 批量日K线 (symbols={symbols})\n  {LINE}")

    # 收集各源数据
    source_results = {}
    for p in providers:
        if not hasattr(p, "fetch_market_kline"):
            source_results[p.name] = (None, p.priority, "不支持该接口")
            continue
        r = _run(p.fetch_market_kline, "1D", 3, "qfq", 15, "", "", symbols)
        source_results[p.name] = (r, p.priority, "")

    # 按股票出表
    for code in symbols:
        print(f"\n  ┌─ {code} ─ 批量日K线 ─────────────────────────────────────")
        print(f"  │ {'源':<14} {'优先级':>4} {'条数':>4} {'最新日期':>12} {'最新收盘':>10} {'耗时':>6}  状态")
        print(f"  │ {'─'*14} {'─'*4} {'─'*4} {'─'*12} {'─'*10} {'─'*6}  ─────")

        for name, (r, pri, skip_reason) in source_results.items():
            if skip_reason:
                print(f"  │ {name:<14} {pri:>4} {'—':>4} {'—':>12} {'—':>10} {'—':>6}  ⏭ {skip_reason}")
                continue
            if r is None:
                continue

            ts = f"{r['t']:.2f}s"
            if not r["ok"]:
                err = r["error"][:24]
                print(f"  │ {name:<14} {pri:>4} {'—':>4} {'—':>12} {'—':>10} {ts:>6}  ❌ {err}")
                continue

            data = r["data"]
            bars = _match_code(data, code)
            if bars and isinstance(bars, list) and len(bars) > 0:
                last_bar = bars[-1]
                n = len(bars)
                dt = _fmt(_bar_field(last_bar, "time", "date"))
                close = _fmt(_bar_field(last_bar, "close"))
                print(f"  │ {name:<14} {pri:>4} {n:>4} {dt:>12} {close:>10} {ts:>6}  ✅")
            else:
                print(f"  │ {name:<14} {pri:>4} {'0':>4} {'—':>12} {'—':>10} {ts:>6}  ⚠️ 无数据")

        print(f"  └{'─' * (W - 2)}")


# ================================================================
# Main
# ================================================================

ALL_TESTS = ["ticker", "batch", "kline", "market_kline"]
TEST_MAP = {
    "ticker": test_ticker,
    "batch": test_batch_quotes,
    "kline": test_kline,
    "market_kline": test_market_kline,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="数据源 Provider 全接口测试")
    parser.add_argument("code", nargs="*", default=[], help="股票代码 (默认 600519 000001 300750)")
    parser.add_argument("--only", choices=ALL_TESTS, help="只跑某个测试")
    parser.add_argument("--tf", default="1D", help="K线周期 (默认 1D)")
    parser.add_argument("--count", type=int, default=5, help="K线条数 (默认 5)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    codes = args.code if args.code else ["600519", "000001", "300750"]

    providers = sorted(_registry.values(), key=lambda p: p.priority)
    if not providers:
        print("❌ 没有已注册的 Provider"); sys.exit(1)

    print(f"\n{'='*W}")
    print(f"  数据源全接口测试  |  代码: {codes}  |  Provider: {len(providers)}个  |  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  源列表: {', '.join(p.name for p in providers)}")
    print(f"{'='*W}")

    run_order = [args.only] if args.only else ALL_TESTS
    try:
        for name in run_order:
            if name == "ticker":
                test_ticker(providers, codes)
            elif name == "batch":
                test_batch_quotes(providers, codes)
            elif name == "kline":
                test_kline(providers, codes, args.tf, args.count)
            elif name == "market_kline":
                test_market_kline(providers, codes)
    except Exception as e:
        if args.debug:
            import traceback; traceback.print_exc()
        else:
            print(f"\n❌ 出错: {e}  (用 --debug 看堆栈)")
        sys.exit(1)

    print(f"\n{'='*W}")
    print("  测试完成")
    print(f"{'='*W}\n")
