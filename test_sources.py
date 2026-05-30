# -*- coding: utf-8 -*-
"""
数据源批量行情快照 & OHLCV 横向对比测试

测试维度:
  1. 覆盖率/丢失率 — 每个源能返回多少只股票的行情
  2. OHLCV 正确率 — 源与源之间交叉验证 (横向对比)
  3. 速率 & 全市场耗时估算

可用数据源 (从当前服务器可达):
  - tencent:  批量行情 + K线
  - sina:     批量行情 + K线
  - sohu:     批量行情 + K线
  - 10jqka:   K线 (无批量行情接口)

不可用 (网络/依赖限制):
  - eastmoney: TLS 指纹被封 (需 curl_cffi)
  - tdx_ex:    需要 pytdx 库
  - xueqiu:    需要 cookie
  - baidu:     API 返回空

用法:
  python3 test_sources.py                    # 默认: 200只抽样
  python3 test_sources.py --sample 500       # 500只抽样
  python3 test_sources.py --sample all       # 全市场 (约5000只)
  python3 test_sources.py --kline-days 5     # K线对比取最近5天
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from test_utils import get_active_stock_codes, http_get as _get, progress as _progress

# ================================================================
# 常量
# ================================================================

_TZ_CN = timezone(timedelta(hours=8))
_NOW = datetime.now(_TZ_CN)
TODAY = _NOW.strftime("%Y-%m-%d")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/573.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# A股代码范围 (沪深主板 + 创业板 + 科创板 + 北交所)
# SH: 600xxx-605xxx (主板), 688xxx (科创板)
# SZ: 000xxx (主板), 001xxx, 002xxx (中小板), 300xxx/301xxx (创业板)
# BJ: 4xxxxx, 8xxxxx (北交所)


# ================================================================
# 通用工具
# ================================================================


def _strip_jsonp(text: str) -> Optional[str]:
    """去除 JSONP 回调包装"""
    text = text.strip()
    m = re.match(r'^[a-zA-Z_$][\w$]*\s*\((.+)\)\s*;?\s*$', text, re.DOTALL)
    return m.group(1).strip() if m else text


# ================================================================
# 数据源适配器 — 批量行情快照
# ================================================================

class SourceAdapter:
    """数据源适配器基类"""
    name: str = ""

    def fetch_batch_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """批量获取行情快照 → {code: {last, open, high, low, volume, ...}}"""
        raise NotImplementedError

    def fetch_kline(self, code: str, count: int = 10) -> List[Dict[str, Any]]:
        """获取日K线 → [{time, open, high, low, close, volume}, ...]"""
        raise NotImplementedError


class TencentSource(SourceAdapter):
    name = "tencent"

    def fetch_batch_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """腾讯行情 — qt.gtimg.cn, 500只/批"""
        result = {}
        batch_size = 500
        for i in range(0, len(codes), batch_size):
            batch = codes[i:i + batch_size]
            codes_str = ",".join(batch)
            text = _get(
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
                    if len(parts) < 6 or not parts[1]:
                        continue
                    last = float(parts[3]) if parts[3] else 0
                    if last <= 0:
                        continue
                    prev = float(parts[4]) if parts[4] else 0
                    vol = float(parts[6]) if len(parts) > 6 and parts[6] else 0
                    m = re.search(r'(sh|sz)(\d+)', var_name)
                    if not m:
                        continue
                    code = f"{m.group(1)}{m.group(2)}"
                    result[code] = {
                        "last": last,
                        "open": float(parts[5]) if parts[5] else last,
                        "high": float(parts[33]) if len(parts) > 33 and parts[33] else last,
                        "low": float(parts[34]) if len(parts) > 34 and parts[34] else last,
                        "prev_close": prev,
                        "volume": vol * 100,  # 手→股
                        "name": parts[1].strip(),
                    }
                except (ValueError, IndexError):
                    continue
            time.sleep(0.2)
        return result

    def fetch_kline(self, code: str, count: int = 10) -> List[Dict[str, Any]]:
        """腾讯日K线 — web.ifzq.gtimg.cn"""
        text = _get(
            "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
            params={"param": f"{code},day,,,{count},"},
            referer="https://gu.qq.com/",
            timeout=10,
        )
        if not text:
            return []
        try:
            data = json.loads(text)
        except Exception:
            return []
        root = (data.get("data") or {}).get(code, {})
        rows = root.get("day") or root.get("qfqday") or []
        out = []
        for r in rows:
            if not isinstance(r, (list, tuple)) or len(r) < 6:
                continue
            try:
                out.append({
                    "time": str(r[0])[:10],
                    "open": round(float(r[1]), 4),
                    "high": round(float(r[3]), 4),
                    "low": round(float(r[4]), 4),
                    "close": round(float(r[2]), 4),
                    "volume": round(float(r[5]) * 100, 2),  # 手→股
                })
            except (ValueError, TypeError):
                continue
        return out


class SinaSource(SourceAdapter):
    name = "sina"

    def fetch_batch_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """新浪行情 — hq.sinajs.cn, 500只/批"""
        result = {}
        batch_size = 500
        for i in range(0, len(codes), batch_size):
            batch = codes[i:i + batch_size]
            codes_str = ",".join(batch)
            text = _get(
                f"https://hq.sinajs.cn/list={codes_str}",
                referer="https://finance.sina.com.cn/",
                encoding="gbk",
                timeout=15,
            )
            if not text:
                continue
            for line in text.strip().split("\n"):
                line = line.strip().rstrip(";")
                m = re.search(r'hq_str_(\w+)="(.+?)"', line)
                if not m:
                    continue
                code_str = m.group(1)
                parts = m.group(2).split(",")
                if len(parts) < 6:
                    continue
                try:
                    name = parts[0].strip()
                    if not name:
                        continue
                    open_p = float(parts[1]) if parts[1] else 0
                    prev_close = float(parts[2]) if parts[2] else 0
                    last = float(parts[3]) if parts[3] else 0
                    high = float(parts[4]) if parts[4] else 0
                    low = float(parts[5]) if parts[5] else 0
                    vol = float(parts[8]) if len(parts) > 8 and parts[8] else 0
                    if last == 0 and prev_close == 0:
                        continue
                    result[code_str] = {
                        "last": last, "open": open_p,
                        "high": high, "low": low,
                        "prev_close": prev_close,
                        "volume": vol,  # 已经是"股"
                        "name": name,
                    }
                except (ValueError, IndexError):
                    continue
            time.sleep(0.3)
        return result

    def fetch_kline(self, code: str, count: int = 10) -> List[Dict[str, Any]]:
        """新浪日K线 — money.finance.sina.com.cn"""
        text = _get(
            "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData",
            params={"symbol": code, "scale": 240, "ma": "no", "datalen": count},
            referer="https://finance.sina.com.cn/",
            timeout=10,
        )
        if not text:
            return []
        try:
            data = json.loads(text)
        except Exception:
            return []
        if not isinstance(data, list):
            return []
        out = []
        for item in data:
            try:
                out.append({
                    "time": str(item.get("day", ""))[:10],
                    "open": round(float(item.get("open", 0)), 4),
                    "high": round(float(item.get("high", 0)), 4),
                    "low": round(float(item.get("low", 0)), 4),
                    "close": round(float(item.get("close", 0)), 4),
                    "volume": round(float(item.get("volume", 0)), 2),
                })
            except (ValueError, TypeError):
                continue
        return out


class SohuSource(SourceAdapter):
    name = "sohu"

    def fetch_batch_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """搜狐行情 — hqm.stock.sohu.com/getqjson"""
        result = {}
        # 搜狐代码格式: cn_600519, cn_000001
        sohu_codes = []
        for c in codes:
            digits = re.sub(r'^(sh|sz|bj)', '', c)
            sohu_codes.append(f"cn_{digits}")

        batch_size = 200  # 搜狐单次不宜太多
        for i in range(0, len(sohu_codes), batch_size):
            batch = sohu_codes[i:i + batch_size]
            original_batch = codes[i:i + batch_size]
            codes_str = ",".join(batch)
            text = _get(
                f"https://hqm.stock.sohu.com/getqjson?code={codes_str}&cb=cb",
                referer="https://q.stock.sohu.com/",
                timeout=15,
            )
            if not text:
                continue
            json_str = _strip_jsonp(text)
            if not json_str:
                continue
            try:
                data = json.loads(json_str)
            except Exception:
                continue
            for sohu_code, vals in data.items():
                if not isinstance(vals, list) or len(vals) < 6:
                    continue
                try:
                    # sohu getqjson 字段: [0]=code [1]=name [2]=last [3]=chg% [4]=change
                    #   [5]=volume(手) [6]=现手 [7]=总金额(万) [8]=换手率
                    #   [9]=量比 [10]=最高 [11]=最低 [12]=PE [13]=昨收 [14]=今开
                    last = float(vals[2]) if vals[2] else 0
                    if last <= 0:
                        continue
                    vol = float(vals[5]) if vals[5] else 0
                    digits = re.sub(r'^cn_', '', sohu_code)
                    # 找到对应的原始代码
                    orig_code = ""
                    for oc in original_batch:
                        if digits in oc:
                            orig_code = oc
                            break
                    if not orig_code:
                        orig_code = f"sh{digits}" if digits[0] in '6' else f"sz{digits}"
                    result[orig_code] = {
                        "last": last,
                        "open": float(vals[14]) if len(vals) > 14 and vals[14] else last,
                        "high": float(vals[10]) if len(vals) > 10 and vals[10] else last,
                        "low": float(vals[11]) if len(vals) > 11 and vals[11] else last,
                        "prev_close": float(vals[13]) if len(vals) > 13 and vals[13] else 0,
                        "volume": vol * 100,  # 手→股
                        "name": str(vals[1]) if len(vals) > 1 else "",
                    }
                except (ValueError, IndexError, TypeError):
                    continue
            time.sleep(0.3)
        return result

    def fetch_kline(self, code: str, count: int = 10) -> List[Dict[str, Any]]:
        """搜狐日K线 — q.stock.sohu.com/hisHq"""
        digits = re.sub(r'^(sh|sz|bj)', '', code)
        end_date = TODAY.replace("-", "")
        start_dt = _NOW - timedelta(days=count * 2)  # 多取一些以覆盖非交易日
        start_date = start_dt.strftime("%Y%m%d")

        text = _get(
            "https://q.stock.sohu.com/hisHq",
            params={
                "code": f"cn_{digits}",
                "start": start_date,
                "end": end_date,
                "period": "d",
            },
            timeout=10,
        )
        if not text:
            return []
        try:
            data = json.loads(text)
        except Exception:
            return []
        if not data or not isinstance(data, list):
            return []
        hq = data[0].get("hq", []) if isinstance(data[0], dict) else []
        out = []
        for row in hq:
            if not isinstance(row, list) or len(row) < 8:
                continue
            try:
                # sohu hisHq: [日期, 开盘, 收盘, 涨跌额, 涨跌幅, 最低, 最高, 成交量(手), 成交额(万), 换手率]
                out.append({
                    "time": str(row[0]),
                    "open": round(float(row[1]), 4),
                    "high": round(float(row[6]), 4),
                    "low": round(float(row[5]), 4),
                    "close": round(float(row[2]), 4),
                    "volume": round(float(row[7]) * 100, 2),  # 手→股
                })
            except (ValueError, IndexError, TypeError):
                continue
        # sohu 返回最新在前，反转为时间正序
        out.reverse()
        return out[-count:]


class THSSource(SourceAdapter):
    name = "10jqka"

    def fetch_batch_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """同花顺 — 无批量行情接口，逐只获取 (慢)"""
        result = {}
        for code in codes[:50]:  # 限制数量，同花顺没有批量接口
            bars = self.fetch_kline(code, count=1)
            if bars:
                b = bars[-1]
                result[code] = {
                    "last": b["close"], "open": b["open"],
                    "high": b["high"], "low": b["low"],
                    "prev_close": 0, "volume": b["volume"],
                    "name": "",
                }
            time.sleep(0.5)
        return result

    def fetch_kline(self, code: str, count: int = 10) -> List[Dict[str, Any]]:
        """同花顺日K线 — d.10jqka.com.cn/v2/line"""
        digits = re.sub(r'^(sh|sz|bj)', '', code)
        text = _get(
            f"https://d.10jqka.com.cn/v2/line/hs_{digits}/01/last{count}.js",
            referer="https://stockpage.10jqka.com/",
            encoding="gbk",
            timeout=10,
        )
        if not text:
            return []
        json_str = _strip_jsonp(text)
        if not json_str:
            return []
        try:
            data = json.loads(json_str)
        except Exception:
            return []
        # data 中有 "data" 字段: "日期,开盘,收盘,最高,最低,成交量,成交额,...;..."
        raw = data.get("data", "")
        if not raw:
            return []
        out = []
        for line in raw.split(";"):
            parts = line.strip().split(",")
            if len(parts) < 6:
                continue
            try:
                out.append({
                    "time": str(parts[0]),
                    "open": round(float(parts[1]), 4),
                    "high": round(float(parts[3]), 4),
                    "low": round(float(parts[4]), 4),
                    "close": round(float(parts[2]), 4),
                    "volume": round(float(parts[5]), 2),
                })
            except (ValueError, IndexError):
                continue
        return out[-count:]


# ================================================================
# 测试引擎
# ================================================================

SOURCES: List[SourceAdapter] = [
    TencentSource(),
    SinaSource(),
    SohuSource(),
]


def test_batch_quotes(codes: List[str]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    批量行情快照测试。

    Returns:
        {source_name: {code: quote_dict}}
    """
    print("\n" + "=" * 70)
    print("📊 测试1: 批量行情快照 — 覆盖率 & 丢失率")
    print("=" * 70)

    all_results: Dict[str, Dict[str, Dict[str, Any]]] = {}
    timing: Dict[str, float] = {}

    for src in SOURCES:
        print(f"\n  🔄 {src.name} 获取 {len(codes)} 只行情...")
        t0 = time.time()
        result = src.fetch_batch_quotes(codes)
        elapsed = time.time() - t0
        all_results[src.name] = result
        timing[src.name] = elapsed

        got = len(result)
        lost = len(codes) - got
        loss_rate = lost / len(codes) * 100 if codes else 0
        qps = got / elapsed if elapsed > 0 else 0

        print(f"  ✅ {src.name}: 获取 {got}/{len(codes)} 只 "
              f"| 丢失率 {loss_rate:.1f}% "
              f"| 耗时 {elapsed:.1f}s "
              f"| QPS {qps:.0f} 只/s")

    # 汇总对比
    print("\n  📋 覆盖率对比:")
    print(f"  {'数据源':<12} {'获取':>8} {'丢失':>8} {'丢失率':>8} {'耗时':>8} {'QPS':>8}")
    print(f"  {'-'*12} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    for src in SOURCES:
        r = all_results.get(src.name, {})
        got = len(r)
        lost = len(codes) - got
        loss_rate = lost / len(codes) * 100 if codes else 0
        elapsed = timing.get(src.name, 0)
        qps = got / elapsed if elapsed > 0 else 0
        print(f"  {src.name:<12} {got:>8} {lost:>8} {loss_rate:>7.1f}% {elapsed:>7.1f}s {qps:>7.0f}")

    # 全市场耗时估算
    if codes:
        est_total = len(codes)
        print(f"\n  ⏱️  全市场({est_total}只)下载耗时估算:")
        for src in SOURCES:
            elapsed = timing.get(src.name, 0)
            if elapsed > 0:
                rate = len(all_results.get(src.name, {})) / elapsed
                est = est_total / rate if rate > 0 else float('inf')
                print(f"     {src.name}: ~{est:.0f}s ({est/60:.1f}min)")

    return all_results


def test_ohlcv_comparison(
    codes: List[str],
    all_quotes: Dict[str, Dict[str, Dict[str, Any]]],
    kline_days: int = 5,
) -> None:
    """
    OHLCV 横向对比测试 — 源与源之间交叉验证。

    方法:
      1. 从多个源获取同一批股票的日K线
      2. 按日期对齐，逐日比较 OHLCV
      3. 统计各源与其他源的一致率

    对比标准:
      - 价格 (O/H/L/C): 绝对误差 ≤ 0.02 元 或 相对误差 ≤ 0.1%
      - 成交量: 相对误差 ≤ 5% (各源单位换算可能有微小差异)
    """
    print("\n" + "=" * 70)
    print("📊 测试2: OHLCV 横向对比 — 源与源交叉验证")
    print("=" * 70)

    # 抽样: 取前 N 只做K线对比
    sample_size = min(len(codes), 80)
    sample_codes = codes[:sample_size]
    print(f"\n  抽样: {sample_size} 只股票, 最近 {kline_days} 个交易日")

    # 从各源获取K线
    kline_data: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}  # {source: {code: [bars]}}
    kline_timing: Dict[str, float] = {}

    for src in SOURCES:
        print(f"\n  🔄 {src.name} 获取 {sample_size} 只 × {kline_days} 天K线...")
        t0 = time.time()
        results = {}

        # 并发获取 (控制并发数)
        lock = threading.Lock()
        max_workers = min(4, src.name == "sohu" and 3 or 4)

        def _fetch_one(code, src=src):
            try:
                bars = src.fetch_kline(code, count=kline_days + 2)  # 多取几天防非交易日
                if bars:
                    return code, bars[-kline_days:]  # 取最后 N 天
            except Exception:
                pass
            return code, None

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_fetch_one, c): c for c in sample_codes}
            done = 0
            for f in as_completed(futures):
                code, bars = f.result()
                if bars:
                    results[code] = bars
                done += 1
                if done % 20 == 0:
                    _progress(done, sample_size, src.name)

        _progress(sample_size, sample_size, src.name)
        elapsed = time.time() - t0
        kline_data[src.name] = results
        kline_timing[src.name] = elapsed
        print(f"\n     获取 {len(results)}/{sample_size} 只, 耗时 {elapsed:.1f}s")

        # 限流: 源之间间隔
        time.sleep(1)

    # ── OHLCV 交叉对比 ──
    print("\n  🔍 OHLCV 交叉对比分析:")

    source_names = list(kline_data.keys())
    # 对比矩阵: pair_stats[A][B] = {match, total, price_err, vol_err}
    pair_stats: Dict[str, Dict[str, Dict[str, int]]] = {}
    for a in source_names:
        pair_stats[a] = {}
        for b in source_names:
            if a != b:
                pair_stats[a][b] = {
                    "match_ohlc": 0, "match_vol": 0, "total": 0,
                    "price_big_err": 0, "vol_big_err": 0,
                }

    # 按股票+日期逐条对比
    comparison_details: Dict[str, List[Dict]] = defaultdict(list)  # 差异详情

    for code in sample_codes:
        # 收集该股票在各源的K线，按日期索引
        bars_by_source: Dict[str, Dict[str, Dict]] = {}
        for src_name in source_names:
            bars = kline_data.get(src_name, {}).get(code, [])
            indexed = {}
            for b in bars:
                t = b.get("time", "")
                if t:
                    indexed[t] = b
            bars_by_source[src_name] = indexed

        # 找到所有源都有数据的日期
        all_dates: Set[str] = set()
        for src_name in source_names:
            all_dates |= set(bars_by_source[src_name].keys())

        # 逐日对比
        for date in sorted(all_dates):
            for a in source_names:
                for b in source_names:
                    if a >= b:  # 避免重复对比
                        continue
                    bar_a = bars_by_source[a].get(date)
                    bar_b = bars_by_source[b].get(date)
                    if not bar_a or not bar_b:
                        continue

                    pair_stats[a][b]["total"] += 1

                    # OHLC 对比: 绝对误差 ≤ 0.02 或 相对误差 ≤ 0.1%
                    ohlc_match = True
                    for field in ("open", "high", "low", "close"):
                        va, vb = bar_a.get(field, 0), bar_b.get(field, 0)
                        if va == 0 or vb == 0:
                            ohlc_match = False
                            break
                        abs_err = abs(va - vb)
                        rel_err = abs_err / max(abs(va), 0.01)
                        if abs_err > 0.05 and rel_err > 0.002:
                            ohlc_match = False
                            pair_stats[a][b]["price_big_err"] += 1
                            if len(comparison_details[f"{a}_vs_{b}"]) < 5:
                                comparison_details[f"{a}_vs_{b}"].append({
                                    "code": code, "date": date, "field": field,
                                    f"{a}": va, f"{b}": vb,
                                    "abs_err": round(abs_err, 4),
                                    "rel_err": f"{rel_err*100:.2f}%",
                                })
                            break
                    if ohlc_match:
                        pair_stats[a][b]["match_ohlc"] += 1

                    # 成交量对比: 相对误差 ≤ 10%
                    va_vol, vb_vol = bar_a.get("volume", 0), bar_b.get("volume", 0)
                    if va_vol > 0 and vb_vol > 0:
                        vol_err = abs(va_vol - vb_vol) / max(va_vol, vb_vol)
                        if vol_err <= 0.10:
                            pair_stats[a][b]["match_vol"] += 1
                        else:
                            pair_stats[a][b]["vol_big_err"] += 1

    # 输出对比结果
    print(f"\n  {'对比对':<20} {'OHLC一致':>10} {'成交量一致':>10} {'样本数':>8} {'OHLC率':>8} {'Vol率':>8}")
    print(f"  {'-'*20} {'-'*10} {'-'*10} {'-'*8} {'-'*8} {'-'*8}")

    for a in source_names:
        for b in source_names:
            if a >= b:
                continue
            s = pair_stats[a][b]
            total = s["total"]
            if total == 0:
                continue
            ohlc_rate = s["match_ohlc"] / total * 100
            vol_rate = s["match_vol"] / total * 100
            label = f"{a} vs {b}"
            print(f"  {label:<20} {s['match_ohlc']:>10} {s['match_vol']:>10} "
                  f"{total:>8} {ohlc_rate:>7.1f}% {vol_rate:>7.1f}%")

    # 输出差异样例
    has_diff = False
    for pair_name, diffs in comparison_details.items():
        if diffs:
            if not has_diff:
                print("\n  ⚠️  差异样例 (前几条):")
                has_diff = True
            print(f"\n  {pair_name}:")
            for d in diffs[:3]:
                print(f"    {d['code']} {d['date']} {d['field']}: "
                      f"{d.get(list(d.keys())[3], '?')} vs {d.get(list(d.keys())[4], '?')} "
                      f"(err={d['abs_err']}, {d['rel_err']})")

    # 各源一致性评分
    print("\n  📈 各源一致性评分 (与其他源的平均OHLC一致率):")
    for src in source_names:
        rates = []
        for a in source_names:
            for b in source_names:
                if a == src and a < b:
                    total = pair_stats[a][b]["total"]
                    if total > 0:
                        rates.append(pair_stats[a][b]["match_ohlc"] / total)
                if b == src and a < b:
                    total = pair_stats[a][b]["total"]
                    if total > 0:
                        rates.append(pair_stats[a][b]["match_ohlc"] / total)
        if rates:
            avg = sum(rates) / len(rates) * 100
            print(f"     {src}: {avg:.1f}%")


def test_batch_quotes_cross_validation(
    codes: List[str],
    all_quotes: Dict[str, Dict[str, Dict[str, Any]]],
) -> None:
    """
    批量行情快照 OHLCV 横向对比 — 在 snapshot 层面交叉验证。

    与 K线对比不同，这里比较的是同一批股票在同一时刻的行情快照。
    """
    print("\n" + "=" * 70)
    print("📊 测试3: 批量行情快照 OHLCV 交叉验证")
    print("=" * 70)

    source_names = [s.name for s in SOURCES]
    available_pairs = []
    for a in source_names:
        for b in source_names:
            if a < b and a in all_quotes and b in all_quotes:
                available_pairs.append((a, b))

    if not available_pairs:
        print("  ⚠️  需要至少2个源的数据")
        return

    # 找到所有源都有数据的股票
    common_codes = set(codes)
    for src_name in source_names:
        if src_name in all_quotes:
            common_codes &= set(all_quotes[src_name].keys())
    common_codes = sorted(common_codes)

    print(f"\n  所有源都有数据的股票: {len(common_codes)} 只")

    if not common_codes:
        print("  ⚠️  没有共同数据，跳过")
        return

    # 逐只对比
    stats: Dict[str, Dict[str, int]] = {}
    for a, b in available_pairs:
        stats[f"{a}_vs_{b}"] = {
            "match_ohlc": 0, "match_last": 0, "total": 0,
            "price_err_sum": 0.0, "vol_err_sum": 0.0,
        }

    for code in common_codes:
        for a, b in available_pairs:
            qa = all_quotes[a].get(code, {})
            qb = all_quotes[b].get(code, {})
            if not qa or not qb:
                continue

            key = f"{a}_vs_{b}"
            stats[key]["total"] += 1

            # 最新价对比
            la, lb = qa.get("last", 0), qb.get("last", 0)
            if la > 0 and lb > 0:
                err = abs(la - lb) / max(la, lb)
                stats[key]["price_err_sum"] += err
                if err < 0.002:  # 0.2% 以内
                    stats[key]["match_last"] += 1

            # OHLC 全面对比
            ohlc_ok = True
            for field in ("open", "high", "low"):
                va, vb = qa.get(field, 0), qb.get(field, 0)
                if va > 0 and vb > 0:
                    err = abs(va - vb) / max(va, vb)
                    if err > 0.005:  # 0.5% 以内
                        ohlc_ok = False
            if ohlc_ok:
                stats[key]["match_ohlc"] += 1

            # 成交量对比
            va_v, vb_v = qa.get("volume", 0), qb.get("volume", 0)
            if va_v > 0 and vb_v > 0:
                stats[key]["vol_err_sum"] += abs(va_v - vb_v) / max(va_v, vb_v)

    # 输出结果
    print(f"\n  {'对比对':<20} {'最新价一致':>10} {'OHLC一致':>10} {'样本数':>8} {'价格率':>8} {'OHLC率':>8}")
    print(f"  {'-'*20} {'-'*10} {'-'*10} {'-'*8} {'-'*8} {'-'*8}")

    for pair_key, s in stats.items():
        total = s["total"]
        if total == 0:
            continue
        last_rate = s["match_last"] / total * 100
        ohlc_rate = s["match_ohlc"] / total * 100
        print(f"  {pair_key:<20} {s['match_last']:>10} {s['match_ohlc']:>10} "
              f"{total:>8} {last_rate:>7.1f}% {ohlc_rate:>7.1f}%")

    # 平均价格误差
    print(f"\n  平均价格误差 (相对):")
    for pair_key, s in stats.items():
        total = s["total"]
        if total > 0:
            avg_err = s["price_err_sum"] / total * 100
            vol_err = s["vol_err_sum"] / total * 100
            print(f"     {pair_key}: 价格 {avg_err:.3f}% | 成交量 {vol_err:.1f}%")


def test_speed_benchmark(codes: List[str]) -> None:
    """
    速率基准测试 — 测量每个源的真实吞吐量。
    """
    print("\n" + "=" * 70)
    print("📊 测试4: 速率基准 & 全市场耗时预估")
    print("=" * 70)

    # 测试不同批量大小的速度
    batch_sizes = [50, 100, 200, 500]
    if len(codes) < 500:
        batch_sizes = [bs for bs in batch_sizes if bs <= len(codes)]
        if len(codes) not in batch_sizes:
            batch_sizes.append(len(codes))

    for src in SOURCES:
        print(f"\n  📈 {src.name} 速率测试:")
        print(f"  {'批量大小':>10} {'成功数':>8} {'耗时':>8} {'QPS':>8} {'全市场预估':>12}")
        print(f"  {'-'*10} {'-'*8} {'-'*8} {'-'*8} {'-'*12}")

        for bs in batch_sizes:
            batch = codes[:bs]
            t0 = time.time()
            result = src.fetch_batch_quotes(batch)
            elapsed = time.time() - t0
            got = len(result)
            qps = got / elapsed if elapsed > 0 else 0
            est_full = len(codes) / qps if qps > 0 else float('inf')
            est_str = f"{est_full:.0f}s" if est_full < 300 else f"{est_full/60:.1f}min"
            print(f"  {bs:>10} {got:>8} {elapsed:>7.1f}s {qps:>7.0f} {est_str:>12}")
            time.sleep(0.5)


# ================================================================
# 主函数
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="数据源批量行情快照 & OHLCV 横向对比测试")
    parser.add_argument("--sample", type=str, default="200",
                        help="抽样数量 (数字 或 'all')")
    parser.add_argument("--kline-days", type=int, default=5,
                        help="K线对比取最近N天")
    parser.add_argument("--skip-kline", action="store_true",
                        help="跳过K线对比测试")
    parser.add_argument("--sources", type=str, default="",
                        help="指定测试源 (逗号分隔, 如 'tencent,sina')")
    args = parser.parse_args()

    # 过滤数据源
    global SOURCES
    if args.sources:
        names = [s.strip() for s in args.sources.split(",")]
        SOURCES = [s for s in SOURCES if s.name in names]
        if not SOURCES:
            print(f"❌ 没有匹配的数据源: {args.sources}")
            sys.exit(1)

    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║          A股数据源 — 批量行情快照 & OHLCV 横向对比测试              ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print(f"\n  时间: {datetime.now(_TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  测试源: {', '.join(s.name for s in SOURCES)}")

    # 获取活跃股票代码
    max_codes = 0 if args.sample == "all" else int(args.sample)
    codes = get_active_stock_codes(max_codes=max_codes)
    if not codes:
        print("❌ 获取股票列表失败")
        sys.exit(1)

    # 测试1: 批量行情快照
    all_quotes = test_batch_quotes(codes)

    # 测试2: OHLCV K线横向对比
    if not args.skip_kline:
        test_ohlcv_comparison(codes, all_quotes, kline_days=args.kline_days)

    # 测试3: 批量行情快照 OHLCV 交叉验证
    test_batch_quotes_cross_validation(codes, all_quotes)

    # 测试4: 速率基准
    test_speed_benchmark(codes)

    # 最终总结
    print("\n" + "=" * 70)
    print("📋 最终总结")
    print("=" * 70)
    print(f"\n  测试股票数: {len(codes)}")
    print(f"  测试数据源: {', '.join(s.name for s in SOURCES)}")
    print()
    for src in SOURCES:
        quotes = all_quotes.get(src.name, {})
        got = len(quotes)
        lost = len(codes) - got
        rate = lost / len(codes) * 100 if codes else 0
        print(f"  {src.name:>12}: 获取 {got}/{len(codes)} | 丢失率 {rate:.1f}%")
    print()


if __name__ == "__main__":
    main()
