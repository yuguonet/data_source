#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================================
# source_sync.py — Coordinator.market_kline 数据源 + 完整性校验 + 写库
# ============================================================================
#
# 核心流程:
#   1. 从 basicinfo_db 获取全市场股票列表
#   2. 每批 500 只交给 Coordinator.coordinate_market_kline()
#   3. 逐只做完整性校验:
#      - 交易日历对比（缺失日检测）
#      - 停复牌检测（vol=0 且 OHLC 相同）
#      - volume > 0（非停牌 bar）
#      - 15m: 每天 16 bar 检查
#   4. 无错误 → 先删旧数据再写入
#      有错误 → 写 log + 记录进重传文件
#   5. 循环直到全部完成
#   6. 最后重试重传文件一次，正确的从中删除
#
# 用法:
# python optimizer/source_sync.py -T 1D                    # 1D: 2021-01 起
# python optimizer/source_sync.py -T 15m                   # 15m: 2024-01-01 起
# python optimizer/source_sync.py -T 1D --end-date 2026-05-17  # 指定截止日期
# python optimizer/source_sync.py -T 1D --resume           # 断点续传
# python optimizer/source_sync.py -T 1D --retry-only       # 只重试错误股票
# python optimizer/source_sync.py -T 1D --dry-run          # 只校验不写库
#
# ============================================================================

from __future__ import annotations

import os
import sys
import csv
import json
import math
import time
import signal
import logging
import argparse
import threading
from bisect import bisect_left
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 路径 & 环境
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if not os.path.isdir(os.path.join(PROJECT_ROOT, "backend_api_python")):
    _cwd = os.getcwd()
    if os.path.isdir(os.path.join(_cwd, "backend_api_python")):
        PROJECT_ROOT = _cwd
sys.path.insert(0, os.path.join(PROJECT_ROOT, "backend_api_python"))

_OPTIMIZER_DIR = os.path.dirname(os.path.abspath(__file__))
if _OPTIMIZER_DIR not in sys.path:
    sys.path.insert(0, _OPTIMIZER_DIR)


def _load_env():
    try:
        from dotenv import load_dotenv
        for p in [
            os.path.join(PROJECT_ROOT, "backend_api_python", ".env"),
            os.path.join(PROJECT_ROOT, ".env"),
        ]:
            if os.path.isfile(p):
                load_dotenv(p, override=False)
                break
    except Exception:
        pass


_load_env()

# ---------------------------------------------------------------------------
# 全局 socket 超时 — 防止网络阻塞导致 Ctrl+C 失效
# Python 信号只在字节码指令间检查；阻塞在 C 层 socket 时 SIGINT 会被挂起。
# 设置默认超时后，socket 操作会在超时后抛 TimeoutError，回到 Python 层处理信号。
# ---------------------------------------------------------------------------
import socket as _socket
_socket.setdefaulttimeout(120)  # 120s 兜底超时

# ---------------------------------------------------------------------------
# 时间常量
# ---------------------------------------------------------------------------

TZ_SH = timezone(timedelta(hours=8))

# 15m 标准 bar 时间（16 根，不含 9:30 开盘集合竞价）
_BAR_TIMES_15M = [
    (9, 45), (10, 0), (10, 15), (10, 30), (10, 45),
    (11, 0), (11, 15), (11, 30),
    (13, 15), (13, 30), (13, 45), (14, 0), (14, 15),
    (14, 30), (14, 45), (15, 0),
]
_BAR_SET_15M: Set[Tuple[int, int]] = set(_BAR_TIMES_15M)

# 交易日历
_TRADING_DAYS_SORTED: List[str] = []
_TRADING_DAY_SET: Set[str] = set()
_PREV_TRADING_DAY_MAP: Dict[str, Optional[str]] = {}   # 预计算: d -> 前一交易日


def _init_trading_calendar(silent: bool = False):
    global _TRADING_DAYS_SORTED, _TRADING_DAY_SET, _PREV_TRADING_DAY_MAP
    if _TRADING_DAY_SET:
        return
    from app.utils.trading_calendar import _load
    _TRADING_DAY_SET = _load()
    _TRADING_DAYS_SORTED = sorted(_TRADING_DAY_SET)
    # 预计算每个交易日的前一交易日
    _PREV_TRADING_DAY_MAP = {}
    for i, d in enumerate(_TRADING_DAYS_SORTED):
        _PREV_TRADING_DAY_MAP[d] = _TRADING_DAYS_SORTED[i - 1] if i > 0 else None
    if not silent:
        print(f"📅 交易日历: {len(_TRADING_DAY_SET)} 天")


def _is_trading_day(d: str) -> bool:
    if not _TRADING_DAY_SET:
        _init_trading_calendar(silent=True)
    return d in _TRADING_DAY_SET


def _trading_days_between(d1: str, d2: str) -> int:
    if d1 >= d2:
        return 0
    if not _TRADING_DAY_SET:
        _init_trading_calendar(silent=True)
    d1_next = (datetime.strptime(d1, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    left = bisect_left(_TRADING_DAYS_SORTED, d1_next)
    right = bisect_left(_TRADING_DAYS_SORTED, d2)
    return max(0, right - left)


def _next_day(d: str) -> str:
    return (datetime.strptime(d, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")


def _prev_day(d: str) -> str:
    return (datetime.strptime(d, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")


def _safe_float(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# 板块判断
# ---------------------------------------------------------------------------

def _detect_board(code: str) -> str:
    """根据代码判断板块: main_sh/main_sz/gem/star/bj/unknown"""
    c = code[:3]
    if c in ("600", "601", "603", "605"):
        return "main_sh"
    if c in ("000", "001", "002", "003"):
        return "main_sz"
    if c in ("300", "301"):
        return "gem"
    if c in ("688", "689"):
        return "star"
    if code[:2] in ("43", "82", "83", "87", "88"):
        return "bj"
    return "unknown"


# ---------------------------------------------------------------------------
# 数据转换: Coordinator bar → 标准记录
# ---------------------------------------------------------------------------

def _parse_bar_time(bar: Dict[str, Any]) -> Optional[datetime]:
    ts = bar.get("time")
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=TZ_SH)
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=TZ_SH)
    if isinstance(ts, str) and ts.strip():
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
            try:
                return datetime.strptime(ts.strip(), fmt).replace(tzinfo=TZ_SH)
            except ValueError:
                continue
    dt_str = bar.get("date") or bar.get("datetime") or ""
    if dt_str:
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
            try:
                return datetime.strptime(str(dt_str).strip(), fmt).replace(tzinfo=TZ_SH)
            except ValueError:
                continue
    return None


def _bars_to_records(bars: List[Dict[str, Any]], timeframe: str) -> List[Dict[str, Any]]:
    """将 Coordinator bars 转为标准记录，过滤无效 bar，去重"""
    seen: Dict[datetime, Dict[str, Any]] = {}
    for bar in bars:
        dt = _parse_bar_time(bar)
        if dt is None:
            continue
        if timeframe == "15m":
            total_min = dt.hour * 60 + dt.minute
            if total_min == 570:  # 9:30 丢弃（集合竞价）
                continue
        o = _safe_float(bar.get("open"))
        h = _safe_float(bar.get("high"))
        l = _safe_float(bar.get("low"))
        c = _safe_float(bar.get("close"))
        v = _safe_float(bar.get("volume"))
        # ── 自动修正 OHLC 越界（方案 A）──
        # 数据源偶尔返回 open/close 超出 high/low 范围，
        # 以 open/close 为准扩展 high/low（实际成交价一定在 high-low 范围内）
        prices = [p for p in (o, h, l, c) if p > 0]
        if prices:
            if h > 0:
                h = max(h, *prices)
            if l > 0:
                l = min(l, *prices)
        # 去重: 同时间戳选质量更好的记录
        if dt in seen:
            prev = seen[dt]
            prev_v = _safe_float(prev.get("volume"))
            # 已有记录 volume>0 而新的 volume=0 → 保留旧的
            if prev_v > 0 and v == 0:
                continue
            # 两边 volume 都 > 0 → 选 OHLC 更完整的（非零字段更多）
            if prev_v > 0 and v > 0:
                prev_nonzero = sum(1 for k in ("open", "high", "low", "close") if _safe_float(prev.get(k)) > 0)
                new_nonzero = sum(1 for val in (o, h, l, c) if val > 0)
                if new_nonzero < prev_nonzero:
                    continue  # 新的不如旧的完整，保留旧的
        seen[dt] = {"time": dt, "open": o, "high": h, "low": l, "close": c, "volume": v}
    return sorted(seen.values(), key=lambda r: r["time"])


# ═══════════════════════════════════════════════════════
# DB 写入（删旧 + 写新，同一事务）
# ═══════════════════════════════════════════════════════

class ValidationResult:
    """单只股票的校验结果"""
    __slots__ = ("code", "errors", "warnings", "bar_count", "suspension_dates")

    def __init__(self, code: str):
        self.code = code
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.bar_count: int = 0
        self.suspension_dates: Set[str] = set()

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def add_error(self, msg: str):
        self.errors.append(msg)

    def add_warning(self, msg: str):
        self.warnings.append(msg)


def validate_stock(
    code: str,
    records: List[Dict[str, Any]],
    timeframe: str,
    start_date: str,
    end_date: str,
    price_tolerance: float = 0.02,   # 保留参数兼容性，已不再使用
) -> ValidationResult:
    """对单只股票做完整性校验"""
    result = ValidationResult(code)
    result.bar_count = len(records)

    if not records:
        result.add_error("无数据")
        return result

    board = _detect_board(code)

    # ── 按日聚合: 用于停牌检测 ──
    # daily_agg: {date: {open, high, low, close, volume, bar_count}}
    daily_agg: Dict[str, Dict[str, Any]] = {}
    date_records: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for rec in records:
        dt = rec.get("time")
        if not isinstance(dt, datetime):
            continue
        d = dt.strftime("%Y-%m-%d")
        date_records[d].append(rec)
        o = _safe_float(rec.get("open"))
        h = _safe_float(rec.get("high"))
        l = _safe_float(rec.get("low"))
        c = _safe_float(rec.get("close"))
        v = _safe_float(rec.get("volume"))

        if d not in daily_agg:
            daily_agg[d] = {"open": o, "high": h, "low": l, "close": c,
                            "volume": v, "bar_count": 1}
        else:
            agg = daily_agg[d]
            # 日 open = 第一根 bar 的 open
            # 日 high/low = 所有 bar 的 max/min
            # 日 close = 最后一根 bar 的 close
            agg["high"] = max(agg["high"], h)
            if l > 0:
                agg["low"] = min(agg["low"], l) if agg["low"] > 0 else l
            agg["close"] = c  # 后出现的覆盖，最终是最后一根
            agg["volume"] += v
            agg["bar_count"] += 1

    sorted_dates = sorted(date_records.keys())
    if not sorted_dates:
        result.add_error("无有效日期")
        return result

    actual_start = sorted_dates[0]
    actual_end = sorted_dates[-1]

    # ── 停牌检测: 基于日级聚合 ──
    # 停牌日: 当天所有 bar 的 volume=0 且 OHLC 全相同且 > 0
    suspension_dates: Set[str] = set()
    for d, agg in daily_agg.items():
        if agg["volume"] == 0 and agg["open"] > 0 and \
           agg["open"] == agg["high"] == agg["low"] == agg["close"]:
            suspension_dates.add(d)
    result.suspension_dates = suspension_dates

    # ── 0. 数据量检查: 实际有效日 < 请求范围交易日的 80% 视为坏数据 ──
    range_start = max(actual_start, start_date)
    range_end = min(actual_end, end_date)
    expected_trading_days = [d for d in _TRADING_DAYS_SORTED if range_start <= d <= range_end]
    expected_count = len(expected_trading_days)
    # 有效日 = 数据中有记录且非停牌的日期
    effective_dates = {d for d in sorted_dates if d not in suspension_dates and range_start <= d <= range_end}
    actual_count = len(effective_dates)
    if expected_count > 0:
        coverage = actual_count / expected_count
        if coverage < 0.80:
            result.add_error(
                f"数据覆盖率不足: {actual_count}/{expected_count} "
                f"({coverage*100:.1f}%) < 80%"
            )
            return result  # 覆盖率太低，后续检查无意义

    # ── 1. 交易日历对比: 检测缺失的交易日（仅 1D，允许 2% 缺失） ──
    if timeframe == "1D":
        missing_days = [d for d in expected_trading_days
                        if d not in date_records and d not in suspension_dates]
        if missing_days:
            missing_pct = len(missing_days) / expected_count if expected_count > 0 else 0
            if missing_pct > 0.02:
                result.add_error(
                    f"交易日缺失过多: {len(missing_days)}/{expected_count} "
                    f"({missing_pct*100:.1f}%) > 2%"
                )
            else:
                for d in missing_days:
                    result.add_warning(f"交易日缺失: {d}")

    # ── 2. 每日数据校验 ──

    # 15m: 最后一天不检查（数据可能不完整）
    is_15m_bar_check = (timeframe == "15m")
    last_date = sorted_dates[-1] if is_15m_bar_check else None

    for d in sorted_dates:
        # 15m: 跳过最后一天
        if d == last_date:
            continue
        day_records = date_records[d]
        is_suspend = d in suspension_dates

        # ── 15m: 检查每天 bar 数，16 根则重新分配标准时间 ──
        skip_bar_check = False
        if timeframe == "15m" and not is_suspend and _is_trading_day(d):
            if len(day_records) == 16:
                # 按时间排序，重新分配 16 根标准 bar 时间
                day_records.sort(key=lambda r: r["time"])
                for rec, (h, m) in zip(day_records, _BAR_TIMES_15M):
                    old_dt = rec["time"]
                    rec["time"] = old_dt.replace(hour=h, minute=m, second=0, microsecond=0)
            else:
                # bar 数不是 16 根，跳过逐 bar 校验
                result.add_warning(f"15m bar数异常: {d} count={len(day_records)} (期望16)")
                skip_bar_check = True

        # ── 停牌日跳过逐 bar 校验 ──
        if is_suspend:
            skip_bar_check = True

        if skip_bar_check:
            continue

        # ── 逐 bar 校验（OHLC 合理性） ──
        for rec in day_records:
            o = _safe_float(rec.get("open"))
            h = _safe_float(rec.get("high"))
            l = _safe_float(rec.get("low"))
            c = _safe_float(rec.get("close"))
            v = _safe_float(rec.get("volume"))

            # vol > 0（非停牌 bar 必须有成交量）
            if v <= 0:
                result.add_error(f"volume<=0: {d} vol={v}")

            # OHLC 全零
            if o == 0 and h == 0 and l == 0 and c == 0:
                result.add_error(f"OHLC 全零: {d}")
                continue

            # OHLC 合理性
            if h > 0 and l > 0 and h < l:
                result.add_error(f"high<low: {d} H={h} L={l}")
                continue
            if o > 0 and h > 0 and (o > h or o < l):
                result.add_warning(f"open 越界(已写入): {d} O={o} H={h} L={l}")
            if c > 0 and h > 0 and (c > h or c < l):
                result.add_warning(f"close 越界(已写入): {d} C={c} H={h} L={l}")

    # ── 3. 尾部检查 ──
    if actual_end < end_date and _is_trading_day(end_date):
        trailing = _trading_days_between(actual_end, end_date)
        if trailing > 0:
            result.add_warning(f"尾部缺失 {trailing} 天: {actual_end} → {end_date}")

    return result


def _prev_trading_day(d: str) -> Optional[str]:
    """获取 d 之前的最近一个交易日（不含 d 本身）"""
    if not _TRADING_DAY_SET:
        _init_trading_calendar(silent=True)
    return _PREV_TRADING_DAY_MAP.get(d)


# ═══════════════════════════════════════════════════════
# DB 写入（删旧 + 写新）
# ═══════════════════════════════════════════════════════

def write_stock_data(
    writer,
    pool,
    market: str,
    code: str,
    timeframe: str,
    records: List[Dict[str, Any]],
    start_date: str,
    end_date: str,
    dry_run: bool = False,
) -> int:
    """先删旧数据，再写入新数据（同一事务）"""
    if dry_run:
        return 0  # dry-run 不应计入 written
    if not records:
        return 0

    # 转为 DB 格式
    db_records = []
    for rec in records:
        ts = rec.get("time")
        if isinstance(ts, datetime):
            dt = ts
        elif isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts, tz=TZ_SH)
        else:
            continue
        if dt.tzinfo:
            dt = dt.replace(tzinfo=None)
        db_records.append({
            "symbol": code,
            "timeframe": timeframe,
            "time": dt,
            "open": _safe_float(rec.get("open")),
            "high": _safe_float(rec.get("high")),
            "low": _safe_float(rec.get("low")),
            "close": _safe_float(rec.get("close")),
            "volume": _safe_float(rec.get("volume")),
        })

    if not db_records:
        return 0

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    years = list(range(start_year, end_year + 1))

    # 同一事务: 先删旧数据，再写新数据（保证原子性）
    _VALID_TABLES = {f"kline_{tf}_{y}" for tf in ("1D", "15m") for y in range(2000, 2035)}

    try:
        with pool.connection() as conn:
            cur = conn.cursor()
            # 删除旧数据
            for year in years:
                table = f"kline_{timeframe}_{year}"
                if table not in _VALID_TABLES:
                    logger.warning("跳过非法表名: %s", table)
                    continue
                try:
                    cur.execute(f"""
                        DELETE FROM "{table}"
                        WHERE symbol = %s
                          AND time >= %s
                          AND time <= %s
                    """, (code, f"{start_date} 00:00:00", f"{end_date} 23:59:59"))
                except Exception as del_err:
                    # 区分"表不存在"和其他错误
                    if "does not exist" in str(del_err).lower() or "undefinedtable" in str(del_err).lower():
                        pass  # 表不存在，正常跳过
                    else:
                        logger.error("删除数据失败 %s/%s/%s: %s", code, table, timeframe, del_err)

            # 在同一事务内写入新数据（逐批 INSERT）
            inserted = 0
            for i in range(0, len(db_records), 5000):
                batch = db_records[i:i + 5000]
                for year in years:
                    table = f"kline_{timeframe}_{year}"
                    if table not in _VALID_TABLES:
                        continue
                    year_batch = [r for r in batch
                                  if r["time"].year == year]
                    if not year_batch:
                        continue
                    try:
                        cur.executemany(
                            f'INSERT INTO "{table}" '
                            f'(symbol, time, open, high, low, close, volume) '
                            f'VALUES (%(symbol)s, %(time)s, '
                            f'%(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)',
                            year_batch,
                        )
                        inserted += len(year_batch)
                    except Exception as ins_err:
                        if "does not exist" in str(ins_err).lower() or "undefinedtable" in str(ins_err).lower():
                            pass  # 表不存在，正常跳过
                        else:
                            logger.error("写入数据失败 %s/%s/%s: %s", code, table, timeframe, ins_err)

            conn.commit()
            cur.close()
            return inserted
    except Exception as e:
        logger.error("写库事务失败 %s/%s: %s", code, timeframe, e)
        return 0


def write_batch_data(
    pool,
    market: str,
    timeframe: str,
    stock_records: Dict[str, List[Dict[str, Any]]],
    start_date: str,
    end_date: str,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    批量写入: 整批股票一个事务，批量 DELETE + 批量 INSERT。
    stock_records: {code: [records, ...]}
    返回: {code: inserted_count}
    """
    if dry_run or not stock_records:
        return {}

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    years = list(range(start_year, end_year + 1))
    _VALID_TABLES = {f"kline_{tf}_{y}" for tf in ("1D", "15m") for y in range(2000, 2035)}

    # ── 预构建全部 db_records，按 (year, code) 分组 ──
    # records_by_year_table[year] = [db_record, ...]
    records_by_year: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    code_row_counts: Dict[str, int] = {}

    for code, records in stock_records.items():
        count = 0
        for rec in records:
            ts = rec.get("time")
            if isinstance(ts, datetime):
                dt = ts
            elif isinstance(ts, (int, float)):
                dt = datetime.fromtimestamp(ts, tz=TZ_SH)
            else:
                continue
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            records_by_year[dt.year].append({
                "symbol": code,
                "timeframe": timeframe,
                "time": dt,
                "open": _safe_float(rec.get("open")),
                "high": _safe_float(rec.get("high")),
                "low": _safe_float(rec.get("low")),
                "close": _safe_float(rec.get("close")),
                "volume": _safe_float(rec.get("volume")),
            })
            count += 1
        code_row_counts[code] = count

    all_codes = list(stock_records.keys())

    try:
        with pool.connection() as conn:
            cur = conn.cursor()

            # ── 批量 DELETE: 用 IN (...) 一次删一批 ──
            for year in years:
                table = f"kline_{timeframe}_{year}"
                if table not in _VALID_TABLES:
                    continue
                try:
                    # 分批 DELETE，每批 500 个 symbol（避免 SQL 过长）
                    for i in range(0, len(all_codes), 500):
                        chunk = all_codes[i:i + 500]
                        placeholders = ",".join(["%s"] * len(chunk))
                        cur.execute(f"""
                            DELETE FROM "{table}"
                            WHERE symbol IN ({placeholders})
                              AND time >= %s AND time <= %s
                        """, (*chunk, f"{start_date} 00:00:00", f"{end_date} 23:59:59"))
                except Exception as del_err:
                    if "does not exist" not in str(del_err).lower() and "undefinedtable" not in str(del_err).lower():
                        logger.error("批量删除失败 %s: %s", table, del_err)

            # ── 批量 INSERT ──
            inserted_by_code: Dict[str, int] = defaultdict(int)
            for year in years:
                table = f"kline_{timeframe}_{year}"
                if table not in _VALID_TABLES:
                    continue
                year_rows = records_by_year.get(year, [])
                if not year_rows:
                    continue
                # 按 symbol 分组统计 inserted
                for i in range(0, len(year_rows), 5000):
                    batch = year_rows[i:i + 5000]
                    try:
                        cur.executemany(
                            f'INSERT INTO "{table}" '
                            f'(symbol, time, open, high, low, close, volume) '
                            f'VALUES (%(symbol)s, %(time)s, '
                            f'%(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)',
                            batch,
                        )
                    except Exception as ins_err:
                        if "does not exist" not in str(ins_err).lower() and "undefinedtable" not in str(ins_err).lower():
                            logger.error("批量写入失败 %s: %s", table, ins_err)

            conn.commit()
            cur.close()

            # 返回每只股票的写入行数
            return {code: code_row_counts.get(code, 0) for code in all_codes}

    except Exception as e:
        logger.error("批量写库事务失败: %s", e)
        return {}


# ═══════════════════════════════════════════════════════
# 重传文件管理
# ═══════════════════════════════════════════════════════

def _retry_path(timeframe: str) -> str:
    return os.path.join(PROJECT_ROOT, "optimizer", f".retry_{timeframe}.json")


def _load_retry_codes(path: str) -> Dict[str, Dict[str, Any]]:
    """加载重传文件: {code: {errors: [...], retries: n}}"""
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_retry_codes(path: str, data: Dict[str, Dict[str, Any]]):
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning("保存重传文件失败: %s", e)


def _batch_update_retry(path: str, add: Dict[str, List[str]], remove: List[str]):
    """批量更新重传文件: add={code: errors}, remove=[code, ...]"""
    data = _load_retry_codes(path)
    for code, errors in add.items():
        data[code] = {"errors": errors, "retries": data.get(code, {}).get("retries", 0)}
    for code in remove:
        data.pop(code, None)
    _save_retry_codes(path, data)


# ═══════════════════════════════════════════════════════
# 检查点
# ═══════════════════════════════════════════════════════

def _checkpoint_path(timeframe: str) -> str:
    return os.path.join(PROJECT_ROOT, "optimizer", f".checkpoint_source_{timeframe}.json")


def _load_checkpoint(path: str) -> Dict[str, Any]:
    if not os.path.isfile(path):
        return {"processed_codes": [], "stats": {}}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {"processed_codes": [], "stats": {}}


def _save_checkpoint(path: str, processed: list, stats: dict):
    data = {"processed_codes": processed, "stats": stats,
            "saved_at": datetime.now(TZ_SH).isoformat()}
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _remove_checkpoint(path: str):
    try:
        if os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════
# 中断信号
# ═══════════════════════════════════════════════════════

_INTERRUPTED = False


def _signal_handler(signum, frame):
    global _INTERRUPTED
    if _INTERRUPTED:
        # 二次中断：直接强杀，不等任何线程
        os._exit(130)
    _INTERRUPTED = True
    print("\n⚠️  收到中断信号，正在保存进度...")


def _start_interrupt_watchdog():
    """
    看门狗线程：独立于主线程，每秒检查 _INTERRUPTED 标志。
    首次中断给主线程 10 秒保存进度；二次中断立即强杀。
    """
    def _watch():
        while True:
            time.sleep(1)
            if _INTERRUPTED:
                # 给主线程 10 秒处理中断（保存检查点等）
                for _ in range(10):
                    time.sleep(1)
                    if not _INTERRUPTED:
                        break  # 主线程处理完毕，重置了标志
                else:
                    # 10 秒后仍未恢复，强制退出
                    if _INTERRUPTED:
                        print("\n⚡ 看门狗：主线程 10 秒未响应，强制退出")
                        os._exit(130)
    t = threading.Thread(target=_watch, daemon=True)
    t.start()
    return t


# ═══════════════════════════════════════════════════════
# CSV 报告
# ═══════════════════════════════════════════════════════

def export_csv(results: List[Dict[str, Any]], path: str):
    if not results:
        return
    fields = ["code", "board", "bars", "written", "status", "errors"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k, "") for k in fields})
    print(f"✅ CSV 报告: {path}（{len(results)} 条）")


# ═══════════════════════════════════════════════════════
# 核心处理: 单批
# ═══════════════════════════════════════════════════════

def process_batch(
    symbols: List[str],
    coordinator,
    cb,
    writer,
    pool,
    market: str,
    timeframe: str,
    start_date: str,
    end_date: str,
    count: int,
    timeout: float,
    preferred_source: str,
    adj: str,
    price_tolerance: float,
    dry_run: bool,
    retry_path: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    处理一批股票: 拉取 → 校验 → 写入/记录错误

    Returns:
        (results_list, stats_dict)
    """
    stats = {"total": len(symbols), "fetched": 0, "passed": 0, "failed": 0, "written": 0, "no_data": 0}
    results = []
    to_retry: Dict[str, List[str]] = {}   # code → errors（待加入重传）
    to_remove: List[str] = []             # 成功的 code（待从重传移除）

    # 拉取数据 — 在守护线程中执行，主线程保持响应信号
    raw_data: Dict[str, list] = {}
    fetch_error: Optional[Exception] = None

    def _do_fetch():
        nonlocal raw_data, fetch_error
        try:
            raw_data = coordinator.coordinate_market_kline(
                market=market,
                timeframe=timeframe,
                count=count,
                adj=adj,
                timeout=timeout,
                preferred_source=preferred_source,
                start_date=start_date,
                end_date=end_date,
                symbols=symbols,
            )
        except Exception as e:
            fetch_error = e

    t = threading.Thread(target=_do_fetch, daemon=True)
    t.start()
    # 主线程循环等待，每 1s 检查一次中断信号
    while t.is_alive():
        t.join(timeout=1.0)
        if _INTERRUPTED:
            # 主线程收到中断，守护线程是 daemon，进程退出时自动清理
            raise KeyboardInterrupt("数据拉取被用户中断")

    if fetch_error is not None:
        logger.error("Coordinator 调用失败: %s", fetch_error)
        for code in symbols:
            to_retry[code] = [f"Coordinator 异常: {fetch_error}"]
            results.append({"code": code, "board": _detect_board(code),
                           "bars": 0, "written": 0, "status": "error",
                           "errors": f"Coordinator 异常: {fetch_error}"})
            stats["failed"] += 1
        _batch_update_retry(retry_path, to_retry, [])
        return results, stats

    # ── 构建查找映射: raw_data 的 key 可能带 SH./SZ. 前缀 ──
    # coordinator 会 normalize 代码（加前缀），但 symbols 是不带前缀的
    # 需要建立 code → raw_data_key 的映射
    #
    # 新接口 coordinate_market_kline 返回 List[Dict]（扁平 bar 列表，每条含 symbol 字段），
    # 需先按 symbol 聚合为 Dict[str, List[Dict]] 供后续逐只校验。
    from app.data_sources.normalizer import strip_market_prefix
    raw_by_symbol: Dict[str, list] = {}
    for bar in raw_data:
        sym = bar.get("symbol", "")
        if not sym:
            continue
        pure = strip_market_prefix(sym)
        raw_by_symbol.setdefault(pure, []).append(bar)

    _prefix_map: Dict[str, str] = {}
    for rk in raw_by_symbol:
        if "." in rk:
            pure = rk.split(".", 1)[1]
            _prefix_map[pure] = rk
        else:
            _prefix_map[rk] = rk

    # ── 第一轮: 逐只校验 ──
    code_records: Dict[str, Tuple[List[Dict[str, Any]], ValidationResult]] = {}
    for code in symbols:
        if _INTERRUPTED:
            break

        # symbols 格式 "sh600519"，coordinator 返回纯数字 "600519"
        # 需要去掉前缀再查 _prefix_map
        code_digits = strip_market_prefix(code)
        raw_key = _prefix_map.get(code_digits, code_digits)
        bars = raw_by_symbol.get(raw_key, [])
        if not bars:
            stats["no_data"] += 1
            to_retry[code] = ["无数据"]
            results.append({"code": code, "board": _detect_board(code),
                           "bars": 0, "written": 0, "status": "no_data",
                           "errors": "无数据"})
            continue

        stats["fetched"] += 1

        # 转为标准记录（已去重）
        records = _bars_to_records(bars, timeframe)
        # 丢弃早于 start_date 的数据（防护数据源返回超范围数据）
        if records and start_date:
            start_cutoff = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=TZ_SH)
            records = [r for r in records
                       if isinstance(r.get("time"), datetime) and r["time"] >= start_cutoff]
        # 1D: 盘中时今天的K线未完成，丢弃今天的未完成数据
        if records and timeframe == "1D":
            today_str = datetime.now(TZ_SH).strftime("%Y-%m-%d")
            today_dt = datetime.strptime(today_str, "%Y-%m-%d").replace(tzinfo=TZ_SH)
            last_rec_time = records[-1].get("time")
            if isinstance(last_rec_time, datetime) and last_rec_time >= today_dt:
                # 检查今天是否还在盘中（15:00前视为盘中）
                now_hm = datetime.now(TZ_SH).hour * 60 + datetime.now(TZ_SH).minute
                if now_hm < 15 * 60:  # 15:00 前 → 盘中，丢弃今天的bar
                    records = [r for r in records
                               if not (isinstance(r.get("time"), datetime)
                                       and r["time"].strftime("%Y-%m-%d") == today_str)]
        if not records:
            stats["no_data"] += 1
            to_retry[code] = ["转换后无有效记录"]
            results.append({"code": code, "board": _detect_board(code),
                           "bars": len(bars), "written": 0, "status": "no_data",
                           "errors": "转换后无有效记录"})
            continue

        # 完整性校验
        vr = validate_stock(code, records, timeframe, start_date, end_date, price_tolerance)

        if vr.has_errors:
            stats["failed"] += 1
            to_retry[code] = vr.errors
            err_summary = "; ".join(vr.errors[:5])
            if len(vr.errors) > 5:
                err_summary += f" (+{len(vr.errors)-5})"
            logger.warning("[校验失败] %s (%s): %s", code, _detect_board(code), err_summary)
            results.append({"code": code, "board": _detect_board(code),
                           "bars": len(records), "written": 0, "status": "error",
                           "errors": err_summary})
        else:
            stats["passed"] += 1
            code_records[code] = (records, vr)

    # ── 批量写库: 一个事务搞定整批 ──
    if code_records and not dry_run:
        stock_data = {c: recs for c, (recs, _vr) in code_records.items()}
        written_map = write_batch_data(pool, market, timeframe,
                                       stock_data, start_date, end_date, dry_run)
        for code, (records, vr) in code_records.items():
            n = written_map.get(code, 0)
            stats["written"] += n
            to_remove.append(code)
            warn_summary = "; ".join(vr.warnings[:3]) if vr.warnings else ""
            results.append({"code": code, "board": _detect_board(code),
                           "bars": len(records), "written": n, "status": "ok",
                           "errors": warn_summary})
    elif code_records and dry_run:
        for code, (records, vr) in code_records.items():
            to_remove.append(code)
            warn_summary = "; ".join(vr.warnings[:3]) if vr.warnings else ""
            results.append({"code": code, "board": _detect_board(code),
                           "bars": len(records), "written": 0, "status": "ok",
                           "errors": warn_summary})

    # 批量更新重传文件（一次 IO）
    _batch_update_retry(retry_path, to_retry, to_remove)

    return results, stats


# ═══════════════════════════════════════════════════════
# 主程序
# ═══════════════════════════════════════════════════════

def main():
    global _INTERRUPTED

    parser = argparse.ArgumentParser(
        description="Coordinator.market_kline 数据源 + 完整性校验 + 写库",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("-T", "--type",
        choices=["1D", "15m"], default="1D",
        help="数据类型: 1D(日线) / 15m(15分钟线)")
    parser.add_argument("--market", default="CNStock", help="市场（默认 CNStock）")
    parser.add_argument("--batch-size", type=int, default=100,
        help="每批处理股票数（默认 100）")
    parser.add_argument("--count", type=int, default=0,
        help="每只股票拉取条数（0=自动计算）")
    parser.add_argument("--timeout", type=float, default=120,
        help="Coordinator 全局超时秒数（默认 120）")
    parser.add_argument("--preferred-source", default="",
        help="指定首选数据源")
    parser.add_argument("--adj", default="", choices=["qfq", "hfq", ""],
        help="复权方式 (默认不复权)")
    parser.add_argument("--start-date", default="",
        help="数据起始日期 (YYYY-MM-DD)，默认为当天")
    parser.add_argument("--end-date", default="",
        help="数据截止日期 (YYYY-MM-DD)，默认为当天")
    parser.add_argument("--price-tolerance", type=float, default=0.02,
        help="(已废弃) 涨跌幅检查容差，涨跌幅校验已移除")
    parser.add_argument("--dry-run", action="store_true",
        help="只拉取校验，不写库")
    parser.add_argument("--resume", action="store_true",
        help="断点续传：跳过已处理的股票")
    parser.add_argument("--retry-only", action="store_true",
        help="只重试重传文件中的股票")

    args = parser.parse_args()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # 启动看门狗：独立线程，检测到中断后 2s 强制退出
    _start_interrupt_watchdog()

    if sys.platform != 'win32':
        _wakeup_r, _wakeup_w = os.pipe()
        os.close(_wakeup_r)  # 只需要写端用于 set_wakeup_fd
        os.set_blocking(_wakeup_w, False)
        signal.set_wakeup_fd(_wakeup_w)

    # 日期范围
    now_date = datetime.now(TZ_SH).strftime('%Y-%m-%d')

    if args.start_date:
        start_date = args.start_date
    else:
        start_date = now_date
    if args.end_date:
        end_date = args.end_date
    else:
        end_date = now_date

    from app.utils.db_market import get_market_kline_writer, get_market_db_manager
    from app.data_sources.coordinator import get_coordinator, CircuitBreaker

    writer = get_market_kline_writer()
    mgr = get_market_db_manager()
    coordinator = get_coordinator()
    cb = CircuitBreaker(failure_threshold=3, cooldown_seconds=120.0, name="source_sync")
    market = args.market

    if not args.dry_run:
        if not mgr.market_db_exists(market):
            mgr.ensure_market_db(market)

    pool = mgr._get_pool(market)

    _init_trading_calendar()

    # 自动计算 count
    count = args.count
    if count <= 0:
        from app.data_sources.provider import calc_kline_count
        count = calc_kline_count(args.type, start_date, end_date)

    retry_path = _retry_path(args.type)
    ckpt_path = _checkpoint_path(args.type)

    # ── 获取股票列表 ──
    print("\n[1/4] 获取股票列表...")
    try:
        from app.utils.basicinfo_db import get_stock_basic_db
        db = get_stock_basic_db()
        all_stocks = db.get_all_stocks(status="active")
    except Exception as e:
        logger.error("获取股票列表失败: %s", e)
        return 1

    all_codes = sorted(s["symbol"] for s in all_stocks)
    print(f"  共 {len(all_codes)} 只A股")

    # ── 断点续传 ──
    processed_set: set = set()
    if args.resume and not args.retry_only:
        ckpt = _load_checkpoint(ckpt_path)
        processed_set = set(ckpt.get("processed_codes", []))
        if processed_set:
            before = len(all_codes)
            all_codes = [c for c in all_codes if c not in processed_set]
            print(f"  📂 断点续传: 已处理 {len(processed_set)} 只，剩余 {len(all_codes)} 只")
            if not all_codes:
                print("  ✅ 所有股票已处理完毕")

    # ── 重试模式 ──
    if args.retry_only:
        retry_data = _load_retry_codes(retry_path)
        all_codes = sorted(retry_data.keys())
        print(f"  🔄 重试模式: {len(all_codes)} 只待重试")

    if not all_codes:
        # 即使主列表为空，也可能需要最终重试
        retry_data = _load_retry_codes(retry_path)
        if retry_data and not args.retry_only:
            all_codes = sorted(retry_data.keys())
            print(f"  🔄 进入最终重试: {len(all_codes)} 只")
        else:
            print("  无需处理")
            _remove_checkpoint(ckpt_path)
            mgr.close_all_pools()
            return 0

    total = len(all_codes)
    batch_size = min(args.batch_size, total)

    print(f"""
╔═══════════════════════════════════════════════════════╗
║  📡 Coordinator.market_kline + 完整性校验 + 写库       ║
╠═══════════════════════════════════════════════════════╣
║  类型: {args.type:<8}  市场: {market:<12}                ║
║  日期: {start_date} → {end_date}                     ║
║  股票: {total} 只  批次: {batch_size}  条数: {count:<8}          ║
║  复权: {args.adj or '不复权':<8}  超时: {args.timeout:.0f}s                   ║
║  模式: {'重试' if args.retry_only else '主循环'}{'  dry-run' if args.dry_run else ''}                         ║
╚═══════════════════════════════════════════════════════╝
""")

    # ── 分批处理（支持中断后交互式续传）──
    print(f"\n[2/4] 拉取 + 校验 + 写入...")

    all_results: List[Dict[str, Any]] = []
    agg_stats = {
        "total": 0, "fetched": 0, "passed": 0, "failed": 0,
        "no_data": 0, "written": 0,
    }

    t0 = time.time()
    batches = [all_codes[i:i + batch_size] for i in range(0, len(all_codes), batch_size)]

    while True:
        for batch_idx, batch_codes in enumerate(batches):
            if _INTERRUPTED:
                break

            batch_start = time.time()
            try:
                results, stats = process_batch(
                    symbols=batch_codes,
                    coordinator=coordinator,
                    cb=cb,
                    writer=writer,
                    pool=pool,
                    market=market,
                    timeframe=args.type,
                    start_date=start_date,
                    end_date=end_date,
                    count=count,
                    timeout=args.timeout,
                    preferred_source=args.preferred_source,
                    adj=args.adj,
                    price_tolerance=args.price_tolerance,
                    dry_run=args.dry_run,
                    retry_path=retry_path,
                )
            except KeyboardInterrupt:
                _INTERRUPTED = True
                break

            all_results.extend(results)
            for k in agg_stats:
                agg_stats[k] += stats.get(k, 0)

            # 更新已处理列表
            for code in batch_codes:
                processed_set.add(code)

            batch_elapsed = time.time() - batch_start
            total_elapsed = time.time() - t0
            done = min((batch_idx + 1) * batch_size, total)

            print(f"\r  [{done}/{total}] "
                  f"拉取={agg_stats['fetched']} 通过={agg_stats['passed']} "
                  f"失败={agg_stats['failed']} 无数据={agg_stats['no_data']} "
                  f"写入={agg_stats['written']:,} "
                  f"耗时={total_elapsed:.0f}s",
                  end='', flush=True)

            # 定期保存检查点
            if (batch_idx + 1) % 5 == 0:
                _save_checkpoint(ckpt_path, list(processed_set), agg_stats)

        print()

        # 始终保存检查点（无论是否中断）
        _save_checkpoint(ckpt_path, list(processed_set), agg_stats)

        # 正常完成或 dry-run → 退出循环
        if not _INTERRUPTED:
            break

        # ── 中断：保存进度，直接退出 ──
        remaining_codes = [c for c in all_codes if c not in processed_set]
        print(f"\n  ⏸️  已中断")
        print(f"  已处理: {len(processed_set)}/{total}")
        print(f"  剩余:   {len(remaining_codes)} 只")
        print(f"  进度已保存，下次用 --resume 继续")
        break

    elapsed_main = time.time() - t0

    # ── 最终重试（已禁用，由用户通过 --retry-only 手动重试）──
    retry_data = _load_retry_codes(retry_path)
    retry_codes = sorted(retry_data.keys())

    if retry_codes and not args.dry_run:
        print(f"\n[3/4] 跳过自动重试: {len(retry_codes)} 只待修复（使用 --retry-only 手动重试）")
    else:
        print(f"\n[3/4] 无需重试")

    elapsed_total = time.time() - t0

    # ── 汇总 ──
    print(f"\n[4/4] 汇总统计")
    print(f"总耗时: {elapsed_total:.1f}s ({elapsed_total/60:.1f}分钟)")
    print(f"  总计:   {agg_stats['total']}")
    print(f"  拉取:   {agg_stats['fetched']}")
    print(f"  校验通过: {agg_stats['passed']}")
    print(f"  校验失败: {agg_stats['failed']}")
    print(f"  无数据:   {agg_stats['no_data']}")
    print(f"  写入行数: {agg_stats['written']:,}")

    # 错误统计
    error_results = [r for r in all_results if r.get("status") == "error"]
    if error_results:
        # 按错误类型聚合
        error_types: Dict[str, int] = defaultdict(int)
        for r in error_results:
            for err in r.get("errors", "").split("; "):
                err_type = err.split(":")[0].strip() if ":" in err else err
                error_types[err_type] += 1
        print(f"\n错误类型分布:")
        for etype, cnt in sorted(error_types.items(), key=lambda x: -x[1])[:10]:
            print(f"  {etype}: {cnt}")

    # 按板块统计
    board_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"ok": 0, "fail": 0})
    for r in all_results:
        board = r.get("board", "unknown")
        if r.get("status") == "ok":
            board_stats[board]["ok"] += 1
        else:
            board_stats[board]["fail"] += 1
    if board_stats:
        print(f"\n板块统计:")
        for board, st in sorted(board_stats.items()):
            print(f"  {board}: 通过={st['ok']} 失败={st['fail']}")

    # CSV 报告
    csv_path = os.path.join(PROJECT_ROOT, "optimizer",
                            f"report_source_{args.type}_{start_date}_{end_date}.csv")
    export_csv(all_results, csv_path)

    # 清理检查点（仅全部完成且无错误时）
    remaining_retry = _load_retry_codes(retry_path)
    if not remaining_retry and not _INTERRUPTED:
        _remove_checkpoint(ckpt_path)
        # 清理重传文件
        try:
            if os.path.isfile(retry_path):
                os.remove(retry_path)
        except Exception:
            pass

    print(f"\n{'='*60}")
    if remaining_retry:
        print(f"  ⚠️  {len(remaining_retry)} 只仍有错误，详见: {retry_path}")
    elif _INTERRUPTED:
        print(f"  ⏸️  已退出，进度已保存。下次用 --resume 继续")
    else:
        print(f"  ✅ 全部完成!")
    print(f"{'='*60}")

    mgr.close_all_pools()
    return 1 if (error_results or _INTERRUPTED) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断，退出。进度已保存，下次用 --resume 继续。")
        sys.exit(1)
