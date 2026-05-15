# -*- coding: utf-8 -*-
"""
协助层 (Coordinator) — 数据源并发调度的核心引擎

=== 在整个链路中的位置 ===

  路由层 (routes/)
      ↓
  服务层 (services/kline.py, portfolio_monitor.py, ...)
      ↓
  数据源门面 (DataSourceFactory)
      ↓
  ★ 协助层 (Coordinator) ← 你在这里
      ↓
  数据源层 (data_sources/cn_stock.py, us_stock.py, ...)
      ↓
  Provider 层 (tencent, sina, eastmoney, akshare, ...)

=== 核心职责 ===

  1. 动态任务队列:   源干完一个 symbol 立刻拿下一个，不闲着（负载均衡）
  2. 并发控制:       每个源的线程数不超过其 max_workers 配置
  3. 熔断联动:       跳过已熔断的源；连续失败过多自动停用该源
  4. 源自动发现:     从 Provider 层按能力/周期/市场自动获取可用源（不硬编码）
  5. 指定源优先:     支持 preferred_source 直接指定数据源，失败后自动回退其他源
  6. Race 模式:      实时行情场景，所有源并发抢答，第一个成功的直接返回

=== 两种调度模式 ===

  模式 A — K线批量获取 (coordinate_kline):
    多只股票 × 多个源 → 动态队列分配 → 每只股票只要有一个源成功就算成功
    场景: 批量加载历史K线、回测数据准备、批量分析

  模式 B — 实时行情 Race (coordinate_ticker):
    1只股票 × 多个源 → 并发抢答 → 第一个返回有效价格的直接用
    场景: 获取实时报价、自选股价格刷新

  模式 D — 全市场批量K线 (coordinate_market_kline):
    Coordinator 分组 → 共享队列 → 多个 Provider 并发取组 → 每个 Provider 调自己的 fetch_market_kline → 合并结果
    场景: 全市场K线加载、全市场行情快照

=== 两种源指定方式 ===

  方式 1 — 自动发现（推荐）:
    不传 sources 参数，传 market="CNStock"
    → Coordinator 调用 Provider 层自动发现可用源
    → 好处: 新增/删除 Provider 无需改调用方

  方式 2 — 手动指定（兼容旧代码）:
    传入 sources=[(name, fetch_fn), ...]
    → 好处: 调用方完全控制用哪些源

=== 函数命名说明（容易混淆的）===

  coordinate_kline  → 实际含义: "并发批量拉K线，动态队列分配多源"
  coordinate_ticker → 实际含义: "实时行情多源Race，谁先返回用谁"
  direct_call       → 实际含义: "直接调用，不加任何协调逻辑"
  _mark_failed      → 实际含义: "标记某源对某symbol失败，放回队列尝试下一个源，或彻底放弃"
  _mark_success     → 实际含义: "标记某symbol获取成功，从队列中移除"
"""

from __future__ import annotations

import atexit
import concurrent.futures
import threading
import time
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from app.data_sources.source_config import (
    SourceConfig, get_source_config, get_sources_for_market, get_all_enabled_sources,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ================================================================
# 熔断器 — 两态状态机: Closed → Open → Closed
# ================================================================

class CircuitBreaker:
    """
    熔断器 — 两态状态机: Closed → Open → Closed。

    每个数据源可配置独立的熔断器实例，支持按源名称分别跟踪故障状态。

    状态机:
        ┌─────────┐  连续失败 ≥ 阈值  ┌──────┐  冷却期结束  ┌─────────┐
        │ Closed  │ ───────────────→ │ Open │ ──────────→ │ Closed  │
        │ (正常)  │ ←─────────────── │(熔断)│             │ (恢复)  │
        └─────────┘    请求成功       └──────┘             └─────────┘

    行为:
        冷却期结束后，自动恢复为 Closed 状态，所有请求正常放行。
        如果恢复后再次连续失败，重新进入 Open。

    线程安全性:
        使用 threading.Lock 保护所有状态读写，多线程并发调用安全。
    """

    _CLOSED = "closed"
    _OPEN = "open"

    def __init__(
        self,
        failure_threshold: int = 3,
        cooldown_seconds: float = 120.0,
        name: str = "default",
    ):
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds
        self._name = name
        self._failures: Dict[str, int] = {}
        self._state: Dict[str, str] = {}
        self._tripped_at: Dict[str, float] = {}
        self._lock = threading.Lock()

    def is_available(self, source: str) -> bool:
        with self._lock:
            state = self._state.get(source, self._CLOSED)
            if state == self._CLOSED:
                return True
            # OPEN — 检查冷却是否到期
            elapsed = time.time() - self._tripped_at[source]
            if elapsed >= self._cooldown_seconds:
                # 冷却到期，直接恢复 Closed
                self._state[source] = self._CLOSED
                self._failures[source] = 0
                self._tripped_at.pop(source, None)
                logger.info("[熔断器:%s] %s 冷却结束，恢复正常", self._name, source)
                return True
            return False

    def record_success(self, source: str):
        with self._lock:
            self._failures[source] = 0
            self._state.pop(source, None)
            self._tripped_at.pop(source, None)

    def record_failure(self, source: str, reason: str = ""):
        with self._lock:
            # 已经在熔断中，不重复计数
            if self._state.get(source) == self._OPEN:
                return
            self._failures[source] = self._failures.get(source, 0) + 1
            if self._failures[source] >= self._failure_threshold:
                self._state[source] = self._OPEN
                self._tripped_at[source] = time.time()
                logger.warning(
                    "[熔断器:%s] %s 连续失败 %d 次，熔断 %ds (原因: %s)",
                    self._name, source, self._failures[source],
                    self._cooldown_seconds, reason,
                )

    def reset(self, source: str = None):
        with self._lock:
            if source:
                self._failures.pop(source, None)
                self._state.pop(source, None)
                self._tripped_at.pop(source, None)
            else:
                self._failures.clear()
                self._state.clear()
                self._tripped_at.clear()


# 全局熔断器实例
_realtime_cb = CircuitBreaker(failure_threshold=3, cooldown_seconds=120.0, name="realtime")


def get_realtime_circuit_breaker() -> CircuitBreaker:
    """获取实时行情熔断器实例"""
    return _realtime_cb

# ================================================================
# 全局常量
# ================================================================

# 单次 fetch 的超时上限（秒）。
# Coordinator 层的兜底超时，防止某个源的 fetch_fn 卡死导致整个队列阻塞。
# 比 SourceConfig 里的超时更严格 — 这是硬上限。
PER_TASK_TIMEOUT = 60.0

# 队列为空后等待新任务的超时（秒）。
# worker 线程取不到任务时会阻塞等待，超时后认为所有工作已完成，退出循环。
QUEUE_DRAIN_TIMEOUT = 3.0

# 单个源连续失败次数上限。超过后该源的 worker 线程自动退出，不再尝试。
# 避免一个完全不可用的源反复失败浪费时间。
MAX_SOURCE_FAILS = 5

# 超时辅助线程池 — 长生命周期，避免每次 _fetch_with_timeout 都创建新线程池。
# max_workers=8 足够覆盖并发的超时监控需求（实际 fetch 在主池执行，这里只是等待+取消）。
_timeout_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=8, thread_name_prefix="coord-timeout"
)
atexit.register(_timeout_pool.shutdown, wait=False)


# ================================================================
# 输入标准化 — 统一的 symbols 去重+加前缀
# ================================================================

def _normalize_symbols(symbols, market: str) -> List[str]:
    """
    输入标准化: 给 symbols 加市场前缀 + 去重（保序）。

    Args:
        symbols: 股票代码列表或逗号分隔字符串
        market:  市场名称（"CNStock" / "HKStock" / ...）

    Returns:
        去重后的带前缀代码列表（如 ["SH600519", "SZ000001"]）
    """
    from app.data_sources.normalizer import add_market_prefix

    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(',') if s.strip()]

    seen: set = set()
    result: List[str] = []
    for s in symbols:
        ns = add_market_prefix(s, market)
        if ns and ns not in seen:
            seen.add(ns)
            result.append(ns)
    return result


def _adjust_quotes(quotes: Dict[str, Dict[str, Any]]) -> None:
    """
    对批量行情做前复权（原地修改）。

    将每个 quote 包装为单 bar kline，调用 apply_fwd_adjust 复权后回写 OHLC。
    quote 需包含 close/open/high/low/volume/time 字段。
    复权失败时保留原始不复权价格（不中断流程）。
    """
    from app.data_sources.provider.adjustment import apply_fwd_adjust

    for code, q in quotes.items():
        close = q.get("close", q.get("last", 0))
        if not close:
            continue
        bar = {
            "time": q.get("time", ""),
            "open": q.get("open", 0),
            "high": q.get("high", 0),
            "low": q.get("low", 0),
            "close": close,
            "volume": q.get("volume", 0),
        }
        try:
            adjusted = apply_fwd_adjust([bar], code)
        except Exception as e:
            logger.warning("[复权] %s 失败，保留不复权价格: %s", code, e)
            continue
        if adjusted and len(adjusted) == 1:
            ab = adjusted[0]
            q["open"] = ab["open"]
            q["high"] = ab["high"]
            q["low"] = ab["low"]
            q["close"] = ab["close"]
            q["last"] = ab["close"]


def _is_valid_kline(bars) -> bool:
    """
    校验 K 线数据是否有效。

    无效判定:
      - None / 非 list
      - 空 list
      - list 内容不是 dict（如 ["error", "N/A"]）
      - dict 里关键字段全为空/0/NaN

    Returns:
        True = 数据可用, False = 应丢弃并重试
    """
    if not bars or not isinstance(bars, list):
        return False
    if len(bars) == 0:
        return False
    # 至少第一个元素得是 dict
    if not isinstance(bars[0], dict):
        return False
    # 检查是否有至少一个非空值（排除 {"code": "SH600519"} 这种只有 code 没行情的）
    first = bars[0]
    has_data = any(
        v is not None and v != "" and v != 0 and v != "0"
        for k, v in first.items()
        if k not in ("code", "symbol", "name")  # 排除标识字段
    )
    return has_data


# ================================================================
# Provider 适配器 — 统一接口签名
# ================================================================
#
# 背景: Provider 层的接口签名和 Coordinator 期望的不一致。
# Provider 返回 NotSupportedResult（表示"我不支持这个"），Coordinator 期望 None。
# 这两个适配器做的就是这个转换。
#

def _make_provider_fetch_fn(provider, adj: str = "qfq") -> Callable:
    """
    K线适配器: 把 Provider.fetch_kline 包装成 Coordinator 能用的 fetch_fn。

    签名转换:
      Provider:  provider.fetch_kline(code, timeframe, count, adj="qfq") -> List[Dict] | NotSupportedResult
      Coordinator 期望:  fetch_fn(symbol, timeframe, limit) -> List[Dict] | None

    转换规则:
      - NotSupportedResult（布尔值为 False）→ 返回 None → Coordinator 跳过该源
      - 空列表 → 返回 None → Coordinator 判定失败，尝试下一个源
      - 非空列表 → 直接返回 → Coordinator 判定成功

    Args:
        adj: 复权方式 — "qfq"(前复权,默认) / "hfq"(后复权) / ""(不复权)
    """
    def fetch_fn(symbol: str, timeframe: str, limit: int):
        try:
            result = provider.fetch_kline(symbol, timeframe, limit, adj=adj)
            if not result:  # None / [] / NotSupportedResult 都走这里
                return None
            return result
        except Exception as e:
            logger.debug("[适配器] %s.fetch_kline(%s) 异常: %s",
                        provider.name, symbol, e)
            return None

    fetch_fn.__name__ = f"provider_{provider.name}"
    return fetch_fn


def _make_provider_quote_fn(provider) -> Callable:
    """
    行情适配器: 把 Provider.fetch_ticker 包装成 Coordinator 能用的 fetch_fn。

    签名转换:
      Provider:  provider.fetch_ticker(code, timeout=8) -> Dict | None | NotSupportedResult
      Coordinator 期望:  fetch_fn(symbol) -> Dict | None

    注意: fetch_fn 只接收 symbol 一个参数（和 K线适配器不同，没有 timeframe/limit）。
    """
    def fetch_fn(symbol: str):
        try:
            result = provider.fetch_ticker(symbol)
            if not result:
                return None
            return result
        except Exception as e:
            logger.debug("[适配器] %s.fetch_ticker(%s) 异常: %s",
                        provider.name, symbol, e)
            return None

    fetch_fn.__name__ = f"provider_{provider.name}_quote"
    return fetch_fn


def _discover_sources(
    market: str,
    timeframe: str,
    preferred_source: str = "",
    capability: str = "kline",
    adj: str = "qfq",
    skip_cb_filter: bool = False,
) -> List[Tuple[str, Callable, SourceConfig]]:
    """
    源自动发现 — 从 Provider 层获取可用数据源列表。

    这是 Coordinator "不硬编码数据源" 的关键。调用方只需告诉 Coordinator
    "我要 CNStock 的 K线"，Coordinator 自己去找哪些 Provider 能提供。

    流程:
      1. 调用 Provider 层的 get_providers() → 按 priority 排序的 Provider 列表
      2. 过滤掉已熔断的源（_realtime_cb.is_available）
      3. 用适配器把 Provider 的 fetch 方法转成 Coordinator 的 fetch_fn
      4. 如果指定了 preferred_source，将其排到第一位

    Args:
        market:    市场名称（"CNStock" / "HKStock" / "USStock" / ...）
        timeframe: K线周期（"1D" / "5m" / ...）。capability="quote" 时可为空。
        preferred_source: 指定的首选源名称（如 "tencent"）
        capability: 能力类型
          - "kline"  → 获取K线数据（默认）
          - "quote"  → 获取实时行情
        adj: 复权方式（仅 capability="kline" 时生效）
          - "qfq"  → 前复权（默认）
          - "hfq"  → 后复权
          - ""     → 不复权

    Returns:
        [(源名称, fetch_fn, 源配置), ...]
        fetch_fn 签名:
          - capability="kline": fetch_fn(symbol, timeframe, limit) -> List[Dict] | None
          - capability="quote": fetch_fn(symbol) -> Dict | None
    """
    from app.data_sources.provider import get_providers

    # 从 Provider 层获取按 priority 排序的源
    providers = get_providers(
        capability=capability,
        timeframe=timeframe if capability == "kline" else None,
        market=market,
    )

    if not providers:
        logger.warning("[协助层] Provider 层无可用源: market=%s capability=%s", market, capability)
        return []

    result = []
    preferred_item = None

    # 根据 capability 选择适配器（K线 vs 行情的接口签名不同）
    if capability == "quote":
        adapter = _make_provider_quote_fn
    else:
        # K线适配器: 传入 adj，由适配器闭包捕获
        adapter = lambda p: _make_provider_fetch_fn(p, adj=adj)

    for p in providers:
        # 熔断检查 — 跳过已熔断的源（skip_cb_filter=True 时跳过此检查）
        if not skip_cb_filter and not _realtime_cb.is_available(p.name):
            logger.debug("[协助层] Provider %s 已熔断，跳过", p.name)
            continue

        # 获取源配置（含 max_workers、超时等并发参数）
        cfg = get_source_config(p.name)

        # 适配 fetch_fn
        fetch_fn = adapter(p)

        item = (p.name, fetch_fn, cfg)

        # 指定源单独记下，最后排到第一位
        if preferred_source and p.name == preferred_source:
            preferred_item = item
        else:
            result.append(item)

    # 指定源排第一
    if preferred_item:
        logger.info("[协助层] 使用指定源 %s (优先), 回退源 %d 个",
                   preferred_source, len(result))
        result.insert(0, preferred_item)
    elif preferred_source:
        logger.warning("[协助层] 指定源 %s 不可用，使用默认分配", preferred_source)

    return result


# ================================================================
# 线程安全的阻塞任务队列
# ================================================================
#
# 为什么不用 queue.Queue？
# 因为需要 put_back（放回队尾）功能 — 当一个源获取某 symbol 失败时，
# 把这个 symbol 放回队列让其他源尝试。标准库的 Queue 没有这个语义。
#

class _WorkQueue:
    """
    阻塞任务队列 — 支持"取任务 → 失败放回 → 其他源接手"的工作模式。

    典型流程:
      1. worker A 从队列取到 symbol "AAPL"
      2. worker A 用 tencent 源获取失败
      3. 调用 put_back("AAPL") 放回队尾
      4. worker B（sina 源）取到 "AAPL"，获取成功
      5. 调用 task_done() 标记完成

    线程安全: 所有操作都加了 threading.Condition 锁。
    """

    def __init__(self, items: List[str]):
        self._items = list(items)
        self._cond = threading.Condition()
        self._done = False      # True 表示"所有工作已完成，不再接受新任务"
        self._pending = 0       # 正在被 worker 处理中的任务数

    def get(self) -> Optional[str]:
        """
        取下一个任务。

        行为:
          - 队列有任务 → 立刻返回
          - 队列空但有 pending 任务 → 阻塞等待（有 pending 时最多等 60s，无 pending 时等 QUEUE_DRAIN_TIMEOUT）
          - 队列空且无 pending 任务 → 返回 None（worker 应退出）

        Returns:
            symbol 字符串，或 None（表示可以退出了）
        """
        with self._cond:
            while not self._items:
                if self._done:
                    return None
                # 有 pending 任务时多等 — 其他 worker 可能失败后 put_back
                # 无 pending 时快速退出 — 确实没活干了
                wait_time = 60.0 if self._pending > 0 else QUEUE_DRAIN_TIMEOUT
                notified = self._cond.wait(timeout=wait_time)
                if not notified and not self._items:
                    return None
            self._pending += 1
            return self._items.pop(0)

    def put_back(self, sym: str):
        """
        放回队尾 — 当某源获取失败时，把 symbol 放回让其他源接手。

        这是动态队列的核心: 一个源失败不代表 symbol 失败，放回去让别的源试。
        """
        with self._cond:
            self._items.append(sym)
            self._pending = max(0, self._pending - 1)
            self._cond.notify()  # 唤醒一个等待的 worker

    def task_done(self):
        """
        标记一个任务完成（成功，不再放回队列）。

        当队列空且 pending 归零时，唤醒所有等待线程（让它们退出）。
        """
        with self._cond:
            self._pending = max(0, self._pending - 1)
            if not self._items and self._pending == 0:
                self._cond.notify_all()

    def drain_done(self):
        """
        强制标记所有工作完成 — 用于超时后强制唤醒所有等待的 worker 线程。
        """
        with self._cond:
            self._done = True
            self._cond.notify_all()

    @property
    def is_empty(self) -> bool:
        with self._cond:
            return len(self._items) == 0


# ================================================================
# 协助层主类
# ================================================================

class Coordinator:
    """
    协助层 — 并发调度引擎。

    提供三种调度模式:
      - coordinate_kline:        K线批量获取（动态队列 + 多源 fallback）
      - coordinate_ticker:       实时行情（单股Race抢答 / 多股记忆源+轮询）
      - coordinate_market_kline: 全市场批量K线（Coordinator分组 + 多Provider并发取组）
      - coordinate_batch_quotes: 批量行情（单源批量请求 + 多源 fallback）

    两种模式的区别:
      coordinate_kline:  N只股票 × M个源 → 动态分配 → 每只股票只要有一个源成功就行
      coordinate_ticker: 单股 → Race抢答 / 多股 → 记忆源优先+轮询 → 自动路由
      coordinate_market_kline: 全市场 × Coordinator分组 → 多Provider并发取组 → Provider调自己的fetch_market_kline → 合并结果
      coordinate_batch_quotes: N只股票 × 单源批量 → 逐源 fallback → 第一个成功的直接用
    """

    def __init__(self):
        self._lock = threading.Lock()
        # per-provider EWMA 响应时间
        self._ewma_rt: Dict[str, float] = {}
        self._ewma_lock = threading.Lock()

    # ── EWMA 响应时间追踪 ──

    _EWMA_ALPHA = 0.3

    def _update_ewma(self, provider_name: str, rt: float):
        """更新 Provider 的 EWMA 响应时间。rt<=0 不更新。"""
        if rt <= 0:
            return
        with self._ewma_lock:
            old = self._ewma_rt.get(provider_name)
            if old is None:
                self._ewma_rt[provider_name] = rt
            else:
                self._ewma_rt[provider_name] = self._EWMA_ALPHA * rt + (1 - self._EWMA_ALPHA) * old

    def allocate_threads(self, providers: list, global_budget: int = 32, symbol_count: int = 0) -> Dict[str, int]:
        """按 max_concurrency / EWMA 响应时间加权分配线程数。symbol_count > 0 时限制总线程数。"""
        if not providers:
            return {}
        weights = {}
        for p in providers:
            max_c = getattr(p, 'max_concurrency', 4)
            with self._ewma_lock:
                rt = self._ewma_rt.get(p.name, 1.0)
            weights[p.name] = max_c / max(rt, 0.1)
        total = sum(weights.values())
        if total <= 0:
            return {p.name: 1 for p in providers}
        alloc = {}
        for p in providers:
            max_c = getattr(p, 'max_concurrency', 4)
            raw = global_budget * weights[p.name] / total
            alloc[p.name] = max(1, min(round(raw), max_c))
        # 当 symbol 数量远少于线程数时，限制总线程避免无意义竞争
        if symbol_count > 0:
            total_alloc = sum(alloc.values())
            max_useful = min(symbol_count * len(providers), total_alloc)
            if total_alloc > max_useful and max_useful > 0:
                scale = max_useful / total_alloc
                for name in alloc:
                    alloc[name] = max(1, round(alloc[name] * scale))
        return alloc

    # ================================================================
    # 模式 A: K线获取 — 假批量，动态队列 + 多源 fallback
    # ================================================================
    #
    # 注意: 这里说的"批量"是假批量。
    #   - 没有真正的批量 API，每个 symbol 都是逐只单独请求 Provider
    #   - 所谓"批量"靠的是动态队列 + 多线程并发模拟出来的
    #   - 真正的批量接口见 coordinate_batch_quotes（单次请求拿多只）
    #
    # 工作流程（以 3 只股票、2 个源为例）:
    #
    #   初始队列: [AAPL, TSLA, MSFT]
    #   tencent worker 1 取到 AAPL → 获取成功 → 从队列移除
    #   tencent worker 2 取到 TSLA → 获取失败 → put_back 放回队尾
    #   sina worker 1 取到 MSFT → 获取成功 → 从队列移除
    #   sina worker 2 空闲 → 取到 TSLA（被 tencent 放回的）→ 获取成功
    #
    #   结果: AAPL(tencent) MSFT(sina) TSLA(sina) — 全部成功
    #
    # 关键设计:
    #   - 每个源的并发数由 SourceConfig.max_workers 控制
    #   - 一个源连续失败 MAX_SOURCE_FAILS 次后自动停用（不浪费时间）
    #   - 每个 symbol 会被所有可用源各试一次，全部失败才算失败
    #

    def coordinate_kline(
        self,
        symbols: List[str],
        timeframe: str,
        limit: int,
        market: str = "",
        timeout: float = 15.0,
        preferred_source: str = "",
        sources: Optional[List[Tuple[str, Callable]]] = None,
        adj: str = "qfq",
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
        """
        K线批量获取 — 动态队列模式。

        这是 Coordinator 最核心的方法。当需要批量拉取多只股票的K线时调用。

        典型调用方:
          - CNStockDataSource.get_kline_batch()
          - KlineService（批量分析场景）
          - BacktestService（回测数据准备）

        Args:
            symbols:   股票代码列表（1 只或多只均可）
            timeframe: K 线周期（"1D" / "5m" / "1H" / ...）
            limit:     K 线条数
            market:    市场名称（"CNStock" / "HKStock"），用于自动发现源
            timeout:   总超时（秒），超时后未完成的 symbol 记为失败
            preferred_source: 指定首选源（如 "tencent"），优先使用，失败后回退
            sources:   手动指定源列表（可选）。为 None 时自动从 Provider 层发现。
                       格式: [(name, fetch_fn), ...]
                       fetch_fn 签名: fetch_fn(symbol, timeframe, limit) -> List[Dict] | None
            adj:       复权方式 — "qfq"(前复权,默认) / "hfq"(后复权) / ""(不复权)

        Returns:
            (results, failed)
            - results: {symbol: [kline_bars]} — 仅包含成功获取到数据的 symbol
            - failed:  [symbol, ...] — 所有源都尝试过但全部失败的 symbol
        """
        if not symbols:
            return {}, list(symbols)

        # ── 第一步: 获取可用源列表 ──
        # 两种方式: 自动发现（推荐）或 手动指定（兼容旧代码）
        if sources is not None:
            # 手动指定模式 — 调用方传入 [(name, fetch_fn), ...]
            source_map = {name: fn for name, fn in sources}
            if preferred_source and preferred_source in source_map:
                available = self._get_preferred_available(
                    preferred_source, market, source_map
                )
            else:
                available = self._get_available_sources(market, source_map)
        else:
            # 自动发现模式 — 从 Provider 层获取源
            discovered = _discover_sources(market, timeframe, preferred_source, adj=adj)
            if not discovered:
                logger.warning("[协助层] 市场 %s 无可用源", market)
                return {}, list(symbols)
            available = [(name, cfg) for name, _, cfg in discovered]
            source_map = {name: fn for name, fn, _ in discovered}

        if not available:
            logger.warning("[协助层] 市场 %s 无可用源", market)
            return {}, list(symbols)

        # ── 第二步: 初始化动态队列和共享状态 ──
        wq = _WorkQueue(symbols)                    # 任务队列
        results: Dict[str, List[Dict[str, Any]]] = {}  # 成功的结果
        results_lock = threading.Lock()
        failed: List[str] = []                       # 全部源都失败的 symbol
        failed_lock = threading.Lock()

        # 记录每个 symbol 已经被哪些源尝试过（避免重复尝试）
        symbol_tried: Dict[str, Set[str]] = {}
        symbol_tried_lock = threading.Lock()

        # 记录每个源的连续失败次数（超过 MAX_SOURCE_FAILS 后该源自动停用）
        source_consecutive_fails: Dict[str, int] = {}
        fails_lock = threading.Lock()

        # ── 第三步: 定义内部辅助函数 ──

        def _get_consecutive_fails(name: str) -> int:
            """查询某源的连续失败次数"""
            with fails_lock:
                return source_consecutive_fails.get(name, 0)

        def _inc_consecutive_fails(name: str):
            """某源失败一次，连续失败计数 +1"""
            with fails_lock:
                source_consecutive_fails[name] = source_consecutive_fails.get(name, 0) + 1

        def _reset_consecutive_fails(name: str):
            """某源成功一次，连续失败计数归零"""
            with fails_lock:
                source_consecutive_fails[name] = 0

        def _mark_success(sym: str, bars: List[Dict[str, Any]], source_name: str):
            """
            标记某 symbol 获取成功。
            成功后该 symbol 从队列中彻底移除（不再让其他源尝试）。

            Returns:
                True  = 首次成功（调用方应 task_done）
                False = 已被其他源抢先成功（调用方不应 task_done，避免重复计数）
            """
            with results_lock:
                if sym in results:
                    return False  # 已被其他源成功获取，忽略重复
                results[sym] = bars
                return True

        def _mark_failed(sym: str, source_name: str):
            """
            标记某源对某 symbol 获取失败。

            行为:
              - 如果还有未尝试的源 → 把 symbol 放回队列（让其他源接手）
              - 如果所有源都试过了 → 标记为彻底失败，从队列中移除

            这就是"动态队列"的核心: 一个源失败 ≠ symbol 失败，放回去让别的源试。
            """
            with results_lock:
                if sym in results:
                    return  # 已经被其他源成功获取了，忽略

            with symbol_tried_lock:
                tried = symbol_tried.setdefault(sym, set())
                tried.add(source_name)
                untried = [name for name, _ in available if name not in tried]

            if untried:
                # 二次检查: put_back 前再确认一次，避免已成功的 symbol 被重复放回
                with results_lock:
                    if sym in results:
                        return
                # 还有未尝试的源 → 放回队尾，让其他 worker 接手
                wq.put_back(sym)
            else:
                # 所有源都试过了，全部失败 → 彻底放弃
                with failed_lock:
                    if sym not in failed:
                        failed.append(sym)
                wq.task_done()

        def _fetch_with_timeout(fn: Callable, sym: str, tf: str, lim: int,
                                timeout_s: float) -> Optional[List[Dict[str, Any]]]:
            """
            带超时的单次 fetch 调用。

            使用全局共享的 _timeout_pool 执行 fetch_fn，超时后自动取消。
            防止某个源的 fetch_fn 卡死（比如网络不通但不报错）导致整个队列阻塞。
            """
            future = _timeout_pool.submit(fn, sym, tf, lim)
            try:
                return future.result(timeout=timeout_s)
            except concurrent.futures.TimeoutError:
                logger.warning("[协助层] %s 获取 %s 超时 (%ss)", fn.__name__, sym, timeout_s)
                future.cancel()
                return None

        def _process_symbol(sym: str, source_name: str, fetch_fn: Callable,
                            cfg: SourceConfig) -> bool:
            """
            处理单个 symbol 的获取请求。

            流程:
              1. 检查是否已被其他源成功获取（避免重复工作）
              2. 记录该源已尝试过此 symbol
              3. 调用 fetch_fn 获取数据（带超时保护）
              4. 成功 → 记录结果 + 重置连续失败计数
              5. 失败 → 记录失败 + 递增连续失败计数 + 可能放回队列

            Returns:
                True = 获取成功, False = 获取失败
            """
            # 已被其他源成功获取，跳过
            with results_lock:
                if sym in results:
                    wq.task_done()
                    return True

            # 记录"该源已尝试过此 symbol"
            with symbol_tried_lock:
                symbol_tried.setdefault(sym, set()).add(source_name)

            start_time = time.time()
            try:
                # 调用 fetch_fn（带超时保护）
                bars = _fetch_with_timeout(fetch_fn, sym, timeframe, limit, PER_TASK_TIMEOUT)
                elapsed = time.time() - start_time

                if bars:
                    # 成功
                    _realtime_cb.record_success(source_name)       # 通知熔断器
                    cfg.record(True, elapsed)            # 记录统计
                    is_first = _mark_success(sym, bars, source_name)
                    _reset_consecutive_fails(source_name)
                    if is_first:
                        wq.task_done()
                    return True
                else:
                    # 失败（返回了空结果）
                    _realtime_cb.record_failure(source_name, "empty")
                    cfg.record(False, elapsed)
                    _inc_consecutive_fails(source_name)
                    _mark_failed(sym, source_name)       # 可能放回队列
                    return False
            except Exception as e:
                # 失败（抛了异常）
                elapsed = time.time() - start_time
                _realtime_cb.record_failure(source_name, str(e))
                cfg.record(False, elapsed)
                logger.debug("[协助层] %s 获取 %s 失败: %s", source_name, sym, e)
                _inc_consecutive_fails(source_name)
                _mark_failed(sym, source_name)
                return False

        def _worker(source_name: str, cfg: SourceConfig, fetch_fn: Callable):
            """
            单个源的 worker 线程主循环。

            不断从队列取 symbol → 获取数据 → 成功/失败处理，直到:
              - 队列为空（get() 返回 None）
              - 连续失败过多（>= MAX_SOURCE_FAILS）
              - 源被熔断（_realtime_cb.is_available 返回 False）
            """
            while True:
                # 检查是否应该退出
                if _get_consecutive_fails(source_name) >= MAX_SOURCE_FAILS:
                    break
                if not _realtime_cb.is_available(source_name):
                    break

                # 从队列取下一个 symbol
                sym = wq.get()
                if sym is None:
                    break  # 队列为空，退出

                _process_symbol(sym, source_name, fetch_fn, cfg)

        # ── 第四步: 构建线程池并启动 ──
        #
        # 每个源分配 max_workers 个线程，所有源的线程放在同一个线程池里。
        # 例如: tencent(max_workers=3) + sina(max_workers=2) → 总共 5 个线程
        #
        total_threads = 0
        thread_plan = []
        for name, cfg in available:
            fn = source_map[name]
            # 线程数取 max_workers 和 symbols 数量的较小值（没必要开比 symbol 还多的线程）
            tc = min(cfg.max_workers, len(symbols))
            thread_plan.append((name, cfg, fn, tc))
            total_threads += tc

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=total_threads, thread_name_prefix="coord"
        ) as pool:
            futures = []
            for name, cfg, fn, tc in thread_plan:
                for _ in range(tc):
                    futures.append(pool.submit(_worker, name, cfg, fn))

            # 等待所有 worker 完成（加 2 秒余量）
            concurrent.futures.wait(futures, timeout=timeout + 2)

        # ── 第五步: 清理 — 收集剩余未处理的 symbol ──
        wq.drain_done()

        while True:
            sym = wq.get()
            if sym is None:
                break
            with results_lock:
                if sym in results:
                    continue
            with failed_lock:
                if sym not in failed:
                    failed.append(sym)

        # 输出统计日志
        stats = " | ".join(cfg.stats_summary() for _, cfg in available)
        logger.info("[协助层] 完成: %d成功 %d失败 | %s", len(results), len(failed), stats)

        return results, failed

    # ================================================================
    # 模式 B: 实时行情 — 单股Race抢答 / 多股记忆源+轮询
    # ================================================================
    #
    # 和 coordinate_kline 的区别:
    #   coordinate_kline:  N只股票，动态队列，每只股票可能被多个源依次尝试
    #   coordinate_ticker: 单股→Race并发抢答 / 多股→记忆源优先+轮询
    #
    # 单股为什么用 Race？
    #   实时行情对延迟敏感。与其等一个源超时再试下一个，不如同时发请求，
    #   谁先返回有效数据就用谁。网络好的源 100ms 就返回了，不用等慢的源 5 秒超时。
    #
    # 多股为什么用记忆源？
    #   批量行情走 fetch_batch_quotes（单次HTTP拿多只），不适合 Race。
    #   记住上次成功的源，下次直接命中，省去轮询开销。
    #

    def coordinate_ticker(
        self,
        symbols,
        sources: Optional[List[Tuple[str, Callable]]] = None,
        timeout: float = 8.0,
        preferred_source: str = "",
        market: str = "",
        max_race_sources: int = 3,
    ) -> Dict[str, Any]:
        """
        实时行情 — 统一入口，自动路由单股/批量。

        路由规则:
          单股 → Race 多源并发抢答，第一个返回有效价格的直接用
          多股 → 记忆源优先，成功直接返回；失败则按 priority 轮询其他 Provider

        典型调用方:
          - CNStockDataSource.get_ticker()
          - KlineService.get_realtime_price()
          - 自选股价格刷新

        Args:
            symbols: 股票代码，str 或 List[str]。str 可含逗号（自动拆分）。
            sources: [(name, fetch_fn), ...]。为 None 时自动发现。
                     fetch_fn 签名: fetch_fn(symbol) -> Dict | None
            timeout: 超时（秒）
            preferred_source: 指定首选源。如果可用，优先使用。
            market:  市场名称（"CNStock"），用于自动发现源

        Returns:
            {symbol: quote_dict} — 仅包含成功获取到的 symbol。
            全部失败返回空 dict。
        """
        # ── 入口标准化 ──
        if isinstance(symbols, str):
            sym_list = [s.strip() for s in symbols.split(',') if s.strip()]
        else:
            sym_list = [s.strip() for s in symbols if s and s.strip()]

        if not sym_list:
            return {}

        if len(sym_list) == 1:
            # 单股 → Race 抢答
            result = self._ticker_race(
                symbol=sym_list[0],
                sources=sources,
                timeout=timeout,
                preferred_source=preferred_source,
                market=market,
                max_race_sources=max_race_sources,
            )
            return {sym_list[0]: result} if result else {}
        else:
            # 多股 → 记忆源 + 轮询批量调度（独立逻辑，不调用 coordinate_batch_quotes_sticky）
            return self._ticker_batch(
                symbols=sym_list,
                market=market,
                timeout=timeout,
            )

    def _ticker_race(
        self,
        symbol: str,
        sources: Optional[List[Tuple[str, Callable]]],
        timeout: float,
        preferred_source: str,
        market: str,
        max_race_sources: int,
    ) -> Optional[Dict[str, Any]]:
        """
        单股 Race 抢答 — 所有源并发，第一个返回有效价格的直接用。

        Args:
            symbol: 股票代码（单只）

        Returns:
            第一个成功获取到的有效 Dict，全部失败返回 None。
        """
        # ── 获取可用源 ──
        if sources is not None:
            if not sources:
                return None
            if preferred_source:
                preferred = [(n, fn) for n, fn in sources if n == preferred_source and _realtime_cb.is_available(n)]
                others = [(n, fn) for n, fn in sources if n != preferred_source and _realtime_cb.is_available(n)]
                available = preferred + others
            else:
                available = [(name, fn) for name, fn in sources if _realtime_cb.is_available(name)]
        else:
            discovered = _discover_sources(
                market=market,
                timeframe="",
                preferred_source=preferred_source,
                capability="quote",
                skip_cb_filter=True,
            )
            if not discovered:
                logger.warning("[协助层] ticker %s market=%s 无可用源", symbol, market)
                return None
            available = [(name, fn) for name, fn, _ in discovered]

        if not available:
            logger.warning("[协助层] ticker %s 无可用源", symbol)
            return None

        if max_race_sources > 0 and len(available) > max_race_sources:
            available = available[:max_race_sources]

        # ── Race: 并发抢答 ──
        result_holder: List[Tuple[str, Dict[str, Any]]] = []
        done_event = threading.Event()
        lock = threading.Lock()

        def _race_one(source_name: str, fetch_fn: Callable):
            if done_event.is_set():
                return
            if not _realtime_cb.is_available(source_name):
                return
            try:
                start = time.time()
                result = fetch_fn(symbol)
                elapsed = time.time() - start

                if result and ("last" in result or "price" in result):
                    _realtime_cb.record_success(source_name)
                    cfg = get_source_config(source_name)
                    cfg.record(True, elapsed)
                    with lock:
                        if not result_holder:
                            result_holder.append((source_name, result))
                            done_event.set()
                else:
                    _realtime_cb.record_failure(source_name, "empty")
                    cfg = get_source_config(source_name)
                    cfg.record(False, elapsed)
            except Exception as e:
                _realtime_cb.record_failure(source_name, str(e))
                cfg = get_source_config(source_name)
                cfg.record(False, 0)
                logger.debug("[协助层] ticker %s %s 失败: %s", source_name, symbol, e)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(available), thread_name_prefix="ticker-race"
        ) as pool:
            futures = [pool.submit(_race_one, name, fn) for name, fn in available]
            done_event.wait(timeout=timeout)

        if result_holder:
            source_name, result = result_holder[0]
            logger.info("[协助层] ticker %s 命中 %s", symbol, source_name)
            return result

        logger.warning("[协助层] ticker %s 所有源失败", symbol)
        return None

    def _ticker_batch(
        self,
        symbols: List[str],
        market: str,
        timeout: float,
    ) -> Dict[str, Any]:
        """
        批量行情 — 记忆源优先 + 熔断过滤。

        委托给 coordinate_batch_quotes_sticky（skip_cb_filter=False）。
        """
        return self.coordinate_batch_quotes_sticky(
            symbols=symbols, market=market, timeout=timeout, skip_cb_filter=False,
        )

    # ================================================================
    # 模式 C: 批量行情 — 优先走 fetch_batch_quotes
    # ================================================================

    _RACE_BATCH_THRESHOLD = 500   # ≤500 走 RACE，>500 走分组并发轮询
    _BATCH_GROUP_SIZE = 500       # 大批量分组每组 500 只

    @staticmethod
    def _normalize_batch_quotes_result(
        raw: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """
        标准化批量行情返回结果:
          - key 统一为纯数字代码 (600519)
          - quote["symbol"] 统一为纯数字代码
          - quote["name"] 若与 symbol 重复则清空
          - OHLC 前复权（Provider 返回不复权原始价，此处统一做前复权）
        """
        from app.data_sources.normalizer import strip_market_prefix

        out: Dict[str, Dict[str, Any]] = {}
        for key, quote in raw.items():
            digits = strip_market_prefix(key)
            if isinstance(quote, dict):
                quote["symbol"] = digits
                raw_name = quote.get("name", "")
                if raw_name and strip_market_prefix(raw_name) == digits:
                    quote["name"] = ""
            out[digits] = quote

        # ── 前复权: 将 quote 包装为单 bar kline 列表，批量复权后回写 ──
        if out:
            _adjust_quotes(out)

        return out

    @staticmethod
    def _normalize_market_kline_result(
        raw: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        标准化全市场K线返回结果:
          - key 统一为纯数字代码
        """
        from app.data_sources.normalizer import strip_market_prefix
        out: Dict[str, List[Dict[str, Any]]] = {}
        for key, bars in raw.items():
            out[strip_market_prefix(key)] = bars
        return out

    def coordinate_batch_quotes(
        self,
        symbols: List[str],
        market: str = "",
        timeout: float = 15.0,
        preferred_source: str = "",
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量行情获取 — 自适应双模式调度。

        输入: symbols 可以是纯数字 (600519) 或带前缀 (SH600519)，内部自动加前缀。
        输出: key 统一为纯数字 (600519)，name/symbol 字段标准化，OHLC 前复权。

        模式选择:
          ≤ 500 只: RACE 模式 — 多源并发抢答，第一个返回非空结果的直接用
          > 500 只: 分组并发轮询 — 按 500 只一组分组，共享队列 + 多源并发消费
                    (与 coordinate_market_kline 同一架构)

        Args:
            symbols: 股票代码列表（纯数字或带前缀均可）
            market:  市场名称（"CNStock" / "HKStock" / ...）
            timeout: 超时（秒）
            preferred_source: 指定首选源（如 "tencent"），优先尝试

        Returns:
            {纯数字代码: quote_dict} — 仅包含成功获取到的 symbol
        """
        if not symbols:
            return {}

        from app.data_sources.provider import get_providers

        # ── 输入标准化 ──
        normalized_symbols = _normalize_symbols(symbols, market)
        if not normalized_symbols:
            return {}

        # 发现支持 batch_quote 的源
        providers = get_providers(capability="batch_quote", market=market)
        if not providers:
            logger.warning("[协助层] batch_quotes market=%s 无可用源", market)
            return {}

        # 按 preferred_source 排序
        if preferred_source:
            preferred = [p for p in providers if p.name == preferred_source]
            others = [p for p in providers if p.name != preferred_source]
            providers = preferred + others

        # 过滤已熔断的源
        available = [p for p in providers if _realtime_cb.is_available(p.name)]
        if not available:
            logger.warning("[协助层] batch_quotes market=%s 所有源已熔断", market)
            return {}

        # ── 调度获取 ──
        if len(normalized_symbols) <= self._RACE_BATCH_THRESHOLD:
            raw = self._batch_quotes_race(normalized_symbols, available, timeout)
        else:
            raw = self._batch_quotes_dispatch(normalized_symbols, available, timeout)

        # ── 输出标准化: key 去前缀，name/symbol 统一 + 前复权 ──
        return self._normalize_batch_quotes_result(raw)

    # ── RACE 模式: 多源并发抢答（≤500 只） ──

    def _batch_quotes_race(
        self,
        symbols: List[str],
        available: list,
        timeout: float,
    ) -> Dict[str, Dict[str, Any]]:
        """
        RACE 模式 — 所有源并发请求同一组 symbols，第一个返回非空结果的直接用。
        如果赢家未覆盖全部 symbols，剩余的从其他源补充。
        """
        done_event = threading.Event()
        lock = threading.Lock()
        winner: Dict[str, Dict[str, Any]] = {}
        winner_name = ""

        def _race_one(provider):
            nonlocal winner, winner_name
            if done_event.is_set():
                return
            if not _realtime_cb.is_available(provider.name):
                return
            cfg = get_source_config(provider.name)
            start = time.time()
            try:
                result = provider.fetch_batch_quotes(symbols, timeout=timeout)
                elapsed = time.time() - start
                if result and not done_event.is_set():
                    with lock:
                        if not done_event.is_set():
                            winner = result
                            winner_name = provider.name
                            _realtime_cb.record_success(provider.name)
                            cfg.record(True, elapsed)
                            done_event.set()
                            logger.info("[协助层] batch_quotes RACE %d只 命中 %s (%.2fs)",
                                        len(result), provider.name, elapsed)
                elif not result:
                    cfg.record(False, elapsed)
                    _realtime_cb.record_failure(provider.name, "empty")
            except Exception as e:
                elapsed = time.time() - start
                cfg.record(False, elapsed)
                _realtime_cb.record_failure(provider.name, str(e))
                logger.debug("[协助层] batch_quotes RACE %s 失败: %s", provider.name, e)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(available), thread_name_prefix="bq-race"
        ) as pool:
            futures = [pool.submit(_race_one, p) for p in available]
            done_event.wait(timeout=timeout)

        if not winner:
            logger.warning("[协助层] batch_quotes RACE %d只 所有源失败", len(symbols))
            return winner

        # ── 赢家未覆盖全部 symbols → 从剩余源补充 ──
        from app.data_sources.normalizer import add_market_prefix, strip_market_prefix
        requested_set = set(add_market_prefix(s, "CNStock") for s in symbols)
        # winner 的 key 可能带前缀也可能不带，统一为纯数字
        covered = set()
        for k in winner:
            covered.add(strip_market_prefix(k))
        requested_digits = set(strip_market_prefix(s) for s in requested_set)
        missing_digits = requested_digits - covered

        if not missing_digits:
            return winner

        logger.info(
            "[协助层] batch_quotes RACE %s 覆盖 %d/%d，缺 %d 只，尝试补充",
            winner_name, len(covered), len(requested_digits), len(missing_digits),
        )

        # 从剩余可用源逐个补充缺失 symbols
        remaining = [p for p in available if p.name != winner_name and _realtime_cb.is_available(p.name)]
        for provider in remaining:
            if not missing_digits:
                break
            # 将 missing_digits 转回带前缀形式
            missing_symbols = [add_market_prefix(d, "CNStock") for d in missing_digits]
            try:
                result = provider.fetch_batch_quotes(missing_symbols, timeout=timeout)
                if result:
                    for k, v in result.items():
                        digits = strip_market_prefix(k)
                        if digits in missing_digits:
                            winner[k] = v
                            missing_digits.discard(digits)
                    _realtime_cb.record_success(provider.name)
                    logger.info(
                        "[协助层] batch_quotes 补充 %s 命中 %d 只，剩余 %d 只",
                        provider.name, len(result), len(missing_digits),
                    )
            except Exception as e:
                logger.debug("[协助层] batch_quotes 补充 %s 失败: %s", provider.name, e)

        if missing_digits:
            logger.warning(
                "[协助层] batch_quotes RACE 补充后仍缺 %d 只: %s",
                len(missing_digits), list(missing_digits)[:5],
            )

        return winner

    # ── 分组并发轮询模式（>500 只） ──

    def _batch_quotes_dispatch(
        self,
        symbols: List[str],
        available: list,
        timeout: float,
    ) -> Dict[str, Dict[str, Any]]:
        """
        分组并发轮询 — 与 coordinate_market_kline 同一架构:
          1. symbols 按 500 只一组分组，放入共享队列
          2. 每个 Provider 启动一个 worker 线程
          3. 各 worker 从队列取组 → fetch_batch_quotes → 合并结果
          4. 先完成的 worker 立即取下一组，直到队列为空
          5. 全局超时后合并所有已获取的数据返回
        """
        from queue import Queue, Empty

        group_size = self._BATCH_GROUP_SIZE
        groups = [symbols[i:i + group_size] for i in range(0, len(symbols), group_size)]
        total_groups = len(groups)

        logger.info("[协助层] batch_quotes %d只 → %d组(每组%d) → %d源并发轮询: %s",
                    len(symbols), total_groups, group_size,
                    len(available), " | ".join(p.name for p in available))

        # 共享任务队列
        task_queue: Queue = Queue()
        for idx, group in enumerate(groups):
            task_queue.put((idx, group, 0))

        # 共享结果
        result: Dict[str, Dict[str, Any]] = {}
        result_lock = threading.Lock()

        # 每组超时 & 重试
        per_task_timeout = min(timeout, 60.0)
        max_group_retries = 3

        # 退出计数器
        pending_groups = 0
        pending_lock = threading.Lock()

        # 全局停止信号
        global_stop = threading.Event()

        # 每个 Provider 一个单线程 executor + 状态追踪
        provider_map = {p.name: p for p in available}
        provider_executors = {}
        provider_futures = {}       # name → (future, group_idx, remaining, retry_count)
        provider_consecutive_timeout = {}
        source_stats: Dict[str, Dict[str, int]] = {}
        stats_lock = threading.Lock()

        for p in available:
            provider_executors[p.name] = concurrent.futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix=f"bq-disp-{p.name}"
            )
            provider_futures[p.name] = None
            provider_consecutive_timeout[p.name] = 0
            with stats_lock:
                source_stats[p.name] = {"ok": 0, "fail": 0, "groups": 0, "timeout": 0}

        def _fetch_group(provider, group_codes):
            return provider.fetch_batch_quotes(group_codes, timeout=int(per_task_timeout))

        def _timed_submit(executor, fn, *args):
            """提交任务并记录提交时间（用于超时判断）"""
            future = executor.submit(fn, *args)
            future._submit_time = time.time()
            return future

        def _submit_next(name: str):
            """给 Provider 派下一组（非阻塞）"""
            while True:
                try:
                    group_idx, group_codes, retry_count = task_queue.get_nowait()
                except Empty:
                    return
                with result_lock:
                    remaining = [c for c in group_codes if c not in result]
                if not remaining:
                    continue
                future = _timed_submit(
                    provider_executors[name],
                    _fetch_group,
                    provider_map[name],
                    remaining,
                )
                provider_futures[name] = (future, group_idx, remaining, retry_count)
                with pending_lock:
                    nonlocal pending_groups
                    pending_groups += 1
                with stats_lock:
                    source_stats[name]["groups"] += 1
                return

        def _dispatcher():
            """主调度线程 — 轮询所有 Provider future"""
            nonlocal pending_groups

            while not global_stop.is_set():
                all_idle = True

                for p in available:
                    name = p.name
                    if name not in provider_executors:
                        continue

                    entry = provider_futures.get(name)
                    if entry is None:
                        _submit_next(name)
                        entry = provider_futures.get(name)
                        if entry is None:
                            continue

                    future, group_idx, remaining, retry_count = entry

                    if not future.done():
                        all_idle = False
                        # 检查单任务超时
                        if hasattr(future, '_submit_time'):
                            elapsed = time.time() - future._submit_time
                            if elapsed > per_task_timeout:
                                future.cancel()
                                logger.debug("[协助层] batch_quotes 组%d %s 超时(%.1fs)",
                                             group_idx, name, elapsed)
                                with stats_lock:
                                    source_stats[name]["timeout"] += 1
                                provider_consecutive_timeout[name] = \
                                    provider_consecutive_timeout.get(name, 0) + 1
                                # 超时: 队列充裕则放回重试，否则丢弃
                                with pending_lock:
                                    pending_groups -= 1
                                queued = task_queue.qsize()
                                if retry_count < max_group_retries and queued > len(available):
                                    task_queue.put((group_idx, remaining, retry_count + 1))
                                    logger.debug("[协助层] batch_quotes 组%d 放回重试(%d)",
                                                 group_idx, retry_count + 1)
                                else:
                                    logger.debug("[协助层] batch_quotes 组%d 丢弃", group_idx)
                                provider_futures[name] = None
                                _submit_next(name)
                        continue

                    # future 已完成
                    try:
                        task_result = future.result(timeout=0)
                    except Exception as e:
                        task_result = None
                        logger.debug("[协助层] batch_quotes 组%d %s 异常: %s",
                                     group_idx, name, e)

                    provider_futures[name] = None
                    provider_consecutive_timeout[name] = 0

                    if task_result:
                        with result_lock:
                            merged_count = 0
                            for sym, quote in task_result.items():
                                if sym not in result:
                                    result[sym] = quote
                                    merged_count += 1
                        with stats_lock:
                            source_stats[name]["ok"] += merged_count
                        logger.debug("[协助层] batch_quotes 组%d %s 成功 %d只",
                                     group_idx, name, merged_count)
                        with pending_lock:
                            pending_groups -= 1
                    else:
                        with stats_lock:
                            source_stats[name]["fail"] += 1
                        with pending_lock:
                            pending_groups -= 1
                        # 失败: 放回队尾重试
                        if retry_count < max_group_retries:
                            task_queue.put((group_idx, remaining, retry_count + 1))
                            logger.debug("[协助层] batch_quotes 组%d %s 失败，放回重试(%d)",
                                         group_idx, name, retry_count + 1)

                    _submit_next(name)

                # 检查是否全部完成
                with pending_lock:
                    if pending_groups <= 0 and task_queue.empty():
                        break

                if all_idle:
                    time.sleep(0.05)

        # 启动所有 Provider worker（先各派一组）
        for p in available:
            _submit_next(p.name)

        # 启动 dispatcher 线程
        dispatch_thread = threading.Thread(
            target=_dispatcher, name="bq-dispatcher", daemon=True
        )
        dispatch_thread.start()

        # 等待全局超时
        dispatch_thread.join(timeout=timeout)
        global_stop.set()

        # 关闭 executor
        for name, executor in provider_executors.items():
            executor.shutdown(wait=False)

        # 统计日志
        total_ok = sum(s["ok"] for s in source_stats.values())
        stat_parts = []
        for name, s in source_stats.items():
            if s["groups"] > 0:
                stat_parts.append(f"{name}: {s['ok']}只/{s['groups']}组")
        logger.info("[协助层] batch_quotes 轮询完成 %d/%d只 | %s",
                    total_ok, len(symbols), " | ".join(stat_parts))

        return result

    # ================================================================
    # 模式 D: 全市场批量K线 — 优先走 fetch_market_kline，逐源 fallback
    # ================================================================

    # ================================================================
    # 死源追踪器 — 内存方式，4小时有效期
    # ================================================================

    # _dead_sources: {source_name: last_dead_timestamp}
    # 当 ≥2 个有效源时，超时的源标记为死源，不参与下一轮任务分配
    _dead_sources: Dict[str, float] = {}
    _dead_sources_lock = threading.Lock()
    _DEAD_SOURCE_TTL = 4 * 3600  # 4小时

    def _is_source_dead(self, source_name: str) -> bool:
        """检查源是否被标记为死源（且未过期）"""
        with self._dead_sources_lock:
            ts = self._dead_sources.get(source_name)
            if ts is None:
                return False
            if time.time() - ts > self._DEAD_SOURCE_TTL:
                # 过期，自动恢复
                del self._dead_sources[source_name]
                return False
            return True

    def _mark_source_dead(self, source_name: str):
        """标记源为死源"""
        with self._dead_sources_lock:
            self._dead_sources[source_name] = time.time()
            logger.warning("[协助层] 标记 %s 为死源 (TTL=%dh)", source_name, self._DEAD_SOURCE_TTL // 3600)

    def _mark_source_alive(self, source_name: str):
        """标记源为活源（成功获取数据后调用）"""
        with self._dead_sources_lock:
            self._dead_sources.pop(source_name, None)

    def coordinate_market_kline(
        self,
        market: str = "",
        timeframe: str = "1D",
        count: int = 120,
        adj: str = "qfq",
        timeout: float = 15.0,
        preferred_source: str = "",
        start_date: str = "",
        end_date: str = "",
        symbols: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        全市场批量K线 — 扁平共享队列 + EWMA 自适应喂料。

        核心设计:
          - 扁平共享队列: 所有 symbols 放一个队列，所有 Provider 的 worker 共同消费
          - EWMA 自适应喂料: 第一轮按静态权重分配，后续按 EWMA 吞吐动态调整 chunk 大小
          - 线程数即并发度: 每个 Provider 开 N 个线程，不需要额外的 dispatcher
          - 限流器: per-Provider 限流器控制请求间隔
          - 死源/熔断: 连续超时标记死源，自动跳过

        与旧版 per-Provider 队列的区别:
          - 去掉 per-Provider Queue + dispatcher 轮询 → 一个共享队列搞定
          - 去掉固定分组 → EWMA 驱动的动态 chunk（快的多吃，慢的少吃）
          - 负载均衡由线程消费速度自然驱动，不需要手动调度
        """
        from app.data_sources.provider import get_providers, NotSupportedResult

        # ── 第一步: 发现支持 kline_batch 的源 ──
        providers = get_providers(capability="kline_batch", timeframe=timeframe, market=market)
        if not providers:
            logger.warning("[协助层] market_kline market=%s tf=%s 无可用源", market, timeframe)
            return {}

        # 过滤熔断 + 死源
        available = []
        for p in providers:
            if not _realtime_cb.is_available(p.name):
                logger.debug("[协助层] market_kline %s 已熔断，跳过", p.name)
                continue
            if self._is_source_dead(p.name):
                logger.debug("[协助层] market_kline %s 已标记为死源，跳过", p.name)
                continue
            available.append(p)

        if not available:
            logger.warning("[协助层] market_kline market=%s tf=%s 无可用源(全部熔断/死亡)", market, timeframe)
            return {}

        # preferred_source 排第一
        if preferred_source:
            preferred = [p for p in available if p.name == preferred_source]
            others = [p for p in available if p.name != preferred_source]
            available = preferred + others

        # prepare() 过滤
        prepared = []
        for p in available:
            try:
                prepare_fn = getattr(p, 'prepare', None)
                if prepare_fn and not prepare_fn():
                    logger.warning("[协助层] market_kline %s prepare() 失败，跳过", p.name)
                    continue
                prepared.append(p)
            except Exception as e:
                logger.warning("[协助层] market_kline %s prepare() 异常: %s，跳过", p.name, e)
        available = prepared
        if not available:
            logger.warning("[协助层] market_kline market=%s tf=%s 无可用源(prepare全部失败)", market, timeframe)
            return {}

        # ── 第二步: 获取股票列表 + 标准化 ──
        if not symbols:
            from app.utils.basicinfo_db import get_stock_basic_db
            symbols = get_stock_basic_db().market_all_codes(status="active")
        if not symbols:
            logger.warning("[协助层] market_kline 获取股票列表失败")
            return {}

        all_codes = _normalize_symbols(symbols, market)
        if not all_codes:
            logger.warning("[协助层] market_kline 标准化后无有效代码")
            return {}

        # count 解析
        if count is None:
            from app.data_sources.provider import calc_kline_count
            from datetime import datetime, timezone, timedelta
            today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
            effective_end = end_date if end_date else today
            effective_start = start_date if start_date else effective_end
            count = calc_kline_count(timeframe, effective_start, effective_end)

        # ── 第三步: 动态分配线程 ──
        thread_alloc = self.allocate_threads(available, symbol_count=len(all_codes))
        logger.info("[协助层] market_kline %d只 → %d源: %s",
                    len(all_codes), len(available),
                    " | ".join(f"{p.name}:{thread_alloc[p.name]}" for p in available))

        # ── 第四步: 构建扁平共享队列 + 共享状态 ──
        queue_lock = threading.Lock()
        shared_queue: deque = deque(all_codes)   # O(1) popleft
        total_initial = len(all_codes)

        result: Dict[str, List[Dict[str, Any]]] = {}
        result_lock = threading.Lock()

        # 失败 symbol 追踪: 记录每个 symbol 被哪些源试过
        # 试过所有源仍失败 → 移入 failed_symbols，不再放回池
        available_names = [p.name for p in available]
        symbol_tried: Dict[str, Set[str]] = {}     # symbol → {源名1, 源名2, ...}
        symbol_tried_lock = threading.Lock()
        failed_symbols: List[str] = []
        failed_lock = threading.Lock()

        source_stats: Dict[str, Dict[str, int]] = {}
        stats_lock = threading.Lock()

        # per-Provider EWMA 吞吐（每秒处理的 symbol 数）— 用于动态 chunk
        provider_throughput: Dict[str, float] = {}
        throughput_lock = threading.Lock()

        # per-Provider 连续超时计数
        provider_consecutive_timeout: Dict[str, int] = {}
        timeout_state_lock = threading.Lock()

        global_stop = threading.Event()

        _PER_TASK_TIMEOUT = 60.0

        # ── 快速失败: per-code 空结果计数 ──
        # 连续被所有 Provider 返回空的 code 直接标记 failed，不再放回队列
        _EMPTY_FAIL_THRESHOLD = len(available)  # 所有源都试过一次就放弃
        symbol_empty_count: Dict[str, int] = {}
        symbol_empty_lock = threading.Lock()

        for p in available:
            provider_consecutive_timeout[p.name] = 0
            with stats_lock:
                source_stats[p.name] = {"ok": 0, "fail": 0, "groups": 0, "timeout": 0}

        def _try_put_back(codes: List[str], source_name: str):
            """
            将失败的 symbols 放回共享池。

            核心逻辑: 记录该源已尝试过此 symbol。
            - 还有未尝试的源 → 放回池
            - 所有源都试过了 → 标记为失败，不再放回

            快速失败: 如果一个 code 连续被多个 Provider 返回空结果
            （即 _is_valid_kline 失败），计数器累加。达到阈值（所有源数）
            直接标记 failed，避免反复循环。
            """
            if not codes:
                return
            to_retry = []
            newly_failed = []
            # 步骤1: 记录尝试，分类出 retry vs failed
            with symbol_tried_lock:
                for c in codes:
                    tried = symbol_tried.setdefault(c, set())
                    tried.add(source_name)
                    if len(tried) < len(available_names):
                        to_retry.append(c)
                    else:
                        newly_failed.append(c)
            # 快速失败: 空结果计数
            with symbol_empty_lock:
                for c in codes:
                    symbol_empty_count[c] = symbol_empty_count.get(c, 0) + 1
                    if symbol_empty_count[c] >= _EMPTY_FAIL_THRESHOLD and c not in newly_failed:
                        newly_failed.append(c)
                        to_retry = [t for t in to_retry if t != c]
            # 步骤2: 标记失败（独立锁，不嵌套）
            if newly_failed:
                with failed_lock:
                    for c in newly_failed:
                        if c not in failed_symbols:
                            failed_symbols.append(c)
            # 步骤3: 过滤已成功 + 放回池（独立锁）
            if to_retry:
                with result_lock:
                    to_retry = [c for c in to_retry if c not in result]
                if to_retry:
                    with queue_lock:
                        shared_queue.extend(to_retry)

        # ── 第五步: 自适应取量函数 ──

        # 首轮静态权重（只算一次，所有 worker 共享）
        _first_round_weights: Dict[str, int] = {}
        _total_weight: int = 0
        for p in available:
            mc = getattr(p, 'max_concurrency', 4)
            bs = getattr(p, 'batch_size', 50)
            _first_round_weights[p.name] = mc * bs
            _total_weight += mc * bs

        def _calc_chunk_size(provider) -> int:
            """
            计算本次该从共享队列取多少只 symbols。

            第一轮（无 EWMA 数据）: 按静态权重分配
              weight = max_concurrency × batch_size（批量能力越强，初始分越多）
            第二轮起: 按 EWMA 吞吐分配
              每个 Provider 按其吞吐占总吞吐的比例 × 剩余 symbols 数

            Returns:
                本次应取的 symbol 数量（≥1）
            """
            name = provider.name
            max_c = getattr(provider, 'max_concurrency', 4)
            batch_sz = getattr(provider, 'batch_size', 50)

            with queue_lock:
                remaining = len(shared_queue)
            if remaining <= 0:
                return 0

            # 单线程模式（非批量 Provider）: 每次取 1 只
            if not getattr(provider, 'fetch_market_kline', None):
                return 1

            with throughput_lock:
                tp = provider_throughput.get(name)

            if tp is None or tp <= 0:
                # 第一轮: 按预计算的静态权重分配
                if _total_weight <= 0:
                    return max(1, min(max_c, remaining))
                share = _first_round_weights.get(name, max_c) / _total_weight
                chunk = max(1, int(share * remaining))
                return min(chunk, max_c, remaining)

            # 第二轮起: 按 EWMA 吞吐分配
            total_tp = 0.0
            with throughput_lock:
                for p in available:
                    total_tp += provider_throughput.get(p.name, 0.0)

            if total_tp <= 0:
                return max(1, min(max_c, remaining))

            share = tp / total_tp
            chunk = max(1, int(share * remaining))
            # 上限: batch_size（单次 API 物理上限）
            return min(chunk, batch_sz, remaining)

        # ── 第六步: Worker 函数 ──

        def _worker(provider):
            """
            扁平化 worker — 从共享队列自适应取量，调 fetch_market_kline。

            流程:
              1. 限流等待
              2. 按 EWMA 吞吐自适应取一批 symbols
              3. 调 fetch_market_kline
              4. 成功 → 合并结果 + 更新吞吐
              5. 失败/超时 → symbols 立即放回共享池（别的源接手）
              6. 连续超时 3 次 → 标记死源，退出
              7. 重复直到队列为空或全局停止
            """
            name = provider.name

            while not global_stop.is_set():
                # 连续超时 3 次 → 标记死源，退出
                with timeout_state_lock:
                    if provider_consecutive_timeout[name] >= 3:
                        if len(available) >= 2:
                            self._mark_source_dead(name)
                        logger.warning("[协助层] market_kline %s 连续超时3次，标记死源退出", name)
                        return

                # 自适应取量
                chunk_size = _calc_chunk_size(provider)
                if chunk_size <= 0:
                    with result_lock:
                        with failed_lock:
                            done = len(result) + len(failed_symbols) >= total_initial
                    if done:
                        return
                    # 队列空但别人还在处理 → 等久一点，减少空转
                    time.sleep(1.0)
                    continue

                with queue_lock:
                    chunk = [shared_queue.popleft() for _ in range(min(chunk_size, len(shared_queue)))]

                if not chunk:
                    time.sleep(0.3)
                    continue

                # 过滤已被其他源完成的
                with result_lock:
                    remaining = [c for c in chunk if c not in result]
                if not remaining:
                    continue

                # ── 单次请求，不重试，失败立即放回共享池 ──
                start = time.time()
                elapsed = 0.0
                put_back = False   # 标记是否已放回池（防重复）
                try:
                    task_result = provider.fetch_market_kline(
                        timeframe=timeframe, count=count,
                        adj=adj, timeout=int(_PER_TASK_TIMEOUT),
                        start_date=start_date, end_date=end_date,
                        symbols=remaining,
                    )
                    elapsed = time.time() - start
                    self._update_ewma(name, elapsed)

                    if isinstance(task_result, NotSupportedResult):
                        with stats_lock:
                            source_stats[name]["fail"] += 1
                        # 不支持的源，不计入 tried（没真正尝试），直接放回
                        with queue_lock:
                            shared_queue.extend(remaining)
                        logger.debug("[协助层] market_kline %s 不支持，退出", name)
                        return

                    if task_result:
                        # 校验每个 symbol 的数据，过滤坏数据
                        valid = {}       # 有效数据
                        bad_codes = []   # 坏数据 → 放回池
                        for code, bars in task_result.items():
                            if _is_valid_kline(bars):
                                valid[code] = bars
                            else:
                                bad_codes.append(code)

                        merged = 0
                        with result_lock:
                            for code, bars in valid.items():
                                if code not in result:
                                    result[code] = bars
                                    merged += 1

                        # 未返回 + 坏数据 → 放回池
                        requested_set = set(remaining)
                        returned_valid = set(valid.keys()) & requested_set
                        not_returned = requested_set - set(task_result.keys())
                        invalid_in_remaining = [c for c in bad_codes if c in requested_set]
                        leftovers = list(not_returned) + invalid_in_remaining
                        # 过滤已被其他源完成的
                        if leftovers:
                            with result_lock:
                                leftovers = [c for c in leftovers if c not in result]
                        if leftovers:
                            # 坏数据 + 未返回: 统一走 _try_put_back 记录尝试
                            # 所有源都试过 → 标记为 failed，不再循环
                            _try_put_back(leftovers, name)
                            logger.debug("[协助层] market_kline %s: %d有效 %d未返回 %d坏数据，%d只放回池",
                                         name, len(returned_valid), len(not_returned),
                                         len(invalid_in_remaining), len(leftovers))
                        # 更新吞吐
                        if elapsed > 0:
                            with throughput_lock:
                                old_tp = provider_throughput.get(name, 0.0)
                                new_tp = merged / elapsed
                                provider_throughput[name] = (
                                    0.3 * new_tp + 0.7 * old_tp if old_tp > 0 else new_tp
                                )
                        with stats_lock:
                            source_stats[name]["ok"] += merged
                            source_stats[name]["groups"] += 1
                        _realtime_cb.record_success(name)
                        self._mark_source_alive(name)
                        with timeout_state_lock:
                            provider_consecutive_timeout[name] = 0
                        logger.debug("[协助层] market_kline %s 完成: %d只 (%.1fs)",
                                     name, merged, elapsed)
                    else:
                        # 空结果 → 放回池（记录该源已尝试）
                        with stats_lock:
                            source_stats[name]["fail"] += 1
                        _realtime_cb.record_failure(name, "empty")
                        _try_put_back(remaining, name)
                        put_back = True
                        logger.debug("[协助层] market_kline %s 空结果，%d只放回池",
                                     name, len(remaining))

                except Exception as e:
                    elapsed = time.time() - start
                    self._update_ewma(name, elapsed)
                    # 超时异常用 timeout 计数，其他用 fail 计数（不双计）
                    if elapsed > _PER_TASK_TIMEOUT:
                        with timeout_state_lock:
                            provider_consecutive_timeout[name] += 1
                        with stats_lock:
                            source_stats[name]["timeout"] += 1
                    else:
                        with stats_lock:
                            source_stats[name]["fail"] += 1
                    _realtime_cb.record_failure(name, str(e))
                    # 异常 → 放回池（记录该源已尝试）
                    with result_lock:
                        still_needed = [c for c in remaining if c not in result]
                    if still_needed:
                        _try_put_back(still_needed, name)
                    put_back = True
                    logger.debug("[Worker] %s 异常: %s，%d只放回池", name, e, len(remaining))

                # 超时检测 → 连续计数
                if elapsed > _PER_TASK_TIMEOUT:
                    with timeout_state_lock:
                        provider_consecutive_timeout[name] += 1
                    with stats_lock:
                        source_stats[name]["timeout"] += 1
                    # 如果上面没放回过（比如正常返回但耗时过长），现在放回
                    if not put_back and remaining:
                        with result_lock:
                            still_needed = [c for c in remaining if c not in result]
                        if still_needed:
                            _try_put_back(still_needed, name)
                    logger.debug("[协助层] market_kline %s 超时(%.1fs)，连续超时 %d 次",
                                 name, elapsed, provider_consecutive_timeout[name])
                elif not put_back:
                    # 非超时且未失败 → 重置连续超时计数
                    with timeout_state_lock:
                        provider_consecutive_timeout[name] = 0

        # ── 第七步: 启动 Worker（临时池，用完即释放，零闲置）──
        total_threads = sum(thread_alloc[p.name] for p in available)

        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=total_threads, thread_name_prefix="mkline"
        )
        futures = []
        for p in available:
            n = thread_alloc[p.name]
            for _ in range(n):
                futures.append(pool.submit(_worker, p))

        # ── 第八步: 等待完成或全局超时 ──
        deadline = time.time() + timeout
        while time.time() < deadline:
            with queue_lock:
                q_empty = len(shared_queue) == 0
            if q_empty:
                time.sleep(1.0)
                with result_lock:
                    with failed_lock:
                        done_count = len(result) + len(failed_symbols)
                if done_count >= total_initial:
                    break
                continue
            time.sleep(0.5)

        # 超时 → 停止所有 worker
        global_stop.set()

        # 等待 worker 收尾（不阻塞在长 fetch 上，最多等 5s）
        concurrent.futures.wait(futures, timeout=5)

        # 关闭池（不等待 — 长 fetch 由 _PER_TASK_TIMEOUT 自行超时退出）
        pool.shutdown(wait=False)

        # ── 第九步: 统计 ──
        stats_lines = []
        with stats_lock:
            for name, st in source_stats.items():
                dead = self._is_source_dead(name)
                status = "💀死" if dead else "✅活"
                with throughput_lock:
                    tp = provider_throughput.get(name, 0.0)
                stats_lines.append(
                    f"{name}: {st['ok']}只成功 {st['fail']}次失败 "
                    f"{st['groups']}组完成 {st['timeout']}次超时 "
                    f"吞吐={tp:.1f}只/s {status}"
                )
        with failed_lock:
            n_failed = len(failed_symbols)
        logger.info("[协助层] market_kline 完成: %d成功 %d失败 %d只/%d只 | %s",
                    len(result), n_failed, len(result) + n_failed, total_initial,
                    " | ".join(stats_lines))
        if n_failed > 0:
            with failed_lock:
                sample = failed_symbols[:10]
            logger.info("[协助层] 失败 symbol 样本(前10): %s", sample)

        return self._normalize_market_kline_result(result)

    # ================================================================
    # 模式 E: 批量行情 — 记忆源优先，直调 Provider
    # ================================================================

    # 记忆源状态（跨调用持久，无 TTL）
    _sticky_source: str = ""
    _sticky_lock = threading.Lock()

    def coordinate_batch_quotes_sticky(
        self,
        symbols: List[str],
        market: str = "",
        timeout: float = 10.0,
        skip_cb_filter: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量行情 — 记忆源优先，直调 Provider.fetch_batch_quotes。

        与 coordinate_batch_quotes 的区别:
          - 不走 Coordinator 的 RACE/分组轮询架构
          - 直接调 Provider 层的 fetch_batch_quotes
          - 自带记忆: 记住当前正常工作的源，下次直接命中
          - 记忆永不失效，直到下次失败才轮换

        调度逻辑:
          1. 有记忆源 → 先试它，成功直接返回
          2. 记忆源失败或无记忆 → 按 priority 轮询所有可用 Provider
          3. 某 Provider 成功 → 记住它，下次直接命中
          4. 全部失败 → 清除记忆，下次重新轮询

        Args:
            symbols: 股票代码列表（纯数字或带前缀均可）
            market:  市场名称（"CNStock" / ...）
            timeout: 单次 fetch 超时（秒）
            skip_cb_filter: True 时跳过熔断器过滤（默认 False）

        Returns:
            {纯数字代码: quote_dict}
        """
        from app.data_sources.provider import get_providers
        from app.data_sources.normalizer import strip_market_prefix

        if not symbols:
            return {}

        # 输入标准化
        prefixed = _normalize_symbols(symbols, market)
        if not prefixed:
            return {}

        # 获取支持 batch_quote 的 Provider（已按 priority 排序）
        providers = get_providers(capability="batch_quote", market=market)
        if not providers:
            logger.warning("[记忆行情] market=%s 无可用 Provider", market)
            return {}

        # 确定尝试顺序: 记忆源排第一
        with Coordinator._sticky_lock:
            sticky = Coordinator._sticky_source

        ordered = []
        if sticky:
            sticky_p = [p for p in providers if p.name == sticky]
            others = [p for p in providers if p.name != sticky]
            ordered = sticky_p + others
        else:
            ordered = list(providers)

        # 熔断过滤
        if not skip_cb_filter:
            ordered = [p for p in ordered if _realtime_cb.is_available(p.name)]
            if not ordered:
                logger.warning("[记忆行情] market=%s 所有源已熔断", market)
                return {}

        # 逐源尝试
        for provider in ordered:
            try:
                result = provider.fetch_batch_quotes(prefixed, timeout=int(timeout))
            except Exception as e:
                logger.debug("[记忆行情] %s 异常: %s", provider.name, e)
                continue

            if not result:
                continue

            # 成功 — 记住这个源
            with Coordinator._sticky_lock:
                Coordinator._sticky_source = provider.name

            if sticky and provider.name != sticky:
                logger.info("[记忆行情] 记忆源 %s 失效，切换到 %s (%d只)",
                            sticky, provider.name, len(result))
            elif not sticky:
                logger.info("[记忆行情] 命中 %s (%d只)", provider.name, len(result))

            # 标准化输出
            return self._normalize_batch_quotes_result(result)

        # 全部失败 — 清除记忆
        with Coordinator._sticky_lock:
            if Coordinator._sticky_source:
                logger.warning("[记忆行情] 记忆源 %s 及所有备选均失败，清除记忆",
                               Coordinator._sticky_source)
                Coordinator._sticky_source = ""

        logger.warning("[记忆行情] 所有 Provider 均失败 (%d只)", len(prefixed))
        return {}

    # ================================================================
    # 透传模式 — 不加任何协调逻辑
    # ================================================================

    @staticmethod
    def direct_call(fn: Callable, *args, **kwargs):
        """
        直接调用 — 不加任何并发/重试/熔断逻辑。

        使用场景: 当调用方已经知道自己要调什么、不需要 Coordinator 的调度能力时。
        例如: CNStockDataSource.get_batch_quotes() 直接调用 Provider 的 fetch_batch_quotes()。

        为什么不直接调 fn？
        保留统一入口，方便以后加日志/监控/限流等横切关注点。
        """
        return fn(*args, **kwargs)

    # ================================================================
    # 内部工具方法
    # ================================================================

    def _get_available_sources(
        self,
        market: str,
        source_map: Dict[str, Callable],
    ) -> List[Tuple[str, SourceConfig]]:
        """
        获取可用源列表（自动发现模式的 fallback）。

        过滤条件:
          1. source_map 中有对应的 fetch_fn（Provider 层注册了该源）
          2. 熔断器未熔断该源

        排序: 按 SourceConfig.effective_weight 降序（权重高的优先）。

        Args:
            market:    市场名称
            source_map: {name: fetch_fn} — Provider 层注册的源

        Returns:
            [(源名称, 源配置), ...] — 按权重降序排列
        """
        if market:
            configs = get_sources_for_market(market)
        else:
            configs = get_all_enabled_sources()

        available = []
        for cfg in configs:
            if cfg.name not in source_map:
                continue
            if not _realtime_cb.is_available(cfg.name):
                logger.debug("[协助层] 源 %s 已熔断，跳过", cfg.name)
                continue
            available.append((cfg.name, cfg))

        return available

    def _get_preferred_available(
        self,
        preferred: str,
        market: str,
        source_map: Dict[str, Callable],
    ) -> List[Tuple[str, SourceConfig]]:
        """
        获取可用源列表，但指定源排在第一位。

        用于 preferred_source 场景: 调用方说"我要用 tencent"，
        这个方法确保 tencent 排在第一个，其他源作为 fallback 排后面。

        如果指定源不可用（未注册或已熔断），回退到默认排序并打 warning。
        """
        all_available = self._get_available_sources(market, source_map)

        if not all_available:
            return []

        preferred_item = None
        others = []
        for item in all_available:
            if item[0] == preferred:
                preferred_item = item
            else:
                others.append(item)

        if preferred_item:
            logger.info("[协助层] 使用指定源 %s (优先), 回退源 %d 个",
                       preferred, len(others))
            return [preferred_item] + others
        else:
            logger.warning("[协助层] 指定源 %s 不可用，回退到默认分配", preferred)
            return all_available


# ================================================================
# 全局单例
# ================================================================

_coordinator = Coordinator()


def get_coordinator() -> Coordinator:
    """
    获取全局 Coordinator 单例。

    整个应用只有一个 Coordinator 实例（线程安全，内部无状态）。
    调用方: CNStockDataSource, DataSourceFactory, routes/*, services/*
    """
    return _coordinator


def Coordinator_direct_call(fn: Callable, *args, **kwargs):
    """
    Coordinator 的直接调用入口 — 不走动态队列/Race/熔断，直接执行 fn。

    命名来源: Coordinator.direct_call() 的模块级快捷方式。
    前缀 "Coordinator_" 标明出处，避免与普通工具函数混淆。

    使用场景:
      - Provider 层的 batch_quote 等接口形状不适合 coordinate_kline / coordinate_ticker 时
      - 调用方已自行处理错误恢复，不需要 Coordinator 的调度保护

    Args:
        fn: 要直接调用的函数
        *args, **kwargs: 透传给 fn 的参数

    Returns:
        fn 的返回值

    示例:
        from app.data_sources.coordinator import Coordinator_direct_call
        result = Coordinator_direct_call(provider.fetch_batch_quotes, symbols)
    """
    return _coordinator.direct_call(fn, *args, **kwargs)
