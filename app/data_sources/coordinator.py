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

=== 五种调度模式 ===

  模式 A — 单股K线 (coordinate_kline):
    1只股票 × 多个源 → 顺序尝试 → 第一个成功的直接返回
    场景: 单只股票历史K线加载

  模式 B — 单股实时行情 Race (coordinate_ticker):
    1只股票 × 多个源 → 并发抢答 → 第一个返回有效价格的直接用
    场景: 获取单只实时报价

  模式 B2 — 批量实时行情 (coordinate_tickers):
    多只股票 → 直接委托 coordinate_batch_quotes
    场景: 自选股列表价格刷新

  模式 C — 批量行情 (coordinate_batch_quotes):
    长效线程 + 主池/重试池 + 硬超时 + 逐 symbol 失败追踪
    按 500 只一批分组，多源并发消费，部分返回时缺失的 symbol 放回重试池
    场景: 全市场行情快照、大批量实时报价

  模式 D — 全市场批量K线 (coordinate_market_kline):
    长效线程 + 主池/重试池 + 硬超时 + 立即退出
    每个源按 max_concurrency 开线程，线程 cap 到实际任务量，循环取 symbol
    场景: 全市场K线加载

=== 两种源指定方式 ===

  方式 1 — 自动发现（推荐）:
    不传 sources 参数，传 market="CNStock"
    → Coordinator 调用 Provider 层自动发现可用源
    → 好处: 新增/删除 Provider 无需改调用方

  方式 2 — 手动指定（兼容旧代码）:
    传入 sources=[(name, fetch_fn), ...]
    → 好处: 调用方完全控制用哪些源

=== 函数命名说明（容易混淆的）===

  coordinate_kline      → 实际含义: "单股K线，多源顺序尝试"
  coordinate_ticker     → 实际含义: "单股实时行情Race，谁先返回用谁"
  coordinate_tickers    → 实际含义: "批量实时行情，委托 batch_quotes"
  coordinate_batch_quotes → 实际含义: "批量行情，多源并发消费"
  coordinate_market_kline → 实际含义: "全市场批量K线"
  direct_call           → 实际含义: "直接调用，不加任何协调逻辑"
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

    def remaining_cooldown(self, source: str) -> float:
        """返回源的剩余冷却时间（秒），未熔断返回 0"""
        with self._lock:
            if self._state.get(source) != self._OPEN:
                return 0.0
            elapsed = time.time() - self._tripped_at[source]
            return max(0.0, self._cooldown_seconds - elapsed)

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
_realtime_cb = CircuitBreaker(failure_threshold=5, cooldown_seconds=120.0, name="realtime")


def get_realtime_circuit_breaker() -> CircuitBreaker:
    """获取实时行情熔断器实例"""
    return _realtime_cb

# ================================================================
# 全局常量
# ================================================================

# 单次 fetch 的超时上限（秒）。
# Coordinator 层的兜底超时，防止某个源的 fetch_fn 卡死导致整个队列阻塞。
# 比 SourceConfig 里的超时更严格 — 这是硬上限。
PER_TASK_TIMEOUT = 8.0

# 超时辅助线程池 — 长生命周期，避免每次 _fetch_with_timeout 都创建新线程池。
# max_workers=8 足够覆盖并发的超时监控需求（实际 fetch 在主池执行，这里只是等待+取消）。
_timeout_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=8, thread_name_prefix="coord-timeout"
)
atexit.register(_timeout_pool.shutdown, wait=False)

# market_kline 专用硬超时池 — 防止 fetch_kline 卡死阻塞 worker 线程。
# 独立于 _timeout_pool，避免 market_kline 大批量场景挤占 coordinate_kline 的超时监控。
_mkline_timeout_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=12, thread_name_prefix="mkline-timeout"
)
atexit.register(_mkline_timeout_pool.shutdown, wait=False)


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

def _make_provider_fetch_fn(provider) -> Callable:
    """
    K线适配器: 把 Provider.fetch_kline 包装成 Coordinator 能用的 fetch_fn。

    签名转换:
      Provider:  provider.fetch_kline(code, timeframe, count, ) -> Dict | NotSupportedResult
      Coordinator 期望:  fetch_fn(symbol, timeframe, limit) -> Dict | None

    转换规则:
      - NotSupportedResult（布尔值为 False）→ 返回 None → Coordinator 跳过该源
      - 空 dict {} → 返回 None → Coordinator 判定失败，尝试下一个源
      - 非空 dict {"bars": [...], "count": n} → 直接返回 → Coordinator 判定成功
      - 超时异常 → 重新抛出 → Coordinator 捕获 TimeoutError，触发熔断器

    Args:
    """
    def fetch_fn(symbol: str, timeframe: str, limit: int):
        try:
            result = provider.fetch_kline(symbol, timeframe, limit)
            if not result:  # None / {} / NotSupportedResult 都走这里
                return None
            return result
        except Exception as e:
            # 超时/网络异常必须穿透，让 Coordinator 区分"无数据"和"超时"
            # requests.exceptions.Timeout 继承自 ConnectionError(OSError)
            # Python 的 socket.timeout 继承自 TimeoutError(OSError)
            if isinstance(e, (TimeoutError, ConnectionError, OSError)):
                raise
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
            # 超时/网络异常穿透，让 Coordinator 的 Race 逻辑正确处理
            if isinstance(e, (TimeoutError, ConnectionError, OSError)):
                raise
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
        # K线适配器
        adapter = lambda p: _make_provider_fetch_fn(p)

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
# 协助层主类
# ================================================================

class Coordinator:
    """
    协助层 — 并发调度引擎。

    提供五种调度模式:
      - coordinate_kline:          单股K线（多源顺序尝试）
      - coordinate_ticker:         单股实时行情（Race 抢答）
      - coordinate_tickers:        批量实时行情（委托 batch_quotes）
      - coordinate_batch_quotes:   批量行情（多源并发消费）
      - coordinate_market_kline:   全市场批量K线（多源并发）
    """

    def __init__(self):
        self._lock = threading.Lock()

    # ================================================================
    # 外部 prepare 接口 — 提前初始化数据源 cookie 等前置依赖
    # ================================================================

    def prepare(self, market: str = "", providers: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        提前初始化数据源前置依赖（cookie、服务器探测等）。

        由外部调用方在应用启动时主动触发，确保各数据源就绪，
        避免首次请求时因 cookie 获取/服务器探测导致延迟。

        Args:
            market:   市场名称（"CNStock" / "HKStock"），为空时初始化所有已注册源
            providers: 指定要初始化的源名称列表（如 ["xueqiu", "tdx_ex"]），
                       为空时根据 market 过滤

        Returns:
            {源名称: 是否就绪} — 每个源的 prepare() 返回值
        """
        from app.data_sources.provider import get_providers, get_provider

        results: Dict[str, bool] = {}

        if providers:
            # 指定源名称列表
            target_providers = []
            for name in providers:
                p = get_provider(name)
                if p:
                    target_providers.append(p)
                else:
                    results[name] = False
                    logger.warning("[Coordinator.prepare] 源 %s 未注册", name)
        else:
            # 按 market 过滤
            target_providers = get_providers(market=market) if market else get_providers()

        for p in target_providers:
            try:
                ok = p.prepare() if hasattr(p, 'prepare') else True
                results[p.name] = ok
                if not ok:
                    logger.warning("[Coordinator.prepare] %s prepare() 返回 False", p.name)
            except Exception as e:
                results[p.name] = False
                logger.warning("[Coordinator.prepare] %s prepare() 异常: %s", p.name, e)

        ready = sum(1 for v in results.values() if v)
        logger.info("[Coordinator.prepare] 完成: %d/%d 就绪 | %s",
                    ready, len(results),
                    " | ".join(f"{n}={'✓' if v else '✗'}" for n, v in results.items()))
        return results

    # ================================================================
    # 模式 A: 单股K线 — 多源顺序尝试，第一个成功即返回
    # ================================================================

    def coordinate_kline(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        market: str = "",
        timeout: float = 15.0,
        preferred_source: str = "",
        sources: Optional[List[Tuple[str, Callable]]] = None,
        
    ) -> Dict[str, Any]:
        """
        单股K线获取 — 多源顺序尝试，第一个成功即返回。

        典型调用方:
          - CNStockDataSource.get_kline()
          - KlineService（单股分析场景）

        Args:
            symbol:    单只股票代码（如 "600519"）
            timeframe: K 线周期（"1D" / "5m" / "1H" / ...）
            limit:     K 线条数
            market:    市场名称（"CNStock" / "HKStock"），用于自动发现源
            timeout:   总超时（秒）
            preferred_source: 指定首选源（如 "tencent"），优先使用，失败后回退
            sources:   手动指定源列表（可选）。为 None 时自动从 Provider 层发现。
                       格式: [(name, fetch_fn), ...]
                       fetch_fn 签名: fetch_fn(symbol, timeframe, limit) -> List[Dict] | None
            （仅支持不复权）

        Returns:
            Dict — 成功时返回 {"symbol": str, "bars": List[Dict], "source": str}，
                   失败时返回空 dict {}。
        """
        if not symbol:
            return {}

        from app.data_sources.normalizer import add_market_prefix, strip_market_prefix

        # ── 入口标准化: 加市场前缀 ──
        prefixed_symbol = add_market_prefix(symbol, market)
        pure_symbol = strip_market_prefix(prefixed_symbol)

        # ── 获取可用源列表 ──
        if sources is not None:
            source_map = {name: fn for name, fn in sources}
            if preferred_source and preferred_source in source_map:
                available = self._get_preferred_available(
                    preferred_source, market, source_map
                )
            else:
                available = self._get_available_sources(market, source_map)
        else:
            discovered = _discover_sources(market, timeframe, preferred_source)
            if not discovered:
                logger.warning("[协助层] 市场 %s 无可用源", market)
                return {}
            available = [(name, cfg) for name, _, cfg in discovered]
            source_map = {name: fn for name, fn, _ in discovered}

        if not available:
            logger.warning("[协助层] 市场 %s 无可用源", market)
            return {}

        # ── 顺序尝试每个源，第一个成功即返回 ──
        for name, cfg in available:
            fetch_fn = source_map[name]
            start = time.time()
            try:
                future = _timeout_pool.submit(fetch_fn, prefixed_symbol, timeframe, limit)
                result = future.result(timeout=PER_TASK_TIMEOUT)
                elapsed = time.time() - start

                if result:
                    bars = result.get("bars", [])
                    if bars:
                        _realtime_cb.record_success(name)
                        cfg.record(True, elapsed)
                        logger.info("[协助层] kline %s 命中 %s (%d条)", pure_symbol, name, len(bars))
                        return {"symbol": pure_symbol, "bars": bars, "source": name}
                cfg.record(False, elapsed)
                logger.debug("[协助层] kline %s %s 返回空", name, pure_symbol)
            except concurrent.futures.TimeoutError:
                elapsed = time.time() - start
                _realtime_cb.record_failure(name, "timeout")
                cfg.record(False, elapsed)
                future.cancel()
                logger.debug("[协助层] kline %s %s 超时 (%ss)", name, pure_symbol, elapsed)
            except Exception as e:
                elapsed = time.time() - start
                # 熔断只对超时计数
                if elapsed > PER_TASK_TIMEOUT:
                    _realtime_cb.record_failure(name, str(e))
                cfg.record(False, elapsed)
                logger.debug("[协助层] kline %s %s 失败: %s", name, pure_symbol, e)

        # 所有源都失败
        logger.warning("[协助层] kline %s 所有源失败", pure_symbol)
        return {}

    # ================================================================
    # 模式 B: 单股实时行情 — Race 抢答
    # ================================================================

    def coordinate_ticker(
        self,
        symbol: str,
        sources: Optional[List[Tuple[str, Callable]]] = None,
        timeout: float = 8.0,
        preferred_source: str = "",
        market: str = "",
        max_race_sources: int = 3,
    ) -> Dict[str, Any]:
        """
        单股实时行情 — Race 多源并发抢答，第一个返回有效价格的直接用。

        典型调用方:
          - CNStockDataSource.get_ticker()
          - KlineService.get_realtime_price()

        Args:
            symbol:    单只股票代码（如 "600519"）
            sources:   [(name, fetch_fn), ...]。为 None 时自动发现。
                       fetch_fn 签名: fetch_fn(symbol) -> Dict | None
            timeout:   超时（秒）
            preferred_source: 指定首选源。如果可用，优先使用。
            market:    市场名称（"CNStock"），用于自动发现源
            max_race_sources: Race 模式最多几个源抢答

        Returns:
            quote_dict — 成功时返回行情字典，失败时返回空 dict。
        """
        if not symbol:
            return {}

        from app.data_sources.normalizer import add_market_prefix
        prefixed = add_market_prefix(symbol, market)
        return self._ticker_race(
            symbol=prefixed,
            sources=sources,
            timeout=timeout,
            preferred_source=preferred_source,
            market=market,
            max_race_sources=max_race_sources,
        ) or {}

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
                    # 空结果不算熔断
                    cfg = get_source_config(source_name)
                    cfg.record(False, elapsed)
            except Exception as e:
                # 熔断只对超时计数
                if elapsed > PER_TASK_TIMEOUT:
                    _realtime_cb.record_failure(source_name, str(e))
                cfg = get_source_config(source_name)
                cfg.record(False, elapsed)
                logger.debug("[协助层] ticker %s %s 失败: %s", source_name, symbol, e)

        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=len(available), thread_name_prefix="ticker-race"
        )
        try:
            futures = [pool.submit(_race_one, name, fn) for name, fn in available]
            done_event.wait(timeout=timeout)
        finally:
            pool.shutdown(wait=False)

        if result_holder:
            source_name, result = result_holder[0]
            logger.info("[协助层] ticker %s 命中 %s", symbol, source_name)
            return result

        logger.warning("[协助层] ticker %s 所有源失败", symbol)
        return None

    # ================================================================
    # 模式 B2: 批量实时行情 — 委托 coordinate_batch_quotes
    # ================================================================

    def coordinate_tickers(
        self,
        symbols: List[str],
        market: str = "",
        timeout: float = 60.0,
        preferred_source: str = "",
    ) -> List[Dict[str, Any]]:
        """
        批量实时行情 — 直接委托 coordinate_batch_quotes。

        典型调用方:
          - 自选股列表价格刷新
          - PortfolioMonitor 批量监控

        Args:
            symbols: 股票代码列表
            market:  市场名称
            timeout: 超时（秒）
            preferred_source: 指定首选源

        Returns:
            List[Dict] — 行情字典列表，每个 dict 含 symbol 字段
        """
        return self.coordinate_batch_quotes(
            symbols=symbols, market=market, timeout=timeout,
            preferred_source=preferred_source,
        )

    # ================================================================
    # ================================================================
    # 模式 C: 批量行情 — 长效线程 + 主池/重试池 + 硬超时
    # ================================================================

    _BATCH_GROUP_SIZE = 500       # 分组每组 500 只

    def coordinate_batch_quotes(
        self,
        symbols: List[str],
        market: str = "",
        timeout: float = 60.0,
        preferred_source: str = "",
    ) -> List[Dict[str, Any]]:
        """
        批量行情获取 — 长效线程 + 主池/重试池 + 硬超时 + 逐 symbol 失败追踪。

        与 coordinate_market_kline 同款调度模式:
          - 每个源按 max_concurrency 开长效线程，循环取批次
          - 主池: 按 _BATCH_GROUP_SIZE 分组的批次
          - 重试池: 失败/超时的 symbol，标记已试过的源，让其他源接手
          - 硬超时: fetch_batch_quotes 卡死不会阻塞 worker
          - 立即退出: 成功 + 彻底失败 = 总数 → 立即返回

        数据校验（比 kline 更细致）:
          - 空结果 / None → 失败
          - 返回了数据但某个 symbol 缺失 → 缺失的放重试池
          - quote 缺少关键字段（price/close）→ 该条标记无效
          - 所有 quote 都是零值 / 空数据 → 整批失败

        Args:
            symbols: 股票代码列表（纯数字或带前缀均可）
            market:  市场名称（"CNStock" / "HKStock" / ...）
            timeout: 总超时（秒），兜底安全阀
            preferred_source: 指定首选源（如 "tencent"），优先尝试

        Returns:
            List[Dict] — 行情字典列表，每个 dict 含 symbol 字段。失败的 symbol 静默丢弃。
        """
        if not symbols:
            return []

        from collections import deque
        from app.data_sources.provider import get_providers, NotSupportedResult
        from app.data_sources.normalizer import strip_market_prefix

        # ── 入口: dict → list ──
        if isinstance(symbols, dict):
            symbols = list(symbols.keys())

        # 输入标准化
        normalized_symbols = _normalize_symbols(symbols, market)
        if not normalized_symbols:
            return []

        total = len(normalized_symbols)

        # ── 第一步: 发现源 + 过滤 ──
        providers = get_providers(capability="batch_quote", market=market)
        if not providers:
            logger.warning("[batch_quotes] market=%s 无可用源", market)
            return []

        # preferred_source 排序
        if preferred_source:
            preferred = [p for p in providers if p.name == preferred_source]
            others = [p for p in providers if p.name != preferred_source]
            providers = preferred + others

        # 熔断过滤
        available = [p for p in providers if _realtime_cb.is_available(p.name)]
        if not available:
            logger.warning("[batch_quotes] market=%s 所有源已熔断", market)
            return []

        # fetch_batch_quotes 方法检查
        available = [p for p in available if getattr(p, 'fetch_batch_quotes', None)]
        if not available:
            logger.warning("[batch_quotes] market=%s 无可用源(无 fetch_batch_quotes)", market)
            return []

        num_sources = len(available)
        group_size = self._BATCH_GROUP_SIZE  # 500

        # ── 第二步: 确定每个源的线程数 ──
        # 由源 MAX_CONCURRENCY 决定，没有的只开1个线程并警告
        source_threads: Dict[str, int] = {}
        for p in available:
            mc = getattr(p, 'max_concurrency', None)
            if mc is None:
                logger.warning("[batch_quotes] %s 未定义 MAX_CONCURRENCY，默认开1个线程", p.name)
                mc = 1
            source_threads[p.name] = mc

        logger.info("[batch_quotes] %d只 → %d源: %s",
                    total, len(available),
                    " | ".join(f"{p.name}({source_threads[p.name]}线程)" for p in available))

        # ── 第三步: 构建主池 + 重试池 + 共享状态 ──
        # 主池: 每个元素是一批 symbol（最多 group_size 只）
        all_codes = [strip_market_prefix(s) for s in normalized_symbols]
        batches = [all_codes[i:i + group_size] for i in range(0, len(all_codes), group_size)]

        main_pool: deque = deque(batches)
        main_pool_lock = threading.Lock()

        # 重试池: 存单个失败 symbol + 已试源集合
        retry_pool: deque = deque()
        retry_pool_lock = threading.Lock()

        # 结果: {symbol: quote_dict}
        results: Dict[str, Dict[str, Any]] = {}
        results_lock = threading.Lock()

        # 彻底失败
        permanent_fail_count = [0]
        permanent_fail_lock = threading.Lock()
        permanent_fail: Set[str] = set()
        permanent_fail_set_lock = threading.Lock()

        # 完成计数
        done_count = [0]
        done_lock = threading.Lock()

        # 每个源的失败表: source_name → {symbol_set}
        source_fails: Dict[str, Set[str]] = {p.name: set() for p in available}
        source_fails_lock = threading.Lock()

        # 全局停止信号
        stop = threading.Event()

        # 统计
        source_stats: Dict[str, Dict[str, int]] = {
            p.name: {"ok": 0, "fail": 0, "timeout": 0, "batches": 0} for p in available
        }
        stats_lock = threading.Lock()

        _PER_TASK_TIMEOUT = PER_TASK_TIMEOUT

        # ── 第四步: 内部辅助函数 ──

        def _check_done():
            with done_lock:
                if done_count[0] >= total:
                    stop.set()

        def _is_valid_quote(q: Any) -> bool:
            """校验单条 quote 数据是否有效。"""
            if not isinstance(q, dict):
                return False
            # 至少要有 price 或 close 字段且非零
            price = q.get("price") or q.get("close") or q.get("last")
            if price is None:
                return False
            try:
                if float(price) <= 0:
                    return False
            except (ValueError, TypeError):
                return False
            return True

        def _get_batch(source_name: str) -> Optional[List[str]]:
            """
            从主池取一批，或从重试池拼一批。

            优先主池（新批次），主池空了从重试池取"该源没试过的" symbol 凑批。
            """
            # 先从主池取整批
            with main_pool_lock:
                if main_pool:
                    return main_pool.popleft()

            # 主池空了，从重试池凑一批
            my_fails = source_fails[source_name]
            batch = []
            with retry_pool_lock:
                deferred = []
                while retry_pool and len(batch) < group_size:
                    sym = retry_pool.popleft()
                    with permanent_fail_set_lock:
                        if sym in permanent_fail:
                            continue
                    if sym in my_fails:
                        deferred.append(sym)
                    else:
                        batch.append(sym)
                for item in deferred:
                    retry_pool.append(item)

            return batch if batch else None

        def _return_to_retry(sym: str, source_name: str, is_invalid: bool = False):
            """
            将单个 symbol 放回重试池，标记源失败。

            Args:
                is_invalid: True = 源返回了明确的错误代码（空dict等），
                            不放重试池，直接记彻底失败。
            """
            # 明确的错误代码 → 不重试，直接彻底失败
            if is_invalid:
                with permanent_fail_set_lock:
                    permanent_fail.add(sym)
                with permanent_fail_lock:
                    permanent_fail_count[0] += 1
                with done_lock:
                    done_count[0] += 1
                _check_done()
                return

            # 正常失败流程：加锁 → 标记 → 检查是否超过1/2活源试过
            with source_fails_lock:
                source_fails[source_name].add(sym)
                fail_count = sum(1 for src in available if sym in source_fails[src.name])

            # 超过1/2活源（未熔断）试过就彻底失败
            active_count = sum(1 for src in available if _realtime_cb.is_available(src.name))
            if active_count > 0 and fail_count * 2 > active_count:
                with permanent_fail_set_lock:
                    permanent_fail.add(sym)
                with permanent_fail_lock:
                    permanent_fail_count[0] += 1
                with done_lock:
                    done_count[0] += 1
                _check_done()
                return

            with retry_pool_lock:
                retry_pool.append(sym)

        def _mark_success(sym: str, quote: Dict[str, Any], source_name: str) -> bool:
            """标记成功。首次成功计入 done_count，后回的丢弃。"""
            with results_lock:
                if sym in results:
                    return False
                # 数据校验
                if not _is_valid_quote(quote):
                    return False
                # 清洗: 去重 name 字段
                rn = quote.get("name", "")
                if rn and strip_market_prefix(rn) == sym:
                    quote["name"] = ""
                quote["symbol"] = sym
                results[sym] = quote

            with done_lock:
                done_count[0] += 1
            _check_done()
            return True

        def _process_batch(source_name: str, provider, batch: List[str]) -> bool:
            """
            处理一批 symbol。

            流程:
              1. 硬超时调用 fetch_batch_quotes
              2. 逐 symbol 校验 + 标记成功
              3. 缺失 / 无效的 symbol → 放回重试池
              4. 整批无数据 → 全部放回重试池

            Returns:
                True = 至少有一个 symbol 成功
            """
            # 已全部完成则跳过
            with done_lock:
                if done_count[0] >= total:
                    return False

            start = time.time()
            try:
                # ── 硬超时包装 ──
                _hard_timeout = _PER_TASK_TIMEOUT + 2
                _fetch_future = _mkline_timeout_pool.submit(
                    provider.fetch_batch_quotes,
                    batch, timeout=int(_PER_TASK_TIMEOUT),
                )
                try:
                    task_result = _fetch_future.result(timeout=_hard_timeout)
                except concurrent.futures.TimeoutError:
                    elapsed = time.time() - start
                    _fetch_future.cancel()
                    # 硬超时 → 熔断计数
                    _realtime_cb.record_failure(source_name, "hard_timeout")
                    cfg = get_source_config(source_name)
                    cfg.record(False, elapsed)
                    with stats_lock:
                        source_stats[source_name]["timeout"] += 1
                    # 整批放回重试池
                    for sym in batch:
                        _return_to_retry(sym, source_name)
                    return False

                elapsed = time.time() - start

                # NotSupported → 不算熔断失败，整批放回重试池
                if isinstance(task_result, NotSupportedResult):
                    with stats_lock:
                        source_stats[source_name]["fail"] += 1
                    for sym in batch:
                        _return_to_retry(sym, source_name)
                    return False

                # 空 dict → 源返回明确的错误代码（不是"没数据"，是"这些代码无效"）
                # 不放重试池，直接记彻底失败
                if isinstance(task_result, dict) and len(task_result) == 0:
                    cfg = get_source_config(source_name)
                    cfg.record(False, elapsed)
                    with stats_lock:
                        source_stats[source_name]["fail"] += 1
                    for sym in batch:
                        _return_to_retry(sym, source_name, is_invalid=True)
                    return False

                # 有返回数据 → 逐 symbol 处理
                if task_result:
                    _realtime_cb.record_success(source_name)
                    cfg = get_source_config(source_name)
                    cfg.record(True, elapsed)

                    returned = set()
                    ok_count = 0
                    for psym, quote in task_result.items():
                        d = strip_market_prefix(psym)
                        if d in returned:
                            continue
                        returned.add(d)
                        # 已被其他源完成，跳过
                        with results_lock:
                            if d in results:
                                continue
                        is_first = _mark_success(d, quote, source_name)
                        if is_first:
                            ok_count += 1
                            with stats_lock:
                                source_stats[source_name]["ok"] += 1

                    # 缺失的 symbol → 放回重试池
                    missing = [s for s in batch if strip_market_prefix(s) not in returned]
                    for sym in missing:
                        _return_to_retry(sym, source_name)

                    with stats_lock:
                        source_stats[source_name]["batches"] += 1
                    return ok_count > 0

                # 空结果 → 整批失败
                cfg = get_source_config(source_name)
                cfg.record(False, elapsed)
                with stats_lock:
                    source_stats[source_name]["fail"] += 1
                for sym in batch:
                    _return_to_retry(sym, source_name)
                return False

            except Exception as e:
                elapsed = time.time() - start
                is_timeout = elapsed > _PER_TASK_TIMEOUT
                # 熔断只对超时计数，错误代码和没数据的不进行计数
                if is_timeout:
                    _realtime_cb.record_failure(source_name, str(e))
                cfg = get_source_config(source_name)
                cfg.record(False, elapsed)
                with stats_lock:
                    if is_timeout:
                        source_stats[source_name]["timeout"] += 1
                    else:
                        source_stats[source_name]["fail"] += 1
                for sym in batch:
                    _return_to_retry(sym, source_name)
                return False

        def _worker(provider):
            """长效线程主循环 — 不断取批次，处理，直到 stop。"""
            name = provider.name
            _cb_warned = False
            while not stop.is_set():
                # 源被熔断 → 暂停等待，不抢任务
                if not _realtime_cb.is_available(name):
                    if not _cb_warned:
                        remaining = _realtime_cb.remaining_cooldown(name)
                        logger.warning("[batch_quotes] %s 已熔断，等待冷却恢复 (剩余 %.0fs)", name, remaining)
                        _cb_warned = True
                    if stop.wait(timeout=5.0):
                        break
                    continue
                if _cb_warned:
                    logger.info("[batch_quotes] %s 冷却结束，恢复工作", name)
                    _cb_warned = False
                batch = _get_batch(name)
                if batch is None:
                    if stop.wait(timeout=3.0):
                        break
                    continue
                _process_batch(name, provider, batch)

        # ── 第五步: 启动线程池 ──
        total_threads = sum(source_threads[p.name] for p in available)

        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=total_threads, thread_name_prefix="bquote"
        )
        all_futures = []
        for provider in available:
            n = source_threads[provider.name]
            for _ in range(n):
                all_futures.append(pool.submit(_worker, provider))

        # 等待: done_count == total 立即返回，否则等到 timeout
        stop.wait(timeout=timeout)
        stop.set()
        pool.shutdown(wait=False)

        # ── 第六步: 收集未处理 → 彻底失败 ──
        with main_pool_lock:
            while main_pool:
                batch = main_pool.popleft()
                for sym in batch:
                    with results_lock:
                        if sym not in results:
                            with permanent_fail_lock:
                                permanent_fail_count[0] += 1

        with retry_pool_lock:
            while retry_pool:
                sym = retry_pool.popleft()
                with results_lock:
                    if sym not in results:
                        with permanent_fail_lock:
                            permanent_fail_count[0] += 1

        # ── 第七步: 统计 ──
        stat_parts = []
        for name, s in source_stats.items():
            if s["ok"] > 0 or s["fail"] > 0 or s["timeout"] > 0:
                stat_parts.append(f"{name}: {s['ok']}只/{s['batches']}批 {s['fail']}失败 {s['timeout']}超时")
        logger.info("[batch_quotes] 完成: %d成功 %d彻底失败 %d只 | %s",
                    len(results), permanent_fail_count[0], total,
                    " | ".join(stat_parts))

        return list(results.values())

    # ================================================================
    # ================================================================
    # 模式 D: 全市场批量K线 — 长效线程 + 主池/重试池
    # ================================================================

    def coordinate_market_kline(
        self,
        market: str = "",
        timeframe: str = "1D",
        count: int = 500,
        
        timeout: float = 60.0,
        preferred_source: str = "",
        start_date: str = "",
        end_date: str = "",
        symbols: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        全市场批量K线 — 长效线程 + 主池/重试池 + 立即退出。

        核心设计:
          1. 每个源按 max_concurrency 开线程（cap 到实际任务量，避免小批量浪费），每个线程是长效的，循环取任务
          2. 主池: 所有待拉取的 symbol，线程各自从池中取
          3. 重试池: 失败/超时的 symbol 放入重试池，标记已试过的源
             - 空闲线程从重试池取"自己没试过的" symbol 继续拉
             - 重试池无锁共享: 哪个线程先拿到有效数据，就把 symbol 从重试池删掉
          4. 彻底失败: 重试池中某 symbol 所有源都试过了 → 删除并记彻底失败+1
          5. 立即退出: 成功数 + 彻底失败数 = 总数 → 立即返回
          6. 数据有效性: 以数据有效且谁先回的为准，后回的直接丢弃
          7. 硬超时: fetch_kline 卡死不会阻塞 worker，超时后 abandon 底层线程，走失败→重试→熔断流程

        线程生命周期:
          - 长效: 不因单次失败退出，持续循环取下一个任务
          - 硬超时保护: 单次 fetch 通过 _mkline_timeout_pool 包装，超时后不再等待，线程自然回收
          - 退出条件: stop event 被 set（done+failed=total 或超时兜底）

        Args:
            market: 市场名称（"CNStock" / "HKStock" / ...）
            timeframe: K线周期（"1D" / "5m" / ...）
            count: 每只股票的K线条数
            timeout: 总超时（秒），兜底安全阀
            preferred_source: 指定首选源
            start_date: 起始日期
            end_date: 结束日期
            symbols: 股票代码列表，为 None 时自动获取全市场

        Returns:
            List[Dict] — K线数据列表，每个 dict 含 symbol 字段表示所属股票。
        """
        from collections import deque
        from app.data_sources.provider import get_providers, NotSupportedResult
        from app.data_sources.normalizer import strip_market_prefix

        # ── 第一步: 发现源 + 过滤 ──
        providers = get_providers(capability="kline_batch", timeframe=timeframe, market=market)
        if not providers:
            logger.warning("[market_kline] market=%s tf=%s 无可用源", market, timeframe)
            return []

        # 熔断过滤
        available = [p for p in providers if _realtime_cb.is_available(p.name)]
        if not available:
            logger.warning("[market_kline] market=%s tf=%s 无可用源(全部熔断)", market, timeframe)
            return []

        # preferred_source 排第一
        if preferred_source:
            preferred = [p for p in available if p.name == preferred_source]
            others = [p for p in available if p.name != preferred_source]
            available = preferred + others

        # prepare() 过滤
        prepared = []
        for p in available:
            try:
                if getattr(p, 'prepare', None) and not p.prepare():
                    logger.warning("[market_kline] %s prepare() 失败，跳过", p.name)
                    continue
                prepared.append(p)
            except Exception as e:
                logger.warning("[market_kline] %s prepare() 异常: %s，跳过", p.name, e)
        available = prepared

        # 过滤掉没有 fetch_kline 方法的源
        available = [p for p in available if getattr(p, 'fetch_kline', None)]

        # ── 入口: dict → list ──
        if isinstance(symbols, dict):
            symbols = list(symbols.keys())

        # ── 第二步: 获取股票列表 + 标准化 ──
        if not symbols:
            from app.utils.basicinfo_db import get_stock_basic_db
            symbols = get_stock_basic_db().market_all_codes(status="active")
        if not symbols:
            logger.warning("[market_kline] 获取股票列表失败")
            return []

        all_codes = _normalize_symbols(symbols, market)
        if not all_codes:
            logger.warning("[market_kline] 标准化后无有效代码")
            return []

        total = len(all_codes)

        # count 解析
        if count is None:
            from app.data_sources.provider import calc_kline_count
            from datetime import datetime, timezone, timedelta
            today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
            effective_end = end_date if end_date else today
            effective_start = start_date if start_date else effective_end
            count = calc_kline_count(timeframe, effective_start, effective_end)

        # ── 第三步: 确定每个源的线程数 ──
        # 规则: 有 max_concurrency → 用它；没有 → 1 + 警告
        # cap: 不超过实际需要的线程数，避免 1 只股票开 40 个线程的浪费
        num_sources = len(available)
        raw_threads: Dict[str, int] = {}
        for p in available:
            mc = getattr(p, 'max_concurrency', None)
            if mc is None:
                logger.warning("[market_kline] %s 未定义 MAX_CONCURRENCY，默认开1个线程", p.name)
                mc = 1
            raw_threads[p.name] = mc

        raw_total = sum(raw_threads.values())
        # cap 到 max(total, num_sources)：每个源至少1线程，但不超总任务数
        effective_total = min(raw_total, max(total, num_sources))
        source_threads: Dict[str, int] = {}
        if raw_total > effective_total:
            for p in available:
                source_threads[p.name] = max(1, round(raw_threads[p.name] * effective_total / raw_total))
            diff = effective_total - sum(source_threads.values())
            for p in available:
                if diff <= 0:
                    break
                source_threads[p.name] += 1
                diff -= 1
        else:
            source_threads = raw_threads

        logger.info("[market_kline] %d只 → %d源: %s",
                    total, num_sources,
                    " | ".join(f"{p.name}({source_threads[p.name]}线程)" for p in available))

        # ── 第四步: 构建主池 + 重试池 + 共享状态 ──

        # 主池: 待拉取的 symbol
        # 规则: 代码数 < 源数 → 主池为空，全部放重试池（让所有源都有机会试）
        main_pool: deque = deque() if total < num_sources else deque(all_codes)
        main_pool_lock = threading.Lock()

        # 重试池: 仅存 symbol，失败记录在 source_fails 中
        retry_pool: deque = deque(all_codes) if total < num_sources else deque()
        retry_pool_lock = threading.Lock()

        # 结果: 谁先回有效数据算谁的，后回的丢弃
        results: Dict[str, List[Dict[str, Any]]] = {}
        results_lock = threading.Lock()

        # 彻底失败计数: 所有源都试过仍失败
        permanent_fail_count = [0]
        permanent_fail_lock = threading.Lock()

        # 完成计数: 成功 + 彻底失败，用于判断退出
        done_count = [0]
        done_lock = threading.Lock()

        # 每个源的失败表: source_name → {symbol_1, symbol_2, ...}
        # 同一源的多个线程共享同一张失败表（不是按线程建表）
        source_fails: Dict[str, Set[str]] = {p.name: set() for p in available}
        source_fails_lock = threading.Lock()

        # 彻底失败集合: 已被2个源试过失败的 symbol（用于从重试池中过滤）
        permanent_fail: Set[str] = set()
        permanent_fail_set_lock = threading.Lock()

        # 全局停止信号: done_count == total 时 set，通知所有线程退出
        stop = threading.Event()

        # 统计
        source_stats: Dict[str, Dict[str, int]] = {
            p.name: {"ok": 0, "fail": 0, "timeout": 0} for p in available
        }
        stats_lock = threading.Lock()

        _PER_TASK_TIMEOUT = PER_TASK_TIMEOUT  # 使用全局常量（60s硬上限）

        # ── 第五步: 内部辅助函数 ──

        def _check_done():
            """检查是否所有 symbol 都有了结果（成功或彻底失败），是则 set stop"""
            with done_lock:
                if done_count[0] >= total:
                    stop.set()

        def _get_symbol(source_name: str) -> Optional[str]:
            """
            从主池或重试池取一个 symbol。

            优先主池（新 symbol），主池空了再看重试池（失败过的 symbol）。
            重试池中只取"该源失败表中没有的" symbol。
            都没有 → 返回 None。
            """
            # 先从主池取
            with main_pool_lock:
                if main_pool:
                    return main_pool.popleft()

            # 主池空了，从重试池取"该源失败表中没有的"
            my_fails = source_fails[source_name]
            with retry_pool_lock:
                deferred = []
                found = None
                while retry_pool:
                    sym = retry_pool.popleft()
                    # 已彻底失败的 symbol 直接丢弃
                    with permanent_fail_set_lock:
                        if sym in permanent_fail:
                            continue
                    if sym in my_fails:
                        deferred.append(sym)
                    else:
                        found = sym
                        break
                # 放回跳过的
                for item in deferred:
                    retry_pool.append(item)
                return found

        def _return_to_retry(sym: str, source_name: str, is_invalid: bool = False):
            """
            将 symbol 放入重试池（失败/超时后归还）。

            记录到该源的失败表。检查是否超过1/2活源试过此 symbol → 是则彻底失败。

            Args:
                is_invalid: True = 源返回了明确的错误代码，不放重试池，直接彻底失败。
            """
            # 明确的错误代码 → 不重试，直接彻底失败
            if is_invalid:
                with permanent_fail_set_lock:
                    permanent_fail.add(sym)
                with permanent_fail_lock:
                    permanent_fail_count[0] += 1
                with done_lock:
                    done_count[0] += 1
                _check_done()
                return

            # 正常失败流程：加锁 → 标记 → 检查是否超过1/2活源试过
            with source_fails_lock:
                source_fails[source_name].add(sym)
                fail_count = sum(1 for src in available if sym in source_fails[src.name])

            # 超过1/2活源（未熔断）试过就彻底失败
            active_count = sum(1 for src in available if _realtime_cb.is_available(src.name))
            if active_count > 0 and fail_count * 2 > active_count:
                with permanent_fail_set_lock:
                    permanent_fail.add(sym)
                with permanent_fail_lock:
                    permanent_fail_count[0] += 1
                with done_lock:
                    done_count[0] += 1
                _check_done()
                return

            # 还有源没试 → 放入重试池
            with retry_pool_lock:
                retry_pool.append(sym)

        def _mark_success(sym: str, bars: List[Dict[str, Any]], source_name: str) -> bool:
            """
            标记成功。第一个返回有效数据的算数，后回的丢弃。

            Returns:
                True = 首次成功，计入 done_count
                False = 已被其他源抢先，丢弃
            """
            with results_lock:
                if sym in results:
                    return False  # 已被其他源抢先，丢弃
                results[sym] = bars

            with done_lock:
                done_count[0] += 1
            _check_done()
            return True

        def _try_fetch(source_name: str, provider, sym: str) -> bool:
            """
            尝试用指定源拉取一个 symbol 的K线。

            流程:
              1. 检查是否已被其他源完成（跳过）
              2. 硬超时调用 fetch_kline(code=sym)（卡死不会阻塞 worker）
              3. 校验数据有效性 (_is_valid_kline)
              4. 有效 → _mark_success（首次成功才计入，后回丢弃）
              5. 硬超时/无效/空/异常 → _return_to_retry（标记源失败，放回重试池）

            Returns:
                True = 成功或已由其他源完成
                False = 失败（已归还重试池或标记彻底失败）
            """
            # 已被其他源完成，直接跳过
            with results_lock:
                if sym in results:
                    return True

            start = time.time()
            try:
                # ── 硬超时包装 ──
                # 直接调用 fetch_kline 如果 socket 卡死，线程会永远阻塞。
                # 用 _mkline_timeout_pool 在独立线程中执行，超时后 abandon 该线程，
                # worker 线程不再等待，立即走失败→重试→熔断流程。
                _hard_timeout = _PER_TASK_TIMEOUT + 2  # 比 provider 自身 timeout 稍长
                _fetch_future = _mkline_timeout_pool.submit(
                    provider.fetch_kline,
                    code=strip_market_prefix(sym), timeframe=timeframe, count=count,
                    timeout=int(_PER_TASK_TIMEOUT),
                    start_date=start_date, end_date=end_date,
                )
                try:
                    task_result = _fetch_future.result(timeout=_hard_timeout)
                except concurrent.futures.TimeoutError:
                    # 硬超时触发 — abandon 底层线程（它会自己超时回来）
                    elapsed = time.time() - start
                    _fetch_future.cancel()
                    _realtime_cb.record_failure(source_name, "hard_timeout")
                    cfg = get_source_config(source_name)
                    cfg.record(False, elapsed)
                    with stats_lock:
                        source_stats[source_name]["timeout"] += 1
                    _return_to_retry(sym, source_name)
                    return False
                elapsed = time.time() - start

                # NotSupported → 该源不支持，不算熔断失败，只放回重试池
                if isinstance(task_result, NotSupportedResult):
                    with stats_lock:
                        source_stats[source_name]["fail"] += 1
                    _return_to_retry(sym, source_name)
                    return False

                # 有返回数据 → 校验有效性
                if task_result:
                    bars = task_result.get("bars", [])
                    if bars and _is_valid_kline(bars):
                        # 有效数据 → 尝试标记成功
                        _realtime_cb.record_success(source_name)
                        cfg = get_source_config(source_name)
                        cfg.record(True, elapsed)
                        is_first = _mark_success(sym, bars, source_name)
                        if is_first:
                            with stats_lock:
                                source_stats[source_name]["ok"] += 1
                        return True

                # 空结果或无效数据 → 不算熔断失败，只放回重试池
                cfg = get_source_config(source_name)
                cfg.record(False, elapsed)
                with stats_lock:
                    source_stats[source_name]["fail"] += 1
                _return_to_retry(sym, source_name)
                return False

            except Exception as e:
                elapsed = time.time() - start
                is_timeout = elapsed > _PER_TASK_TIMEOUT
                # 熔断只对超时计数，错误代码和没数据的不进行计数
                if is_timeout:
                    _realtime_cb.record_failure(source_name, str(e))
                cfg = get_source_config(source_name)
                cfg.record(False, elapsed)
                with stats_lock:
                    if is_timeout:
                        source_stats[source_name]["timeout"] += 1
                    else:
                        source_stats[source_name]["fail"] += 1
                _return_to_retry(sym, source_name)
                return False

        def _worker(provider):
            """
            长效线程主循环 — 不断从主池/重试池取 symbol，拉数据，直到 stop。

            线程不因单次失败退出，持续循环取下一个任务。
            超时后自动将 symbol 归还重试池，标记源失败，继续下一个。
            池子空了等待通知，有新 symbol（重试池被放回）时继续工作。
            源被熔断时暂停等待，不抢任务。
            """
            name = provider.name
            _cb_warned = False
            while not stop.is_set():
                # 源被熔断 → 暂停等待，不抢任务
                if not _realtime_cb.is_available(name):
                    if not _cb_warned:
                        remaining = _realtime_cb.remaining_cooldown(name)
                        logger.warning("[market_kline] %s 已熔断，等待冷却恢复 (剩余 %.0fs)", name, remaining)
                        _cb_warned = True
                    if stop.wait(timeout=5.0):
                        break
                    continue
                if _cb_warned:
                    logger.info("[market_kline] %s 冷却结束，恢复工作", name)
                    _cb_warned = False
                sym = _get_symbol(name)
                if sym is None:
                    # 池子空了，等待: 可能有新 symbol 被放回重试池，或 stop 信号
                    # 不能退出 — 其他 worker 可能正在处理 symbol 并即将放回重试池
                    # 只靠 stop 信号退出
                    if stop.wait(timeout=3.0):
                        break
                    continue
                _try_fetch(name, provider, sym)

        # ── 第六步: 启动线程池 ──
        total_threads = sum(source_threads[p.name] for p in available)

        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=total_threads, thread_name_prefix="mkline"
        )
        all_futures = []
        for provider in available:
            n = source_threads[provider.name]
            for _ in range(n):
                all_futures.append(pool.submit(_worker, provider))

        # 等待: done_count == total 立即返回，否则等到 timeout
        stop.wait(timeout=timeout)
        stop.set()  # 确保所有线程收到退出信号
        pool.shutdown(wait=False)

        # ── 第七步: 收集未处理的 symbol → 彻底失败 ──
        # 主池剩余
        with main_pool_lock:
            while main_pool:
                sym = main_pool.popleft()
                with results_lock:
                    if sym not in results:
                        with permanent_fail_lock:
                            permanent_fail_count[0] += 1

        # 重试池剩余
        with retry_pool_lock:
            while retry_pool:
                sym = retry_pool.popleft()
                with results_lock:
                    if sym not in results:
                        with permanent_fail_lock:
                            permanent_fail_count[0] += 1

        # ── 第八步: 统计 ──
        stats_lines = []
        for name, s in source_stats.items():
            if s["ok"] > 0 or s["fail"] > 0 or s["timeout"] > 0:
                stats_lines.append(f"{name}: {s['ok']}成功 {s['fail']}失败 {s['timeout']}超时")
        logger.info("[market_kline] 完成: %d成功 %d彻底失败 %d只 | %s",
                    len(results), permanent_fail_count[0], total,
                    " | ".join(stats_lines))

        # 转为 List[dict]，每条 bar 加 symbol 字段
        from app.data_sources.normalizer import strip_market_prefix
        output: List[Dict[str, Any]] = []
        for sym, bars in results.items():
            pure = strip_market_prefix(sym)
            for bar in bars:
                bar["symbol"] = pure
                output.append(bar)
        return output

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
