# -*- coding: utf-8 -*-
"""
数据源配置模块 — 并发控制 + 市场分配 + 吞吐跟踪

模块职责:
    管理每个数据源的并发限制、支持的市场集合、以及实时吞吐统计。
    Coordinator 根据这些配置动态分配任务，最大化利用每个源的并发能力。

设计原理:
    - 声明式配置: 每个源声明自己支持哪些市场、最大并发数、是否支持批量
    - 滑动窗口统计: 使用 deque 实现 O(1) 的滑动窗口，避免全量遍历历史数据
    - 动态权重: effective_weight = throughput × success_rate，权重高的源多干活
    - 线程安全: 所有统计操作使用 threading.Lock 保护

在架构中的位置:
    数据源层 — 被 Coordinator 读取，被 Provider 回写统计

关键依赖:
    - threading.Lock: 线程安全
    - collections.deque: 高效滑动窗口

设计决策 — 为什么用滑动窗口而非全局统计?
    全局统计会受历史数据影响，无法反映源的"当前"健康状态。
    60秒滑动窗口只关注最近的请求表现，能快速感知源的质量变化。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Set, Tuple

# 各 Provider 的 max_workers 与 Provider.MAX_CONCURRENCY 保持一致。
# 每个 Provider 文件定义自己的 MAX_CONCURRENCY 常量（模块级），
# 新增/删除数据源只需增删 Provider 文件 + 本表对应条目。


@dataclass
class SourceConfig:
    """
    单个数据源的并发/市场/吞吐配置。

    属性分为两类:
    1. 静态配置 (name, max_workers, markets, batch_capable, batch_size, enabled)
       — 在启动时确定，运行时一般不变
    2. 动态统计 (_window, _total_* 等)
       — 由 Coordinator 在每次请求后回写，用于动态调度

    滑动窗口算法:
        使用 deque(maxlen=10000) 存储最近的请求记录，每条记录为
        (timestamp, success, elapsed) 三元组。
        _prune_and_calc() 方法在每次查询时修剪过期条目（超过 WINDOW_SECONDS），
        然后在剩余条目上计算 QPS、成功率、平均延迟。
        时间复杂度: O(n) 其中 n 为窗口内条目数（通常 < 1000）

    线程安全性:
        使用 _lock 保护所有统计读写操作

    关键属性:
        name: 源名称（与 Provider.name 对应）
        max_workers: 该源最大并发线程数
        markets: 支持的市场集合 {"CNStock", "HKStock"}
        batch_capable: 是否支持批量请求（fetch_quotes_batch）
        batch_size: 单次批量请求的最大条数
        enabled: 是否启用该源
    """

    name: str                          # 源名称（与 Provider.name 对应）
    max_workers: int = 3               # 该源最大并发线程数
    markets: Set[str] = field(default_factory=set)   # 支持的市场集合
    batch_capable: bool = True         # 是否支持批量请求（fetch_quotes_batch）
    batch_size: int = 500              # 单次批量请求的最大条数
    enabled: bool = True               # 是否启用该源

    # ── 吞吐跟踪（真正的滑动窗口，由 Coordinator 回写）──
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    # 每条记录: (timestamp, success: bool, elapsed: float)
    _window: deque = field(default_factory=lambda: deque(maxlen=10000), repr=False)
    _total_requests: int = 0
    _total_success: int = 0
    _total_time: float = 0.0

    # 滑动窗口大小: 60秒
    # 为什么是60秒？太短会导致统计波动大，太长会延迟感知源质量变化
    WINDOW_SECONDS: float = 60.0

    def record(self, success: bool, elapsed: float):
        """
        记录一次请求的结果（由 Coordinator 调用）。

        将 (时间戳, 是否成功, 耗时) 追加到滑动窗口。

        Args:
            success: 请求是否成功
            elapsed: 请求耗时（秒）
        """
        now = time.time()
        with self._lock:
            self._window.append((now, success, elapsed))
            self._total_requests += 1
            self._total_time += elapsed
            if success:
                self._total_success += 1

    def _prune_and_calc(self) -> Tuple[int, int, float]:
        """
        修剪过期条目，返回窗口内的统计值。

        算法:
            1. 从 deque 左侧（最旧）开始，移除超过 WINDOW_SECONDS 的条目
               由于 deque 有序（按时间追加），遇到第一个未过期的即可停止
            2. 遍历剩余条目，统计请求数、成功数、总耗时

        Returns:
            (requests, success, total_elapsed) 窗口内的统计数据
        """
        cutoff = time.time() - self.WINDOW_SECONDS
        # O(1) 修剪: deque 左侧弹出，直到遇到未过期的条目
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()
        requests = len(self._window)
        success = sum(1 for _, s, _ in self._window if s)
        total_elapsed = sum(e for _, _, e in self._window)
        return requests, success, total_elapsed

    @property
    def throughput(self) -> float:
        """
        最近窗口的实际 QPS（请求/秒）。

        计算方式: 窗口内总请求数 / 窗口内总耗时
        注意: 不是 窗口大小(60s) 的除法，而是实际请求总耗时的除法
        """
        with self._lock:
            reqs, _, elapsed = self._prune_and_calc()
            if reqs == 0 or elapsed <= 0:
                return 0.0
            return reqs / elapsed

    @property
    def success_rate(self) -> float:
        """最近窗口的成功率 (0.0 ~ 1.0)"""
        with self._lock:
            reqs, success, _ = self._prune_and_calc()
            if reqs == 0:
                return 1.0  # 无数据时假设正常
            return success / reqs

    @property
    def avg_latency(self) -> float:
        """最近窗口的平均延迟（秒），仅统计成功的请求"""
        with self._lock:
            _, success, elapsed = self._prune_and_calc()
            if success == 0:
                return 0.0
            return elapsed / success

    def effective_weight(self) -> float:
        """
        有效权重 — 用于 Coordinator 动态分配任务。

        计算公式: effective_weight = throughput × success_rate

        设计意图:
        - throughput 高 → 源处理能力强，多分配任务
        - success_rate 高 → 源稳定可靠，优先使用
        - 两者相乘，综合考虑"快"和"稳"
        - 没有历史数据时，返回 max_workers 作为默认权重

        Returns:
            有效权重值，值越大表示该源越适合承担更多任务
        """
        t = self.throughput
        if t <= 0:
            return float(self.max_workers)
        return t * self.success_rate

    def stats_summary(self) -> str:
        """
        返回简短的统计摘要，用于日志和监控。

        格式: "源名: qps=X ok=X% lat=Xs workers=X"
        """
        return (
            f"{self.name}: qps={self.throughput:.1f} "
            f"ok={self.success_rate:.0%} "
            f"lat={self.avg_latency:.2f}s "
            f"workers={self.max_workers}"
        )


# ================================================================
# 源配置注册表
# ================================================================

SOURCE_CONFIGS: Dict[str, SourceConfig] = {

    # 腾讯实时行情 — 最稳定的免费源之一
    # 支持 A股 + 港股，与 Provider.max_concurrency 保持一致
    "tencent": SourceConfig(
        name="tencent",
        max_workers=6,  # = provider/tencent.py MAX_CONCURRENCY
        markets={"CNStock", "HKStock"},
        batch_capable=True,
        batch_size=500,
    ),

    # 新浪行情 — 传统免费源，仅支持A股
    # 与 Provider.max_concurrency 保持一致
    "sina": SourceConfig(
        name="sina",
        max_workers=4,  # = provider/sina.py MAX_CONCURRENCY
        markets={"CNStock"},
        batch_capable=True,
        batch_size=500,
    ),

    # 东财 datacenter — 数据最全，支持大批次
    # batch_size=6000（东财 API 单次支持大量数据）
    "eastmoney": SourceConfig(
        name="eastmoney",
        max_workers=4,  # = provider/eastmoney.py MAX_CONCURRENCY
        markets={"CNStock"},
        batch_capable=True,
        batch_size=6000,
    ),



    # 同花顺(10jqka) — HTTP 接口，分钟分时+日/周K线
    # 仅支持 A 股
    "10jqka": SourceConfig(
        name="10jqka",
        max_workers=4,  # = provider/10jqka.py MAX_CONCURRENCY
        markets={"CNStock"},
        batch_capable=True,
        batch_size=500,
    ),





    # TwelveData — 国际数据源，支持 A股 + 港股
    "twelvedata": SourceConfig(
        name="twelvedata",
        max_workers=2,  # = provider/twelve_data.py MAX_CONCURRENCY
        markets={"CNStock", "HKStock"},
        batch_capable=False,
        batch_size=1,
    ),

    # ── 新增源（从 akline_market.py 迁移）──

    # 东方财富 trends2 极速源 — 最快的免费A股源
    # 通过 push2.eastmoney.com trends2 API，1min聚合为15min
    "em_trends2": SourceConfig(
        name="em_trends2",
        max_workers=6,  # = provider/em_trends2.py MAX_CONCURRENCY
        markets={"CNStock"},
        batch_capable=True,
        batch_size=50,
    ),

    # 雪球 — 投资社区数据源
    "xueqiu": SourceConfig(
        name="xueqiu",
        max_workers=8,  # = provider/xueqiu.py MAX_CONCURRENCY
        markets={"CNStock"},
        batch_capable=True,
        batch_size=50,
    ),

    # 搜狐财经 — 免费数据源
    "sohu": SourceConfig(
        name="sohu",
        max_workers=8,  # = provider/sohu.py MAX_CONCURRENCY
        markets={"CNStock"},
        batch_capable=True,
        batch_size=50,
    ),

    # 百度股市通 — 免费数据源
    "baidu": SourceConfig(
        name="baidu",
        max_workers=8,  # = provider/baidu.py MAX_CONCURRENCY
        markets={"CNStock"},
        batch_capable=True,
        batch_size=50,
    ),
}


def get_source_config(name: str) -> SourceConfig:
    """
    按名称获取源配置。

    Args:
        name: 数据源名称

    Returns:
        对应的 SourceConfig，不存在时返回默认配置（2个worker，无市场）
    """
    return SOURCE_CONFIGS.get(name, SourceConfig(
        name=name,
        max_workers=2,
        markets=set(),
        batch_capable=False,
    ))


def get_sources_for_market(market: str) -> list[SourceConfig]:
    """
    获取支持指定市场的所有启用源，按 effective_weight 降序排列。

    Coordinator 使用此函数决定任务分配顺序：权重高的源优先使用。

    Args:
        market: 市场名称 ("CNStock" / "HKStock")

    Returns:
        排序后的 SourceConfig 列表
    """
    return sorted(
        [c for c in SOURCE_CONFIGS.values()
         if c.enabled and market in c.markets],
        key=lambda c: c.effective_weight(),
        reverse=True,
    )


def get_all_enabled_sources() -> list[SourceConfig]:
    """获取所有启用的源配置"""
    return [c for c in SOURCE_CONFIGS.values() if c.enabled]
