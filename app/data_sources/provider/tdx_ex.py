# -*- coding: utf-8 -*-
"""
通达信数据源 Provider (pytdx 二进制协议)

API来源 & 最新信息:
  - pytdx开源库: https://github.com/rainx/pytdx
  - 服务器列表: pytdx/config/hosts.py（本文件_CANDIDATE_SERVERS已复制）
  - ExHQ协议端口7727, HQ协议端口7709
  - 自动探测可用服务器，按延迟排序，首次探测约5秒
  - prepare()方法确保有可用服务器
  - category映射: 0=5m, 1=15m, 2=30m, 3=1H, 4=日线, 5=周线, 7=1m, 8=1m(备选)

与 10jqka 的区别:
  - 10jqka:  同花顺HTTP接口 (d.10jqka.com.cn)，无需额外依赖
  - tdx_ex:  pytdx二进制协议，通达信原生接口，需pip install pytdx

支持的功能:
  - K线: ✅ 全周期 1m/5m/15m/30m/1H/1D/1W
  - fetch_ticker: ✅ 单只实时行情（get_security_quotes/get_instrument_quotes）
  - fetch_batch_quotes: ✅ 原生批量（单次请求多只，pytdx协议）

  - 自动重连: 连接断了自动释放并重连

单位注意（重要）:
  - fetch_ticker: pytdx返回的vol单位是"手"，代码中已×100转"股"
  - fetch_batch_quotes: 同上，vol已×100转"股"
  - fetch_kline: pytdx返回的vol单位是"股"（注意:行情和K线的vol单位不同!）
  - 价格字段直接是"元"，不需要÷
  - 复权: 不复权数据通过 TDX 除权除息数据(adjustment模块)转前复权
"""

from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

_TZ_CN = timezone(timedelta(hours=8))

from app.data_sources.provider import register, NotSupportedResult
from app.data_sources.normalizer import normalize_cn_code
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ================================================================
# pytdx 可用性检查
# ================================================================

HAS_TDX = False
HAS_HQ = False
try:
    from pytdx.exhq import TdxExHq_API
    HAS_TDX = True
except ImportError:
    pass

try:
    from pytdx.hq import TdxHq_API
    HAS_HQ = True
except ImportError:
    pass


# ================================================================
# Category 映射 — ExHQ 与 HQ 共用
# ================================================================
# get_instrument_bars / get_security_bars 的 category 含义一致:
#   0=5m, 1=15m, 2=30m, 3=1H, 4=日线, 5=周线, 6=月线, 7=1m, 8=1m(备选), 9=日线(备选)

_TF_CATEGORIES = {
    "1m":  [8, 7],
    "5m":  [0],
    "15m": [1, 8, 9],
    "30m": [2],
    "1H":  [3],
    "1D":  [4, 9],
    "1W":  [5],
}

_SUPPORTED_TF = set(_TF_CATEGORIES.keys())


# ================================================================
# 候选服务器 — ExHQ (7727) + HQ (7709)
# ================================================================

# pytdx 官方 HQ 服务器列表（端口 7709）:
#   https://github.com/rainx/pytdx/blob/master/pytdx/config/hosts.py
# 更新时可从此地址获取最新列表

_CANDIDATE_SERVERS: List[Tuple[str, int, str]] = [
    # === ExHQ 端口 7727 ===
    ("112.74.214.43", 7727, "exhq"),
    ("180.153.18.170", 7727, "exhq"),
    ("180.153.18.171", 7727, "exhq"),
    ("60.191.117.167", 7727, "exhq"),
    ("115.238.56.198", 7727, "exhq"),
    ("115.238.90.165", 7727, "exhq"),
    ("218.75.126.9", 7727, "exhq"),
    ("60.12.136.251", 7727, "exhq"),
    ("60.12.136.250", 7727, "exhq"),
    ("119.147.212.81", 7727, "exhq"),
    ("124.160.88.183", 7727, "exhq"),
    ("101.227.73.20", 7727, "exhq"),
    ("101.227.77.254", 7727, "exhq"),
    ("14.215.128.18", 7727, "exhq"),
    ("59.173.18.140", 7727, "exhq"),
    ("60.28.23.80", 7727, "exhq"),
    ("221.231.141.60", 7727, "exhq"),
    ("113.105.142.162", 7727, "exhq"),
    ("218.108.98.244", 7727, "exhq"),
    ("61.152.107.171", 7727, "exhq"),
    ("61.153.144.66", 7727, "exhq"),
    ("218.108.47.69", 7727, "exhq"),
    ("180.153.39.51", 7727, "exhq"),
    ("118.114.77.13", 7727, "exhq"),
    ("61.135.142.88", 7727, "exhq"),
    ("218.85.139.19", 7727, "exhq"),
    ("202.108.253.130", 7727, "exhq"),
    ("202.108.253.131", 7727, "exhq"),
    # === HQ 端口 7709（pytdx 官方列表）===
    ("218.85.139.19", 7709, "hq"),
    ("218.85.139.20", 7709, "hq"),
    ("58.23.131.163", 7709, "hq"),
    ("218.6.170.47", 7709, "hq"),
    ("123.125.108.14", 7709, "hq"),
    ("180.153.18.170", 7709, "hq"),
    ("180.153.18.171", 7709, "hq"),
    ("180.153.18.172", 7709, "hq"),
    ("202.108.253.130", 7709, "hq"),
    ("202.108.253.131", 7709, "hq"),
    ("202.108.253.139", 7709, "hq"),
    ("60.191.117.167", 7709, "hq"),
    ("115.238.56.198", 7709, "hq"),
    ("218.75.126.9", 7709, "hq"),
    ("115.238.90.165", 7709, "hq"),
    ("124.160.88.183", 7709, "hq"),
    ("60.12.136.250", 7709, "hq"),
    ("218.108.98.244", 7709, "hq"),
    ("218.108.47.69", 7709, "hq"),
    ("223.94.89.115", 7709, "hq"),
    ("218.57.11.101", 7709, "hq"),
    ("58.58.33.123", 7709, "hq"),
    ("14.17.75.71", 7709, "hq"),
    ("114.80.63.12", 7709, "hq"),
    ("114.80.63.35", 7709, "hq"),
    ("180.153.39.51", 7709, "hq"),
    ("119.147.212.81", 7709, "hq"),
    ("221.231.141.60", 7709, "hq"),
    ("101.227.73.20", 7709, "hq"),
    ("101.227.77.254", 7709, "hq"),
    ("14.215.128.18", 7709, "hq"),
    ("59.173.18.140", 7709, "hq"),
    ("60.28.23.80", 7709, "hq"),
    ("218.60.29.136", 7709, "hq"),
    ("122.192.35.44", 7709, "hq"),
    ("112.95.140.74", 7709, "hq"),
    ("112.95.140.92", 7709, "hq"),
    ("112.95.140.93", 7709, "hq"),
    ("114.80.149.19", 7709, "hq"),
    ("114.80.149.21", 7709, "hq"),
    ("114.80.149.22", 7709, "hq"),
    ("114.80.149.91", 7709, "hq"),
    ("114.80.149.92", 7709, "hq"),
    ("121.14.104.60", 7709, "hq"),
    ("121.14.104.66", 7709, "hq"),
    ("123.126.133.13", 7709, "hq"),
    ("123.126.133.14", 7709, "hq"),
    ("123.126.133.21", 7709, "hq"),
    ("211.139.150.61", 7709, "hq"),
    ("59.36.5.11", 7709, "hq"),
    ("119.29.19.242", 7709, "hq"),
    ("123.138.29.107", 7709, "hq"),
    ("123.138.29.108", 7709, "hq"),
    ("124.232.142.29", 7709, "hq"),
    ("183.57.72.11", 7709, "hq"),
    ("183.57.72.12", 7709, "hq"),
    ("183.57.72.13", 7709, "hq"),
    ("183.57.72.15", 7709, "hq"),
    ("183.57.72.21", 7709, "hq"),
    ("183.57.72.22", 7709, "hq"),
    ("183.57.72.23", 7709, "hq"),
    ("183.57.72.24", 7709, "hq"),
    ("183.60.224.177", 7709, "hq"),
    ("183.60.224.178", 7709, "hq"),
    ("113.105.92.100", 7709, "hq"),
    ("113.105.92.101", 7709, "hq"),
    ("113.105.92.102", 7709, "hq"),
    ("113.105.92.103", 7709, "hq"),
    ("113.105.92.104", 7709, "hq"),
    ("113.105.92.99", 7709, "hq"),
    ("117.34.114.13", 7709, "hq"),
    ("117.34.114.14", 7709, "hq"),
    ("117.34.114.15", 7709, "hq"),
    ("117.34.114.16", 7709, "hq"),
    ("117.34.114.17", 7709, "hq"),
    ("117.34.114.18", 7709, "hq"),
    ("117.34.114.20", 7709, "hq"),
    ("117.34.114.27", 7709, "hq"),
    ("117.34.114.30", 7709, "hq"),
    ("117.34.114.31", 7709, "hq"),
    ("182.131.3.252", 7709, "hq"),
    ("183.60.224.11", 7709, "hq"),
    ("58.210.106.91", 7709, "hq"),
    ("58.63.254.216", 7709, "hq"),
    ("58.63.254.219", 7709, "hq"),
    ("58.63.254.247", 7709, "hq"),
    ("123.125.108.90", 7709, "hq"),
    ("175.6.5.153", 7709, "hq"),
    ("182.118.47.151", 7709, "hq"),
    ("182.131.3.245", 7709, "hq"),
    ("202.100.166.27", 7709, "hq"),
    ("222.161.249.156", 7709, "hq"),
    ("42.123.69.62", 7709, "hq"),
    ("58.63.254.191", 7709, "hq"),
    ("58.63.254.217", 7709, "hq"),
]


# ================================================================
# 前复权（共享模块）
# ================================================================
from app.data_sources.provider.adjustment import apply_fwd_adjust as _apply_fwd_adjust


# [并发常量] 最大并发线程数 — Coordinator.allocate_threads() 据此分配 worker。
# ⚠️ 请勿删除或随意修改: 此常量直接影响调度层线程分配，改错会导致请求过载或资源浪费。
# 选值依据: pytdx TCP长连接，受服务器连接数限制。
# 同步位置: source_config.py max_workers 需与此值保持一致。
MAX_CONCURRENCY = 8


@register(priority=22)
class TdxExDataSource:
    """
    通达信数据源 — pytdx 二进制协议（priority=22）。

    与 10jqka provider（d.10jqka.com.cn HTTP）完全独立。
    自动探测 ExHQ (7727) 和 HQ (7709) 双协议服务器。

    能力:
      - K线: 1m/5m/15m/30m/1H/1D/1W
      - 行情: 单只/批量实时行情
      - 全市场批量: 并发获取

    线程安全性:
      - 线程本地连接池
      - 自动探测可用服务器

    依赖:
      - pytdx 未安装时不注册
    """

    name = "tdx_ex"
    priority = 22
    max_concurrency = MAX_CONCURRENCY
    min_interval = 0.0
    jitter_min = 0.0
    jitter_max = 0.0

    capabilities = {
        "kline": True,
        "kline_priority": 22,
        "kline_tf": _SUPPORTED_TF,
        "kline_batch": True,
        "kline_batch_priority": 22,
        "quote": True,
        "quote_priority": 22,
        "batch_quote": True,
        "batch_quote_priority": 22,
        "hk": False,
        "markets": {"CNStock"},
    }

    # ── 类级别共享状态（所有实例共享服务器池 & 连接池）──
    _live_servers: List[Tuple[str, int, str]] = []
    _server_lock = threading.Lock()
    _server_idx = [0]
    _discovered = False
    _discover_lock = threading.Lock()
    _conn_pool = threading.local()

    def __init__(self):
        """启动时探测服务器"""
        self._discover_servers()

    # ================================================================
    # 服务器探测 & 连接管理
    # ================================================================

    @classmethod
    def _discover_servers(cls, force: bool = False):
        """并行探测 ExHQ + HQ 服务器，按延迟排序。force=True 强制重新探测"""
        with cls._discover_lock:
            if cls._discovered and not force:
                return
            cls._discovered = True
            cls._live_servers = []

        results: List[Tuple[str, int, str, float]] = []

        def _probe(host: str, port: int, proto: str):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                t0 = time.time()
                s.connect((host, port))
                latency = time.time() - t0
                s.close()

                # 验证协议握手 + 能拉数据
                if proto == "exhq" and HAS_TDX:
                    try:
                        api = TdxExHq_API()
                        api.connect(host, port, time_out=3)
                        data = None
                        for mkt in [28, 33, 0, 1]:
                            try:
                                data = api.get_instrument_bars(9, mkt, '000001', 0, 1)
                                if data:
                                    break
                            except Exception:
                                continue
                        api.disconnect()
                        if data:
                            results.append((host, port, "exhq", latency))
                            return
                    except Exception:
                        pass

                if proto == "hq" and HAS_HQ:
                    try:
                        api = TdxHq_API()
                        api.connect(host, port, time_out=3)
                        data = api.get_security_bars(9, 1, '600519', 0, 1)
                        api.disconnect()
                        if data:
                            results.append((host, port, "hq", latency))
                            return
                    except Exception:
                        pass
            except Exception:
                pass

        threads = [
            threading.Thread(target=_probe, args=(h, p, proto), daemon=True)
            for h, p, proto in _CANDIDATE_SERVERS
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        results.sort(key=lambda x: x[3])
        cls._live_servers = [(h, p, proto) for h, p, proto, _ in results]

        exhq_count = sum(1 for _, _, p, _ in results if p == "exhq")
        hq_count = sum(1 for _, _, p, _ in results if p == "hq")
        logger.info("[TDX] 服务器探测完成: %d 个可用 (ExHQ=%d, HQ=%d)",
                    len(cls._live_servers), exhq_count, hq_count)

    def prepare(self) -> bool:
        """下载前准备: 确保有可用服务器"""
        if not HAS_TDX and not HAS_HQ:
            return False
        if not self._live_servers:
            self._discover_servers(force=True)
        return bool(self._live_servers)

    @classmethod
    def _get_conn(cls) -> Optional[Tuple[Any, str]]:
        """获取当前线程的连接 (api, proto)，断了自动重连"""
        conn_info = getattr(cls._conn_pool, 'conn_info', None)
        if conn_info:
            api, proto = conn_info
            try:
                if proto == "exhq":
                    api.get_instrument_count(0)
                else:
                    api.get_security_count(0)
                return conn_info
            except Exception:
                try:
                    api.disconnect()
                except Exception:
                    pass
                cls._conn_pool.conn_info = None

        if not cls._live_servers:
            cls._discover_servers(force=True)
        if not cls._live_servers:
            return None

        n = len(cls._live_servers)
        for _ in range(n):
            with cls._server_lock:
                idx = cls._server_idx[0] % n
                cls._server_idx[0] += 1
            host, port, proto = cls._live_servers[idx]
            try:
                if proto == "exhq" and HAS_TDX:
                    api = TdxExHq_API()
                    api.connect(host, port, time_out=3)
                    cls._conn_pool.conn_info = (api, "exhq")
                    return cls._conn_pool.conn_info
                elif proto == "hq" and HAS_HQ:
                    api = TdxHq_API()
                    api.connect(host, port, time_out=3)
                    cls._conn_pool.conn_info = (api, "hq")
                    return cls._conn_pool.conn_info
            except Exception:
                continue
        return None

    @classmethod
    def _release_conn(cls):
        """释放当前线程的连接"""
        conn_info = getattr(cls._conn_pool, 'conn_info', None)
        if conn_info:
            try:
                conn_info[0].disconnect()
            except Exception:
                pass
            cls._conn_pool.conn_info = None

    # ================================================================
    # 核心数据获取
    # ================================================================

    @classmethod
    def _fetch_kline_raw(
        cls,
        code: str,
        timeframe: str = "15m",
        limit: int = 200,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        获取单只股票K线数据（内部），支持 1m/5m/15m/30m/1H/1D/1W。

        ExHQ 和 HQ 双协议自动切换。
        """
        categories = _TF_CATEGORIES.get(timeframe)
        if not categories:
            return None

        nc = normalize_cn_code(code)
        market_sh = nc.startswith("sh")
        symbol = nc[2:]

        conn_info = cls._get_conn()
        if not conn_info:
            return None

        api, proto = conn_info

        # 根据协议选择 market 映射
        if proto == "exhq":
            market = 28 if market_sh else 33
            fetch_fn = lambda cat, mkt, sym, start, count: api.get_instrument_bars(cat, mkt, sym, start, count)
        else:
            market = 1 if market_sh else 0
            fetch_fn = lambda cat, mkt, sym, start, count: api.get_security_bars(cat, mkt, sym, start, count)

        # 尝试多个 category
        data = None
        for cat in categories:
            try:
                data = fetch_fn(cat, market, symbol, 0, limit)
                if data:
                    break
            except Exception:
                # 连接断了，释放后重试
                cls._release_conn()
                conn_info = cls._get_conn()
                if not conn_info:
                    return None
                api, proto = conn_info
                if proto == "exhq":
                    market = 28 if market_sh else 33
                    fetch_fn = lambda cat, mkt, sym, start, count: api.get_instrument_bars(cat, mkt, sym, start, count)
                else:
                    market = 1 if market_sh else 0
                    fetch_fn = lambda cat, mkt, sym, start, count: api.get_security_bars(cat, mkt, sym, start, count)
                continue

        if not data:
            return None

        # 日线/周线只保留日期，分钟线保留完整时间
        _daily_tfs = {"1D", "1W"}
        result = []
        for bar in data:
            dt = str(bar.get("datetime", ""))
            if not dt:
                continue
            try:
                if "-" in dt and ":" in dt:
                    ts = dt[:10] if timeframe in _daily_tfs else dt[:16] + ":00"
                elif len(dt) == 8 and dt.isdigit():
                    ts = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
                else:
                    try:
                        _dt = datetime.fromtimestamp(int(float(dt)))
                        ts = _dt.strftime("%Y-%m-%d") if timeframe in _daily_tfs else _dt.strftime("%Y-%m-%d %H:%M") + ":00"
                    except (ValueError, OSError):
                        continue
                result.append({
                    "time": ts,
                    "open": round(float(bar.get("open", 0)), 4),
                    "high": round(float(bar.get("high", 0)), 4),
                    "low": round(float(bar.get("low", 0)), 4),
                    "close": round(float(bar.get("close", 0)), 4),
                    "volume": round(float(bar.get("vol", 0)), 2),
                })
            except (ValueError, TypeError, KeyError):
                continue

        if not result:
            return None

        result.sort(key=lambda x: x["time"])
        return result[-limit:] if len(result) > limit else result

    @classmethod
    def _fetch_quote_raw(cls, code: str) -> Optional[Dict[str, Any]]:
        """获取单只股票实时行情（内部）"""
        nc = normalize_cn_code(code)
        market_sh = nc.startswith("sh")
        symbol = nc[2:]

        conn_info = cls._get_conn()
        if not conn_info:
            return None

        api, proto = conn_info

        try:
            if proto == "exhq":
                market = 28 if market_sh else 33
                data = api.get_instrument_quotes([(market, symbol)])
            else:
                market = 1 if market_sh else 0
                data = api.get_security_quotes([(market, symbol)])

            if not data or len(data) == 0:
                return None

            q = data[0] if isinstance(data, list) else data
            last = float(q.get("price", 0) or 0)
            if last <= 0:
                return None

            prev = float(q.get("last_close", 0) or 0)
            open_p = float(q.get("open", 0) or last)
            high = float(q.get("high", 0) or last)
            low = float(q.get("low", 0) or last)
            chg = round(last - prev, 4) if prev else 0
            vol = float(q.get("vol", 0) or 0) * 100  # pytdx 返回"手"，需 *100 转"股"

            return {
                "last": last,
                "change": chg,
                "changePercent": round(chg / prev * 100, 2) if prev else 0,
                "high": high,
                "low": low,
                "open": open_p,
                "previousClose": prev,
                "volume": vol,
                "time": "",
                "name": "",
                "symbol": symbol,
            }
        except Exception as e:
            logger.debug("[TDX] fetch_quote %s 失败: %s", code, e)
            cls._release_conn()
            return None

    # pytdx get_security_quotes / get_instrument_quotes 硬限 80 只
    # 超过 80 只不报错，静默截断只返回前 80 只
    _TDX_BATCH_LIMIT = 80

    @classmethod
    def _fetch_batch_quotes_raw(cls, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """批量实时行情（内部），自动分批 + 并行处理"""

        def _fetch_one_batch(batch_codes: List[str]) -> Dict[str, Dict[str, Any]]:
            """单批行情请求"""
            conn_info = cls._get_conn()
            if not conn_info:
                return {}

            api, proto = conn_info

            # 构建请求参数
            pairs: List[Tuple[int, str]] = []
            code_map: Dict[int, str] = {}   # index → 原始输入 code
            for raw_code in batch_codes:
                nc = normalize_cn_code(raw_code)
                market_sh = nc.startswith("sh")
                symbol = nc[2:]
                if proto == "exhq":
                    market = 28 if market_sh else 33
                else:
                    market = 1 if market_sh else 0
                pairs.append((market, symbol))
                code_map[len(pairs) - 1] = raw_code

            result: Dict[str, Dict[str, Any]] = {}

            try:
                if proto == "exhq":
                    data = api.get_instrument_quotes(pairs)
                else:
                    data = api.get_security_quotes(pairs)

                if not data:
                    return {}

                for i, q in enumerate(data):
                    if not isinstance(q, dict):
                        continue
                    raw_code = code_map.get(i)
                    if not raw_code:
                        continue
                    last = float(q.get("price", 0) or 0)
                    if last <= 0:
                        continue
                    nc = normalize_cn_code(raw_code)
                    prev = float(q.get("last_close", 0) or 0)
                    chg = round(last - prev, 4) if prev else 0
                    vol = float(q.get("vol", 0) or 0) * 100  # pytdx 返回"手"，需 *100 转"股"
                    result[raw_code] = {
                        "last": last,
                        "change": chg,
                        "changePercent": round(chg / prev * 100, 2) if prev else 0,
                        "high": float(q.get("high", 0) or last),
                        "low": float(q.get("low", 0) or last),
                        "open": float(q.get("open", 0) or last),
                        "previousClose": prev,
                        "volume": vol,
                        "time": "",
                        "name": "",
                        "symbol": nc[2:],
                    }
            except Exception as e:
                logger.debug("[TDX] fetch_batch_quotes 单批失败: %s", e)
                cls._release_conn()

            return result

        # 分批 + 并行（有效代码已过滤，batch 内不会被毒药代码拖垮）
        limit = cls._TDX_BATCH_LIMIT
        batches = [codes[i:i + limit] for i in range(0, len(codes), limit)]

        if len(batches) <= 1:
            return _fetch_one_batch(batches[0]) if batches else {}

        result: Dict[str, Dict[str, Any]] = {}
        lock = threading.Lock()
        max_workers = min(len(batches), 12)

        def _fetch_and_merge(batch):
            local = _fetch_one_batch(batch)
            if local:
                with lock:
                    result.update(local)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_fetch_and_merge, b) for b in batches]
            for f in futures:
                try:
                    f.result(timeout=15)
                except Exception:
                    pass

        return result

    # ================================================================
    # 对外接口 — BaseDataSource Protocol
    # ================================================================

    def fetch_kline(
        self, code: str, timeframe: str = "15m", count: int = 300,
        adj: str = "", timeout: int = 10,
        start_date: str = "", end_date: str = "",
    ) -> Dict[str, Any]:
        """获取单只股票K线，支持 1m/5m/15m/30m/1H/1D/1W"""
        if timeframe not in _TF_CATEGORIES:
            return NotSupportedResult(self.name, "fetch_kline", f"不支持 {timeframe} 周期")

        if not HAS_TDX and not HAS_HQ:
            return NotSupportedResult(self.name, "fetch_kline", "未安装 pytdx")

        if not self._live_servers:
            return NotSupportedResult(self.name, "fetch_kline", "无可用服务器")

        fetch_count = count
        if start_date:
            from app.data_sources.provider import calc_kline_count
            fetch_count = calc_kline_count(timeframe, start_date, end_date)

        data = self._fetch_kline_raw(code, timeframe, fetch_count)
        if not data:
            return {}

        # 日期过滤
        from app.data_sources.provider import filter_bars_by_date
        if start_date or end_date:
            data = filter_bars_by_date(data, start_date, end_date)

        if adj == "qfq":
            data = _apply_fwd_adjust(data, code)

        return {"bars": data, "count": len(data)} if data else {}

    def fetch_ticker(self, code: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
        """获取单只股票实时行情"""
        if not HAS_TDX and not HAS_HQ:
            return NotSupportedResult(self.name, "fetch_ticker", "未安装 pytdx")
        if not self._live_servers:
            return NotSupportedResult(self.name, "fetch_ticker", "无可用服务器")
        return self._fetch_quote_raw(code)

    def fetch_batch_quotes(self, codes: List[str], timeout: int = 10) -> Dict[str, Dict[str, Any]]:
        """批量实时行情"""
        if not HAS_TDX and not HAS_HQ:
            return NotSupportedResult(self.name, "fetch_batch_quotes", "未安装 pytdx")
        if not self._live_servers:
            return NotSupportedResult(self.name, "fetch_batch_quotes", "无可用服务器")
        return self._fetch_batch_quotes_raw(codes)


# 仅在 pytdx 可用时注册（HAS_TDX/HAS_HQ 检查留给 prepare()）
