"""配置加载。所有可调参数来自 YAML，代码里只放默认值。"""
from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, get_type_hints

import yaml


@dataclass
class EndpointsConfig:
    rest_base: str = "https://fapi.binance.com"
    ws_public_base: str = "wss://fstream.binance.com/public"
    ws_market_base: str = "wss://fstream.binance.com/market"


@dataclass
class StreamsConfig:
    depth_speed: str = "100ms"          # depth@100ms
    agg_trade: bool = True
    mark_price_speed: str = "1s"        # markPrice@1s；空字符串表示不订


@dataclass
class BackoffConfig:
    initial_s: float = 1.0
    max_s: float = 60.0
    factor: float = 2.0
    jitter: float = 0.3                 # 抖动比例 0~1


@dataclass
class WsConfig:
    open_timeout_s: float = 20.0
    close_timeout_s: float = 5.0
    idle_timeout_s: float = 30.0        # 整条连接静默超过此值就重连
    max_message_bytes: int = 4 * 1024 * 1024
    rotate_after_s: float = 23 * 3600   # 官方 24h 上限前主动轮换
    rotation_overlap_s: float = 30.0    # 新旧连接重叠时间上限
    unsolicited_pong_s: float = 300.0   # 主动发 pong 的间隔，0 关闭
    backoff: BackoffConfig = field(default_factory=BackoffConfig)
    stream_silence_s: dict[str, float] = field(default_factory=lambda: {
        "depth": 5.0, "aggTrade": 120.0, "markPrice": 5.0,
    })
    inject_close_after_s: float | None = None   # 故障注入(仅测试)：连接建立 N 秒后主动断开一次


@dataclass
class RestConfig:
    timeout_s: float = 10.0
    depth_limit: int = 1000
    weight_budget_per_min: int = 1200   # 官方 2400，本地只用一半
    time_sync_interval_s: float = 60.0
    exchange_info_interval_s: float = 3600.0
    backoff: BackoffConfig = field(default_factory=lambda: BackoffConfig(2.0, 120.0, 2.0, 0.3))
    max_retries: int = 5


@dataclass
class OrderBookConfig:
    export_levels: int = 20
    buffer_max_events: int = 20000
    stale_after_ms: int = 5000
    resync_min_interval_s: float = 1.0


@dataclass
class BackfillConfig:
    enabled: bool = True
    page_limit: int = 1000
    max_pages_per_gap: int = 500
    window_hours: float = 48.0          # 官方限制：最近 2 天（错误码 -4166）
    window_safety_margin_min: float = 30.0
    max_attempts: int = 5
    min_interval_s: float = 0.5


@dataclass
class StorageConfig:
    raw_shard_max_bytes: int = 64 * 1024 * 1024
    raw_shard_max_seconds: float = 900.0
    raw_flush_interval_s: float = 1.0
    raw_fsync_interval_s: float = 5.0
    compress_closed_shards: bool = True
    parquet_batch_rows: int = 5000
    parquet_flush_interval_s: float = 30.0
    parquet_compression: str = "zstd"
    queue_max: int = 200000
    inject_write_error_after_records: int | None = None   # 故障注入(仅测试)：写入 N 条后抛错


@dataclass
class MonitoringConfig:
    status_interval_s: float = 5.0
    disk_warn_free_gb: float = 20.0
    disk_fail_free_gb: float = 5.0
    latency_window: int = 20000
    log_level: str = "INFO"


@dataclass
class CollectorConfig:
    exchange: str = "binance"
    market: str = "usdm_futures"
    symbols: list[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT"])
    data_dir: str = "data/store"
    proxy: str | None = None
    endpoints: EndpointsConfig = field(default_factory=EndpointsConfig)
    streams: StreamsConfig = field(default_factory=StreamsConfig)
    ws: WsConfig = field(default_factory=WsConfig)
    rest: RestConfig = field(default_factory=RestConfig)
    orderbook: OrderBookConfig = field(default_factory=OrderBookConfig)
    backfill: BackfillConfig = field(default_factory=BackfillConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)

    @property
    def data_path(self) -> Path:
        return Path(self.data_dir)

    def depth_stream(self, symbol: str) -> str:
        return f"{symbol.lower()}@depth@{self.streams.depth_speed}"

    def agg_trade_stream(self, symbol: str) -> str:
        return f"{symbol.lower()}@aggTrade"

    def mark_price_stream(self, symbol: str) -> str:
        sp = self.streams.mark_price_speed
        return f"{symbol.lower()}@markPrice" + (f"@{sp}" if sp else "")

    def public_streams(self) -> list[str]:
        return [self.depth_stream(s) for s in self.symbols]

    def market_streams(self) -> list[str]:
        out: list[str] = []
        for s in self.symbols:
            if self.streams.agg_trade:
                out.append(self.agg_trade_stream(s))
            if self.streams.mark_price_speed is not None:
                out.append(self.mark_price_stream(s))
        return out

    def public_url(self) -> str:
        return f"{self.endpoints.ws_public_base}/stream?streams={'/'.join(self.public_streams())}"

    def market_url(self) -> str:
        return f"{self.endpoints.ws_market_base}/stream?streams={'/'.join(self.market_streams())}"


class ConfigError(ValueError):
    pass


def _build(cls: type, data: Any, path: str = "") -> Any:
    if data is None:
        return cls()
    if not isinstance(data, dict):
        raise ConfigError(f"{path or 'root'} 应为映射，实际是 {type(data).__name__}")
    known = {f.name: f for f in fields(cls)}
    hints = get_type_hints(cls)
    unknown = set(data) - set(known)
    if unknown:
        raise ConfigError(f"{path or 'root'} 有未知配置项: {sorted(unknown)}")
    kwargs = {}
    for name, f in known.items():
        if name not in data:
            continue
        v = data[name]
        sub = hints.get(name)
        if isinstance(sub, type) and is_dataclass(sub):
            kwargs[name] = _build(sub, v, f"{path}.{name}" if path else name)
        else:
            kwargs[name] = v
    return cls(**kwargs)


def load_config(path: str | Path | None) -> CollectorConfig:
    if path is None:
        return CollectorConfig()
    p = Path(path)
    with open(p, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    cfg = _build(CollectorConfig, data)
    cfg.symbols = [s.upper() for s in cfg.symbols]
    if not cfg.symbols:
        raise ConfigError("symbols 不能为空")
    if cfg.orderbook.export_levels <= 0:
        raise ConfigError("orderbook.export_levels 必须 > 0")
    if cfg.rest.depth_limit not in (5, 10, 20, 50, 100, 500, 1000):
        raise ConfigError("rest.depth_limit 只能是 5/10/20/50/100/500/1000")
    return cfg


def config_to_dict(cfg: Any) -> Any:
    if is_dataclass(cfg):
        return {f.name: config_to_dict(getattr(cfg, f.name)) for f in fields(cfg)}
    if isinstance(cfg, dict):
        return {k: config_to_dict(v) for k, v in cfg.items()}
    if isinstance(cfg, list):
        return [config_to_dict(v) for v in cfg]
    return cfg
