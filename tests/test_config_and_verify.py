import json
from pathlib import Path

import pytest

from data.collector.config import ConfigError, load_config, CollectorConfig


def test_example_config_loads():
    cfg = load_config(Path(__file__).resolve().parent.parent / "deploy/config/collector.example.yaml")
    assert cfg.symbols == ["BTCUSDT", "ETHUSDT"]
    assert cfg.public_url().startswith("wss://fstream.binance.com/public/stream?streams=btcusdt@depth@100ms")
    assert "btcusdt@aggTrade" in cfg.market_url() and "btcusdt@markPrice@1s" in cfg.market_url()
    assert cfg.ws.rotate_after_s < 24 * 3600


def test_unknown_key_rejected(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("symbols: [BTCUSDT]\nws:\n  bogus: 1\n")
    with pytest.raises(ConfigError):
        load_config(p)


def test_invalid_depth_limit_rejected(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("rest:\n  depth_limit: 123\n")
    with pytest.raises(ConfigError):
        load_config(p)


def test_verify_day_detects_duplicates_and_tamper(tmp_path):
    import pyarrow as pa, pyarrow.parquet as pq
    from data.collector.verify import verify_day
    from data.collector.storage.raw import RawWriter
    w = RawWriter(tmp_path, "s", 1, 1 << 30, 1e9, 0, 0, compress=False)
    w.write("depth", "BTCUSDT", {"recv_time_ns": 1_767_225_600_000_000_000, "raw": "{}"}, 1)
    w.close_all()
    d = tmp_path / "normalized/agg_trades/BTCUSDT/20260101"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"symbol": ["BTCUSDT", "BTCUSDT"], "a": [1, 1]}), d / "x.parquet")
    r = verify_day(tmp_path, "20260101")
    assert r["raw"][0]["ok"] and r["normalized"]["agg_trades"]["duplicate_keys"] == 1 and not r["ok"]
