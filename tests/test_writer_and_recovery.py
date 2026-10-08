import asyncio
import json
import time
from pathlib import Path

import pytest

from data.collector import SCHEMA_VERSION
from data.collector.storage.normalized import NormalizedWriter
from data.collector.storage.raw import RawWriter
from data.collector.writer import WriterThread


def make_writer(tmp_path, maxsize=1000, inject_after=None):
    raw = RawWriter(tmp_path, "s1", SCHEMA_VERSION, 1 << 30, 1e9, 0, 0, compress=False)
    raw.inject_error_after = inject_after
    norm = NormalizedWriter(tmp_path, "s1", 20, SCHEMA_VERSION, 10, 0, "zstd")
    return WriterThread(raw, norm, maxsize)


def raw_op(i):
    return ("raw", "depth", "BTCUSDT", {"recv_time_ns": 1_700_000_000_000_000_000 + i, "id": i, "raw": "{}"}, i)


# 11a. 磁盘写入失败 → failed 状态，不再宣称正常
def test_write_failure_sets_error_state(tmp_path):
    w = make_writer(tmp_path, inject_after=3)
    w.start()
    for i in range(6):
        assert w.put(raw_op(i))
    time.sleep(0.5)
    st = w.status()
    assert st["error"] and "ENOSPC" in st["error"] and st["raw_records_written"] == 3
    w.stop()
    w.join(5)
    assert not w.is_alive()


# 11b. 队列积压可监测、溢出计数
def test_queue_backlog_visible_and_overflow_counted(tmp_path):
    w = make_writer(tmp_path, maxsize=5)
    # 不启动线程：模拟写线程卡住
    ok = [w.put(raw_op(i)) for i in range(8)]
    assert ok.count(True) == 5 and ok.count(False) == 3
    assert w.status()["queue_size"] == 5 and w.status()["overflow_total"] == 3


def test_graceful_stop_drains_queue(tmp_path):
    w = make_writer(tmp_path)
    for i in range(50):
        w.put(raw_op(i))
    w.start()
    w.stop()
    w.join(10)
    assert w.ops_done == 50 and w.raw.records_fsynced == 50
    assert len(list((tmp_path / "raw").rglob("*.manifest.json"))) == 1


# 11c. 进程异常重启：恢复未关闭分片 + downtime gap + 成交水位
def test_restart_recovery_records_downtime_and_resumes_trade_watermark(tmp_path, monkeypatch):
    from data.collector.collector import Collector
    from data.collector.config import CollectorConfig
    from data.collector.quality import QualityRegistry
    from data.collector.trades import TradeProcessor
    from data.collector.storage.checkpoint import Checkpoint

    # 上一次运行留下：一个没 manifest 的分片（尾部损坏）和一个 checkpoint（非 clean）
    shard = tmp_path / "raw/aggTrade/BTCUSDT/20260101/old_000001.jsonl"
    shard.parent.mkdir(parents=True)
    shard.write_text(json.dumps({"recv_time_ns": 5, "id": 100, "raw": "{}"}) + "\n" + '{"recv_time_ns": 6, "id": 101, "ra')
    cp = Checkpoint(tmp_path / "state/checkpoint.json")
    cp.commit({"session_id": "old", "clean_shutdown": False, "trades": {"BTCUSDT": {"last_a": 100, "last_T_ms": 1, "last_recv_ns": 5}}})

    cfg = CollectorConfig(symbols=["BTCUSDT"], data_dir=str(tmp_path))
    c = Collector(cfg)
    c._recover_shards()
    assert c.recovered_shards and c.recovered_shards[0]["records"] == 1 and c.recovered_shards[0]["truncated_bytes"] > 0
    gaps = []
    c.q.on_gap = gaps.append
    c.trades["BTCUSDT"] = TradeProcessor("BTCUSDT", c.q)
    c._recover_previous_run()
    kinds = {(g.stream, g.repair_status) for g in gaps}
    assert ("depth", "unrepairable") in kinds and ("aggTrade", "open") in kinds and ("markPrice", "unrepairable") in kinds
    assert c.trades["BTCUSDT"].downtime_gap is not None
    assert all("did not shut down cleanly" in g.reason for g in gaps)
    assert c.trades["BTCUSDT"].last_a == 100
    # 重启后第一条 ws 成交跳号 → 形成待补缺口
    from tests.test_trades import trade
    c.trades["BTCUSDT"].on_ws(trade(105), 10, 10, 1, "c")
    assert any(g.kind == "aggtrade_id" and g.prev_known_id == 100 and g.next_known_id == 105 for g in gaps)


def test_status_health_levels(tmp_path):
    from data.collector.collector import Collector
    from data.collector.config import CollectorConfig
    cfg = CollectorConfig(symbols=["BTCUSDT"], data_dir=str(tmp_path))
    c = Collector(cfg)
    st = c.build_status()
    assert st["health"]["level"] == "failed"      # 写线程未启动(不 alive) 且未在停止流程
    c.writer.start()
    time.sleep(0.1)
    st = c.build_status()
    assert st["health"]["level"] in ("ok", "degraded")
    c.writer.error = "boom"
    assert c.build_status()["health"]["level"] == "failed"
    c.writer.stop(); c.writer.join(5)
