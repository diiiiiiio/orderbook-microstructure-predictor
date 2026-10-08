import gzip
import json
import os
from pathlib import Path

from data.collector.storage.raw import (RawWriter, recover_open_shard, verify_manifest,
                                        manifest_path, iter_jsonl)
from data.collector.storage.checkpoint import Checkpoint


def rec(i: int, ns: int = 1_700_000_000_000_000_000) -> dict:
    return {"recv_time_ns": ns + i * 1_000_000, "id": 1000 + i, "raw": "{\"x\":%d}" % i}


def test_raw_rotate_compress_manifest_and_verify(tmp_path: Path):
    w = RawWriter(tmp_path, "s1", 1, max_bytes=200, max_seconds=1e9, flush_interval_s=0,
                  fsync_interval_s=0, compress=True)
    for i in range(20):
        w.write("depth", "BTCUSDT", rec(i), id_value=1000 + i)
    w.maybe_flush(force=True)
    w.close_all()
    files = sorted((tmp_path / "raw/depth/BTCUSDT").rglob("*.jsonl.gz"))
    assert len(files) >= 2
    total = 0
    for f in files:
        v = verify_manifest(f)
        assert v["ok"], v
        m = json.loads(manifest_path(f).read_text())
        assert m["first_id"] is not None and m["last_id"] >= m["first_id"]
        assert m["sha256"] and m["records"] == v["records"]
        total += v["records"]
    assert total == 20
    ids = [r["id"] for f in files for r in iter_jsonl(f)]
    assert ids == list(range(1000, 1020))


def test_tamper_detected(tmp_path: Path):
    w = RawWriter(tmp_path, "s1", 1, 1 << 30, 1e9, 0, 0, compress=False)
    w.write("depth", "BTCUSDT", rec(0))
    w.close_all()
    f = next((tmp_path / "raw").rglob("*.jsonl"))
    with open(f, "a") as fh:
        fh.write('{"forged":1}\n')
    v = verify_manifest(f)
    assert not v["ok"] and not v["sha256_ok"] and not v["records_ok"]


def test_recover_truncated_tail_preserves_good_lines_and_isolates_tail(tmp_path: Path):
    p = tmp_path / "raw/depth/BTCUSDT/20260101/s0_000001.jsonl"
    p.parent.mkdir(parents=True)
    good = [json.dumps(rec(i)) + "\n" for i in range(5)]
    with open(p, "w") as fh:
        fh.writelines(good)
        fh.write('{"recv_time_ns": 1, "id": 99, "raw": "unfinished')   # 崩溃截断
    r = recover_open_shard(p)
    assert r["records"] == 5 and r["truncated_bytes"] > 0
    assert Path(r["corrupt_tail"]).read_text().startswith('{"recv_time_ns": 1')
    assert [x["id"] for x in iter_jsonl(p)] == [1000, 1001, 1002, 1003, 1004]
    assert verify_manifest(p)["ok"]
    m = json.loads(manifest_path(p).read_text())
    assert m["recovered"] is True and m["first_id"] == 1000 and m["last_id"] == 1004


def test_recover_clean_file_no_truncation(tmp_path: Path):
    p = tmp_path / "a.jsonl"
    p.write_text("".join(json.dumps(rec(i)) + "\n" for i in range(3)))
    r = recover_open_shard(p)
    assert r["records"] == 3 and r["truncated_bytes"] == 0 and "corrupt_tail" not in r


def test_fsynced_positions_only_after_fsync(tmp_path: Path):
    w = RawWriter(tmp_path, "s1", 1, 1 << 30, 1e9, flush_interval_s=1e9, fsync_interval_s=1e9, compress=False)
    w.write("trade", "ETHUSDT", rec(0), id_value=7)
    assert w.fsynced_positions() == {}
    w.maybe_flush(force=True)
    pos = w.fsynced_positions()["trade/ETHUSDT"]
    assert pos["last_id"] == 7 and pos["records_fsynced_in_shard"] == 1
    w.write("trade", "ETHUSDT", rec(1), id_value=8)
    assert w.fsynced_positions()["trade/ETHUSDT"]["last_id"] == 7      # 8 尚未 fsync，水位不前进
    w.close_all()
    assert w.fsynced_positions()["trade/ETHUSDT"]["last_id"] == 8


def test_write_failure_raises(tmp_path: Path):
    w = RawWriter(tmp_path, "s1", 1, 1 << 30, 1e9, 0, 0, compress=False)
    w.write("depth", "BTCUSDT", rec(0))
    shard = next(iter(w.shards.values()))
    shard.fh.close()          # 模拟句柄失效/磁盘错误
    import pytest
    with pytest.raises(ValueError):
        w.write("depth", "BTCUSDT", rec(1))


def test_checkpoint_atomic_and_corrupt_recovery(tmp_path: Path):
    cp = Checkpoint(tmp_path / "state/checkpoint.json")
    cp.commit({"a": 1})
    cp2 = Checkpoint(tmp_path / "state/checkpoint.json")
    assert cp2.get("a") == 1 and cp2.get("committed_time_ns")
    (tmp_path / "state/checkpoint.json").write_text("{broken")
    cp3 = Checkpoint(tmp_path / "state/checkpoint.json")
    assert cp3.data == {}
    assert any(p.name.startswith("checkpoint.corrupt") for p in (tmp_path / "state").iterdir())


def test_normalized_writer_roundtrip(tmp_path: Path):
    import pyarrow.parquet as pq
    from data.collector.storage.normalized import NormalizedWriter
    from data.collector.records import BookRow, GapRecord, QualityEvent
    nw = NormalizedWriter(tmp_path, "s1", levels=2, schema_version=1, batch_rows=100,
                          flush_interval_s=1e9, compression="zstd")
    row = BookRow("BTCUSDT", 1, 10, 9, 8, 1700000000000, 1700000000000, 1_700_000_000_000_000_000, 5, 1, "c1",
                  ["100.00"], ["1.000"], ["100.10", "100.20"], ["2.000", "3.000"], 1, 2, True, "LIVE", ["x"],
                  available_time_ns=1_700_000_000_000_000_100)
    nw.add_book_row(row)
    nw.add_trade({"symbol": "BTCUSDT", "a": 1, "p": "1", "q": "2", "nq": "2", "f": 1, "l": 1, "T_ms": 1700000000000,
                  "E_ms": None, "m": True, "aggressor_side": "sell", "st": 1, "source": "rest_backfill",
                  "recv_time_ns": None, "known_time_ns": 1_700_000_000_000_000_000, "recv_seq": None,
                  "connection_id": None, "extra_json": "{}"})
    nw.add_gap(GapRecord("g1", "BTCUSDT", "depth", "depth_sequence", "certain", "pu_mismatch",
                         1_700_000_000_000_000_000, 1, 2, 3, 4, "recv_time_ns", 8, 20, "open", "s1"))
    nw.add_quality(QualityEvent(1_700_000_000_000_000_000, "BTCUSDT", "depth", "state_change", "info", "s1",
                                detail={"from": "SYNCING", "to": "LIVE"}))
    nw.close()
    b = pq.read_table(tmp_path / "normalized/book_top20").to_pylist()
    assert b[0]["bid_px_0"] == "100.00" and b[0]["bid_px_1"] is None and b[0]["ask_qty_1"] == "3.000"
    t = pq.read_table(tmp_path / "normalized/agg_trades").to_pylist()
    assert t[0]["recv_time_ns"] is None and t[0]["source"] == "rest_backfill"
    g = pq.read_table(tmp_path / "normalized/gaps").to_pylist()
    assert g[0]["repair_status"] == "open"
    q = pq.read_table(tmp_path / "normalized/quality_events").to_pylist()
    assert json.loads(q[0]["detail"])["to"] == "LIVE"


def test_fsynced_watermark_persists_across_shard_rotation(tmp_path: Path):
    """checkpoint 用的水位：fsync 后可见、关闭分片后仍保留、只反映已 fsync 的 id。"""
    w = RawWriter(tmp_path, "s1", 1, max_bytes=150, max_seconds=1e9, flush_interval_s=1e9, fsync_interval_s=1e9, compress=False)
    w.write("aggTrade", "BTCUSDT", rec(0), id_value=100)
    assert w.fsynced_positions() == {}
    w.maybe_flush(force=True)
    assert w.fsynced_positions()["aggTrade/BTCUSDT"]["last_id"] == 100
    w.write("aggTrade", "BTCUSDT", rec(1), id_value=101)          # 未 fsync
    assert w.fsynced_positions()["aggTrade/BTCUSDT"]["last_id"] == 100
    for i in range(2, 8):                                          # 触发分片滚动（关闭即 fsync）
        w.write("aggTrade", "BTCUSDT", rec(i), id_value=100 + i)
    pos = w.fsynced_positions()["aggTrade/BTCUSDT"]
    assert pos["last_id"] >= 101
    w.close_all()
    assert w.fsynced_positions()["aggTrade/BTCUSDT"]["last_id"] == 107
