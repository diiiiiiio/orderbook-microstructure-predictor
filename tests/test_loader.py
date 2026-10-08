"""data.loader：分区扫描、去重、排序、is_valid 过滤、类型转换、summary。"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data import loader as L


def _write(root: Path, table: str, symbol: str, day: str, name: str, rows: list[dict], schema: pa.Schema):
    d = root / "normalized" / table / symbol / day
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), d / f"{name}.parquet")


BOOK_SCHEMA = pa.schema([
    pa.field("symbol", pa.string()), pa.field("book_epoch", pa.int64()), pa.field("u", pa.int64()),
    pa.field("T_ms", pa.int64()), pa.field("recv_seq", pa.int64()), pa.field("is_valid", pa.bool_()),
    pa.field("bid_px_0", pa.string()), pa.field("bid_qty_0", pa.string()),
])


def _book(u, T, seq, valid=True, epoch=1, px="100.5", qty="2"):
    return dict(symbol="BTCUSDT", book_epoch=epoch, u=u, T_ms=T, recv_seq=seq, is_valid=valid,
                bid_px_0=px, bid_qty_0=qty)


@pytest.fixture
def store(tmp_path: Path) -> Path:
    # 第 1 天两个文件（乱序写入 + 重连造成的重复 u），第 2 天一个文件
    _write(tmp_path, "book_top20", "BTCUSDT", "20260925", "s1_1",
           [_book(3, 1030, 3), _book(1, 1010, 1), _book(2, 1020, 2, valid=False)], BOOK_SCHEMA)
    _write(tmp_path, "book_top20", "BTCUSDT", "20260925", "s2_1",
           [_book(3, 1030, 7, px="999"), _book(4, 1040, 8)], BOOK_SCHEMA)        # u=3 重复，晚收到
    _write(tmp_path, "book_top20", "BTCUSDT", "20260926", "s2_2",
           [_book(5, 1050, 9), _book(5, 1050, 9, epoch=2)], BOOK_SCHEMA)          # 不同 epoch 不算重复
    return tmp_path


def test_days_between():
    assert L.days_between("2026-09-25", "2026-09-27") == ["20260925", "20260926", "20260927"]
    assert L.days_between("20260925") == ["20260925"]
    with pytest.raises(ValueError):
        L.days_between("20260926", "20260925")


def test_load_dedup_sort_and_valid_filter(store: Path):
    tbl = L.load("book_top20", "BTCUSDT", "20260925", "20260926", data_dir=store)
    assert tbl["u"].to_pylist() == [1, 3, 4, 5, 5]
    assert tbl["T_ms"].to_pylist() == sorted(tbl["T_ms"].to_pylist())
    assert tbl.filter(pa.compute.equal(tbl["u"], 3))["bid_px_0"].to_pylist() == ["100.5"]   # 保留先收到的
    all_rows = L.load("book_top20", "BTCUSDT", "20260925", data_dir=store, valid_only=False)
    assert all_rows["u"].to_pylist() == [1, 2, 3, 4]


def test_load_columns_subset_and_empty(store: Path):
    tbl = L.load("book_top20", "BTCUSDT", "20260925", data_dir=store, columns=["T_ms", "bid_px_0"])
    assert tbl.column_names == ["T_ms", "bid_px_0"]
    assert tbl.num_rows == 3
    assert L.load("book_top20", "ETHUSDT", "20260925", data_dir=store).num_rows == 0
    assert L.available_days("book_top20", "BTCUSDT", data_dir=store) == ["20260925", "20260926"]
    with pytest.raises(ValueError):
        L.load("nope", "BTCUSDT", "20260925", data_dir=store)


def test_to_float_and_summary(store: Path):
    tbl = L.to_float(L.load("book_top20", "BTCUSDT", "20260925", data_dir=store))
    assert pa.types.is_float64(tbl.schema.field("bid_px_0").type)
    assert pa.types.is_float64(tbl.schema.field("bid_qty_0").type)
    assert pa.types.is_int64(tbl.schema.field("u").type)
    s = L.summary(tbl, "T_ms", gap_ms=15)
    assert s["rows"] == 3 and s["span_s"] == 0.03 and s["max_gap_ms"] == 20 and s["n_gaps_over"] == 1
    assert s["start"] == "1970-01-01T00:00:01.010Z"
    assert L.summary(pa.table({}))["rows"] == 0


def test_gaps_keep_latest_version(tmp_path: Path):
    schema = pa.schema([pa.field("gap_id", pa.string()), pa.field("detected_time_ns", pa.int64()),
                        pa.field("update_time_ns", pa.int64()), pa.field("repair_status", pa.string())])
    _write(tmp_path, "gaps", "BTCUSDT", "20260925", "s1_1",
           [dict(gap_id="g1", detected_time_ns=10, update_time_ns=10, repair_status="open")], schema)
    _write(tmp_path, "gaps", "BTCUSDT", "20260925", "s1_2",
           [dict(gap_id="g1", detected_time_ns=10, update_time_ns=50, repair_status="repaired"),
            dict(gap_id="g0", detected_time_ns=5, update_time_ns=5, repair_status="open")], schema)
    g = L.load("gaps", "BTCUSDT", "20260925", data_dir=tmp_path)
    assert g.to_pylist() == [
        dict(gap_id="g0", detected_time_ns=5, update_time_ns=5, repair_status="open"),
        dict(gap_id="g1", detected_time_ns=10, update_time_ns=50, repair_status="repaired")]


def test_segments():
    tbl = pa.table({"T_ms": [0, 10, 20, 100, 110, 300]})
    assert L.segments(tbl, max_gap_ms=50) == [(0, 3), (3, 5), (5, 6)]
    assert L.segments(tbl, max_gap_ms=1000) == [(0, 6)]
    assert L.segments(pa.table({"T_ms": pa.array([], pa.int64())})) == []
