"""Normalized 研究层：Parquet。

  <data_dir>/normalized/<table>/<SYMBOL>/<YYYYMMDD>/<session>_<seq>.parquet
表：book_top20 / agg_trades / mark_price / gaps / quality_events。
写入以批为单位，每批一个独立文件（崩溃只丢当前批，不损坏旧文件）。
价格/数量存字符串（无损）；研究时再按需转 Decimal/float。
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from data.collector.records import BookRow, GapRecord, QualityEvent

log = logging.getLogger("collector.normalized")


def _levels_fields(n: int) -> list[pa.Field]:
    out = []
    for side in ("bid", "ask"):
        for i in range(n):
            out.append(pa.field(f"{side}_px_{i}", pa.string()))
        for i in range(n):
            out.append(pa.field(f"{side}_qty_{i}", pa.string()))
    return out


def book_schema(levels: int) -> pa.Schema:
    return pa.schema([
        pa.field("symbol", pa.string()),
        pa.field("book_epoch", pa.int64()),
        pa.field("u", pa.int64()), pa.field("U", pa.int64()), pa.field("pu", pa.int64()),
        pa.field("E_ms", pa.int64()), pa.field("T_ms", pa.int64()),
        pa.field("recv_time_ns", pa.int64()), pa.field("recv_monotonic_ns", pa.int64()),
        pa.field("available_time_ns", pa.int64()),
        pa.field("recv_seq", pa.int64()), pa.field("connection_id", pa.string()),
        pa.field("session_id", pa.string()),
        pa.field("n_bid_levels_known", pa.int32()), pa.field("n_ask_levels_known", pa.int32()),
        pa.field("is_valid", pa.bool_()), pa.field("state", pa.string()),
        pa.field("flags", pa.string()),
        pa.field("schema_version", pa.int32()),
        *_levels_fields(levels),
    ])


AGG_TRADE_SCHEMA = pa.schema([
    pa.field("symbol", pa.string()),
    pa.field("a", pa.int64()),
    pa.field("p", pa.string()), pa.field("q", pa.string()), pa.field("nq", pa.string()),
    pa.field("f", pa.int64()), pa.field("l", pa.int64()),
    pa.field("T_ms", pa.int64()), pa.field("E_ms", pa.int64()),
    pa.field("m", pa.bool_()),
    pa.field("aggressor_side", pa.string()),      # buy / sell，由 m 推导
    pa.field("st", pa.int64()),
    pa.field("source", pa.string()),              # ws / rest_backfill
    pa.field("recv_time_ns", pa.int64()),         # ws 实时接收时间；backfill 为 null
    pa.field("known_time_ns", pa.int64()),        # 本机最早获知时间（ws=recv, backfill=响应接收）
    pa.field("recv_seq", pa.int64()), pa.field("connection_id", pa.string()),
    pa.field("session_id", pa.string()),
    pa.field("extra_json", pa.string()),
    pa.field("schema_version", pa.int32()),
])

MARK_PRICE_SCHEMA = pa.schema([
    pa.field("symbol", pa.string()),
    pa.field("E_ms", pa.int64()),
    pa.field("p", pa.string()), pa.field("ap", pa.string()), pa.field("i", pa.string()),
    pa.field("P", pa.string()), pa.field("r", pa.string()),
    pa.field("T_next_funding_ms", pa.int64()),
    pa.field("st", pa.int64()),
    pa.field("recv_time_ns", pa.int64()), pa.field("recv_monotonic_ns", pa.int64()),
    pa.field("recv_seq", pa.int64()), pa.field("connection_id", pa.string()),
    pa.field("session_id", pa.string()),
    pa.field("extra_json", pa.string()),
    pa.field("schema_version", pa.int32()),
])

GAP_SCHEMA = pa.schema([
    pa.field("gap_id", pa.string()), pa.field("symbol", pa.string()), pa.field("stream", pa.string()),
    pa.field("kind", pa.string()), pa.field("certainty", pa.string()), pa.field("reason", pa.string()),
    pa.field("detected_time_ns", pa.int64()),
    pa.field("start_time_ns", pa.int64()), pa.field("end_time_ns", pa.int64()),
    pa.field("start_exchange_time_ms", pa.int64()), pa.field("end_exchange_time_ms", pa.int64()),
    pa.field("time_basis", pa.string()),
    pa.field("prev_known_id", pa.int64()), pa.field("next_known_id", pa.int64()),
    pa.field("repair_status", pa.string()), pa.field("session_id", pa.string()),
    pa.field("connection_id", pa.string()),
    pa.field("book_epoch_before", pa.int64()), pa.field("book_epoch_after", pa.int64()),
    pa.field("note", pa.string()), pa.field("update_time_ns", pa.int64()),
])

QUALITY_SCHEMA = pa.schema([
    pa.field("time_ns", pa.int64()), pa.field("symbol", pa.string()), pa.field("stream", pa.string()),
    pa.field("event_type", pa.string()), pa.field("severity", pa.string()),
    pa.field("session_id", pa.string()), pa.field("connection_id", pa.string()),
    pa.field("detail", pa.string()),
])


def book_row_to_dict(row: BookRow, levels: int, session_id: str, schema_version: int) -> dict[str, Any]:
    d: dict[str, Any] = {
        "symbol": row.symbol, "book_epoch": row.book_epoch, "u": row.u, "U": row.U, "pu": row.pu,
        "E_ms": row.E, "T_ms": row.T, "recv_time_ns": row.recv_time_ns,
        "recv_monotonic_ns": row.recv_monotonic_ns, "available_time_ns": row.available_time_ns,
        "recv_seq": row.recv_seq, "connection_id": row.connection_id, "session_id": session_id,
        "n_bid_levels_known": row.n_bid_levels_known, "n_ask_levels_known": row.n_ask_levels_known,
        "is_valid": row.is_valid, "state": row.state, "flags": ",".join(row.flags),
        "schema_version": schema_version,
    }
    for side, pxs, qtys in (("bid", row.bid_px, row.bid_qty), ("ask", row.ask_px, row.ask_qty)):
        for i in range(levels):
            d[f"{side}_px_{i}"] = pxs[i] if i < len(pxs) else None
            d[f"{side}_qty_{i}"] = qtys[i] if i < len(qtys) else None
    return d


def utc_day_ns(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y%m%d")


class ParquetTable:
    def __init__(self, root: Path, table: str, schema: pa.Schema, session_id: str,
                 batch_rows: int, flush_interval_s: float, compression: str, day_key: str):
        self.root = root / table
        self.table = table
        self.schema = schema
        self.session_id = session_id
        self.batch_rows = batch_rows
        self.flush_interval_s = flush_interval_s
        self.compression = compression
        self.day_key = day_key
        self._buf: dict[str, list[dict[str, Any]]] = {}
        self._seq = 0
        self._last_flush = time.monotonic()
        self.rows_written = 0
        self.files_written = 0
        self.last_write_error: str | None = None

    def add(self, symbol: str, row: dict[str, Any]) -> None:
        self._buf.setdefault(symbol, []).append(row)
        if len(self._buf[symbol]) >= self.batch_rows:
            self._flush_symbol(symbol)

    def maybe_flush(self, force: bool = False) -> None:
        if force or time.monotonic() - self._last_flush >= self.flush_interval_s:
            for sym in list(self._buf):
                self._flush_symbol(sym)
            self._last_flush = time.monotonic()

    def _flush_symbol(self, symbol: str) -> None:
        rows = self._buf.pop(symbol, None)
        if not rows:
            return
        by_day: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            by_day.setdefault(utc_day_ns(r[self.day_key]), []).append(r)
        for day, part in by_day.items():
            tbl = pa.Table.from_pylist(part, schema=self.schema)
            d = self.root / symbol / day
            d.mkdir(parents=True, exist_ok=True)
            self._seq += 1
            path = d / f"{self.session_id}_{self._seq:06d}.parquet"
            tmp = path.with_suffix(".parquet.tmp")
            pq.write_table(tbl, tmp, compression=self.compression)
            with open(tmp, "rb") as fh:
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            self.rows_written += tbl.num_rows
            self.files_written += 1

    def pending(self) -> int:
        return sum(len(v) for v in self._buf.values())

    def close(self) -> None:
        self.maybe_flush(force=True)


class NormalizedWriter:
    def __init__(self, data_dir: Path, session_id: str, levels: int, schema_version: int,
                 batch_rows: int, flush_interval_s: float, compression: str):
        root = data_dir / "normalized"
        self.levels = levels
        self.session_id = session_id
        self.schema_version = schema_version
        common = dict(session_id=session_id, batch_rows=batch_rows,
                      flush_interval_s=flush_interval_s, compression=compression)
        self.book = ParquetTable(root, "book_top20", book_schema(levels), day_key="recv_time_ns", **common)
        self.trades = ParquetTable(root, "agg_trades", AGG_TRADE_SCHEMA, day_key="known_time_ns", **common)
        self.mark = ParquetTable(root, "mark_price", MARK_PRICE_SCHEMA, day_key="recv_time_ns", **common)
        self.gaps = ParquetTable(root, "gaps", GAP_SCHEMA, day_key="detected_time_ns",
                                 **dict(common, batch_rows=1, flush_interval_s=0))
        self.quality = ParquetTable(root, "quality_events", QUALITY_SCHEMA, day_key="time_ns",
                                    **dict(common, batch_rows=200, flush_interval_s=min(flush_interval_s, 10)))
        self.tables = [self.book, self.trades, self.mark, self.gaps, self.quality]

    def add_book_row(self, row: BookRow) -> None:
        self.book.add(row.symbol, book_row_to_dict(row, self.levels, self.session_id, self.schema_version))

    def add_trade(self, row: dict[str, Any]) -> None:
        row.setdefault("session_id", self.session_id)
        row.setdefault("schema_version", self.schema_version)
        self.trades.add(row["symbol"], row)

    def add_mark(self, row: dict[str, Any]) -> None:
        row.setdefault("session_id", self.session_id)
        row.setdefault("schema_version", self.schema_version)
        self.mark.add(row["symbol"], row)

    def add_gap(self, gap: GapRecord) -> None:
        self.gaps.add(gap.symbol, gap.to_dict())

    def add_quality(self, ev: QualityEvent) -> None:
        self.quality.add(ev.symbol or "_", ev.to_dict())

    def maybe_flush(self, force: bool = False) -> None:
        for t in self.tables:
            t.maybe_flush(force)

    def pending(self) -> int:
        return sum(t.pending() for t in self.tables)

    def close(self) -> None:
        for t in self.tables:
            t.close()
