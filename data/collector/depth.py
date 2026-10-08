"""深度管线：每个交易对一个。把 RawMessage 喂给 OrderBook，管理快照请求、gap 记录、状态事件。"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

from data.collector.clock import mono_ns, now_ns
from data.collector.config import OrderBookConfig
from data.collector.numeric import SymbolSpec
from data.collector.orderbook import OrderBook
from data.collector.quality import QualityRegistry
from data.collector.records import (BookOutput, BookOutputKind, BookRow, BookState, DepthEvent,
                                    DepthSnapshot, GapRecord)

log = logging.getLogger("collector.depth")

SnapshotFetcher = Callable[[str, int], Awaitable[DepthSnapshot | None]]
RowSink = Callable[[BookRow], None]


class DepthPipeline:
    def __init__(self, symbol: str, spec: SymbolSpec, cfg: OrderBookConfig, q: QualityRegistry,
                 fetch_snapshot: SnapshotFetcher, row_sink: RowSink, depth_limit: int):
        self.symbol = symbol
        self.cfg = cfg
        self.q = q
        self.book = OrderBook(symbol, spec, cfg.export_levels, cfg.buffer_max_events, cfg.stale_after_ms)
        self.fetch_snapshot = fetch_snapshot
        self.row_sink = row_sink
        self.depth_limit = depth_limit
        self._snapshot_task: asyncio.Task | None = None
        self._last_snapshot_req_mono = 0.0
        self.current_gap: GapRecord | None = None
        self.last_valid_recv_ns: int | None = None
        self.last_valid_u: int | None = None
        self.last_valid_E: int | None = None
        self.rows_emitted = 0
        self.snapshots_requested = 0
        self.snapshots_failed = 0
        self.sync_failures = 0
        self.recoveries = 0
        self.first_sync_done = False

    # ---------- 输入 ----------
    def on_message(self, data: dict[str, Any], recv_ns: int, recv_mono: int, recv_seq: int, cid: str) -> None:
        c = self.q.c(self.symbol, "depth")
        c.mark(recv_ns, data.get("E") if isinstance(data.get("E"), int) else None)
        try:
            evt = DepthEvent.from_payload(data, recv_ns, recv_mono, recv_seq, cid)
        except (ValueError, TypeError, KeyError) as exc:
            c.parse_errors += 1
            self.q.event(self.symbol, "depth", "parse_error", "error", cid, error=str(exc))
            if self.book.state in (BookState.LIVE, BookState.STALE):
                self._handle_outputs([self.book._invalidate("event_unparseable", {"error": str(exc)})], None)
            return
        if isinstance(evt.E, int):
            self.q.observe_latency(self.symbol, "depth", recv_ns, evt.E)
        outs = self.book.on_event(evt)
        self._handle_outputs(outs, evt)
        if self.book.snapshot_wanted:
            self._ensure_snapshot()

    def on_tick(self) -> None:
        """周期调用：STALE 检测、需要快照时补请求。"""
        out = self.book.check_stale(now_ns())
        if out is not None:
            self._handle_outputs([out], None)
        if self.book.state == BookState.INVALID:
            self._resync("after_invalid")
        elif self.book.state == BookState.SYNCING and self.book.snapshot_wanted:
            self._ensure_snapshot()

    def start(self) -> None:
        self.book.start_sync("startup")

    # ---------- 内部 ----------
    def _resync(self, reason: str) -> None:
        now = time.monotonic()
        if now - self._last_snapshot_req_mono < self.cfg.resync_min_interval_s:
            return
        out = self.book.start_sync(reason)
        self.q.event(self.symbol, "depth", "resync_start", "warning", reason=reason, epoch=self.book.epoch)
        self._ensure_snapshot()

    def _ensure_snapshot(self) -> None:
        if self._snapshot_task and not self._snapshot_task.done():
            return
        if not self.book.buffer:
            return                      # 官方顺序：先有 WS 增量缓存，再取 REST 快照
        now = time.monotonic()
        if now - self._last_snapshot_req_mono < self.cfg.resync_min_interval_s:
            return
        self._last_snapshot_req_mono = now
        self.snapshots_requested += 1
        self._snapshot_task = asyncio.create_task(self._do_snapshot(), name=f"snapshot-{self.symbol}")

    async def _do_snapshot(self) -> None:
        try:
            snap = await self.fetch_snapshot(self.symbol, self.depth_limit)
        except Exception as exc:                      # noqa: BLE001
            snap = None
            self.q.event(self.symbol, "depth", "snapshot_error", "error", error=str(exc))
        if snap is None:
            self.snapshots_failed += 1
            self.book.snapshot_wanted = True
            return
        if self.book.state != BookState.SYNCING:
            return
        outs = self.book.on_snapshot(snap)
        self._handle_outputs(outs, None)

    def _handle_outputs(self, outs: list[BookOutput], evt: DepthEvent | None) -> None:
        c = self.q.c(self.symbol, "depth")
        avail = now_ns()
        for o in outs:
            if o.kind == BookOutputKind.ROW and o.row is not None:
                o.row.available_time_ns = avail
                self.row_sink(o.row)
                self.rows_emitted += 1
                c.accepted += 1
                self.last_valid_recv_ns = o.row.recv_time_ns
                self.last_valid_u = o.row.u
                self.last_valid_E = o.row.E
            elif o.kind == BookOutputKind.DROPPED:
                if o.reason == "duplicate":
                    c.duplicates += 1
                elif o.reason == "old":
                    c.old_or_out_of_order += 1
                else:
                    c.extra[f"dropped_{o.reason}"] += 1
            elif o.kind == BookOutputKind.CONFLICT:
                c.extra["conflicts"] += 1
                self.q.event(self.symbol, "depth", "same_u_different_content", "error",
                             evt.connection_id if evt else None, **o.detail)
            elif o.kind == BookOutputKind.NEED_SNAPSHOT:
                c.extra[f"need_snapshot_{o.reason}"] += 1
                self.q.event(self.symbol, "depth", "need_snapshot", "warning", reason=o.reason, **o.detail)
                if o.reason == "buffer_overflow":
                    self._open_gap("depth_buffer_overflow", "certain", o.reason, evt)
            elif o.kind == BookOutputKind.STATE:
                self._on_state(o, evt)

    def _on_state(self, o: BookOutput, evt: DepthEvent | None) -> None:
        st = o.state
        cid = evt.connection_id if evt else None
        sev = "info" if st == BookState.LIVE else "warning" if st in (BookState.SYNCING, BookState.STALE) else "error"
        self.q.event(self.symbol, "depth", f"state_{st.value.lower()}", sev, cid, reason=o.reason,
                     epoch=self.book.epoch, **{k: v for k, v in o.detail.items() if k not in ("connection_id", "epoch", "reason")})
        c = self.q.c(self.symbol, "depth")
        if st == BookState.INVALID:
            c.extra[f"invalid_{o.reason}"] += 1
            if o.reason in ("crossed_book",):
                c.extra["crossed_book"] += 1
            self.sync_failures += 1 if o.reason.startswith("pu_mismatch_during_sync") else 0
            self._open_gap("depth_sequence" if "pu_mismatch" in o.reason else "depth_consistency",
                           "certain", o.reason, evt, o.detail)
        elif st == BookState.STALE:
            self._open_gap("depth_stale", "suspected", o.reason, None, o.detail)
        elif st == BookState.SYNCING:
            if self.current_gap is None and self.first_sync_done:
                self._open_gap("depth_sync", "certain", o.reason, evt, o.detail)
        elif st == BookState.LIVE:
            if self.current_gap is not None:
                g = self.current_gap
                self.current_gap = None
                self.recoveries += 1
                self.q.close_gap(g, "not_applicable",
                                 end_time_ns=now_ns(), end_exchange_time_ms=None, next_known_id=o.detail.get("first_u"),
                                 book_epoch_after=self.book.epoch,
                                 note=(g.note + "; 已用新快照恢复当前盘口(新 book_epoch)，区间内盘口历史不可恢复").strip("; "))
            self.first_sync_done = True

    def _open_gap(self, kind: str, certainty: str, reason: str, evt: DepthEvent | None,
                  detail: dict[str, Any] | None = None) -> None:
        if self.current_gap is not None:
            # 已经在缺口里（比如 STALE → INVALID），只补充说明
            self.current_gap.note = (self.current_gap.note + f"; then {reason}").strip("; ")
            if certainty == "certain":
                self.current_gap.certainty = "certain"
            return
        self.current_gap = self.q.open_gap(
            self.symbol, "depth", kind, certainty, reason,
            start_time_ns=self.last_valid_recv_ns, start_exchange_time_ms=self.last_valid_E,
            prev_known_id=self.last_valid_u, connection_id=evt.connection_id if evt else None,
            book_epoch_before=self.book.epoch,
            time_basis="start = 最后一条有效盘口的本机接收时间 / 其 E(ms)；end = 新 epoch LIVE 时刻(本机)",
            note=f"detail={detail}" if detail else "")

    def status(self) -> dict[str, Any]:
        b = self.book
        return {
            "state": b.state.value, "book_epoch": b.epoch, "last_u": b.last_u,
            "last_valid_recv_time_ns": self.last_valid_recv_ns, "rows_emitted": self.rows_emitted,
            "levels_known": {"bids": len(b.bids), "asks": len(b.asks)},
            "coverage_bounded": {"bid": b.cov_bid_floor is not None, "ask": b.cov_ask_ceiling is not None},
            "buffered_events": len(b.buffer),
            "snapshots_requested": self.snapshots_requested, "snapshots_failed": self.snapshots_failed,
            "resyncs": b.stats.resyncs, "recoveries": self.recoveries,
            "dropped_old_before_snapshot": b.stats.events_dropped_old, "seq_gaps": b.stats.seq_gaps, "crosses": b.stats.crosses, "conflicts": b.stats.conflicts,
            "coverage_failures": b.stats.coverage_failures, "buffer_overflows": b.stats.buffer_overflows,
            "stale_transitions": b.stats.stale_transitions,
            "in_gap": self.current_gap.gap_id if self.current_gap else None,
        }
