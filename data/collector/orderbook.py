"""本地订单簿：USDⓈ-M 官方衔接规则 + 状态机 + 覆盖范围跟踪。纯逻辑，无 I/O、无时钟。

官方规则（USDⓈ-M，与 Spot 不同）：
  1. 缓存深度增量；2. REST 取快照 lastUpdateId；
  3. 丢弃 u < lastUpdateId 的事件；
  4. 第一条应用的事件须满足 U <= lastUpdateId <= u；
  5. 之后每条事件的 pu 必须等于上一条已应用事件的 u，否则从第 2 步重来。
数量为绝对值；为 0 删除价位；删除不存在的价位属正常。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from data.collector.numeric import PrecisionError, SymbolSpec
from data.collector.records import (BookOutput, BookOutputKind, BookRow, BookState,
                                    DepthEvent, DepthSnapshot)


@dataclass
class BookStats:
    events_applied: int = 0
    events_dropped_old: int = 0
    events_dropped_dup: int = 0
    conflicts: int = 0
    seq_gaps: int = 0
    crosses: int = 0
    parse_errors: int = 0
    coverage_failures: int = 0
    buffer_overflows: int = 0
    snapshot_stale: int = 0
    resyncs: int = 0
    stale_transitions: int = 0


class OrderBook:
    def __init__(self, symbol: str, spec: SymbolSpec, export_levels: int = 20,
                 buffer_max_events: int = 20000, stale_after_ms: int = 5000,
                 conflict_window: int = 2000):
        self.symbol = symbol
        self.spec = spec
        self.export_levels = export_levels
        self.buffer_max_events = buffer_max_events
        self.stale_after_ns = stale_after_ms * 1_000_000
        self.conflict_window = conflict_window
        self.state = BookState.SYNCING
        self.epoch = 0
        self.last_u: int | None = None
        self.last_event_recv_ns: int | None = None
        self.bids: dict[int, int] = {}
        self.asks: dict[int, int] = {}
        self.cov_bid_floor: int | None = None     # 快照覆盖的最低买价（ticks）；None = 全覆盖
        self.cov_ask_ceiling: int | None = None
        self.buffer: list[DepthEvent] = []
        self.pending_snapshot: DepthSnapshot | None = None
        self.snapshot_wanted = True
        self.stats = BookStats()
        self._recent_keys: dict[int, str] = {}
        self._recent_order: list[int] = []
        self.invalid_reason: str = ""
        self.last_snapshot_id: int | None = None

    # ---------- 状态 ----------
    def start_sync(self, reason: str) -> BookOutput:
        prev = self.state
        self.state = BookState.SYNCING
        self.buffer.clear()
        self.pending_snapshot = None
        self.snapshot_wanted = True
        self.bids.clear()
        self.asks.clear()
        self.last_u = None
        self.cov_bid_floor = self.cov_ask_ceiling = None
        self.stats.resyncs += 1
        return BookOutput(BookOutputKind.STATE, state=BookState.SYNCING, reason=reason,
                          detail={"from": prev.value})

    def _invalidate(self, reason: str, detail: dict[str, Any] | None = None) -> BookOutput:
        self.state = BookState.INVALID
        self.invalid_reason = reason
        self.buffer.clear()
        self.pending_snapshot = None
        self.snapshot_wanted = False
        return BookOutput(BookOutputKind.STATE, state=BookState.INVALID, reason=reason,
                          detail=detail or {})

    def check_stale(self, now_ns: int) -> BookOutput | None:
        if self.state == BookState.LIVE and self.last_event_recv_ns is not None \
                and now_ns - self.last_event_recv_ns > self.stale_after_ns:
            self.state = BookState.STALE
            self.stats.stale_transitions += 1
            return BookOutput(BookOutputKind.STATE, state=BookState.STALE, reason="no_depth_event",
                              detail={"silent_ms": (now_ns - self.last_event_recv_ns) / 1e6})
        return None

    # ---------- 输入 ----------
    def on_event(self, evt: DepthEvent) -> list[BookOutput]:
        if self.state == BookState.SYNCING:
            return self._buffer_event(evt)
        if self.state == BookState.INVALID:
            # 等待外层调用 start_sync；期间不产生任何有效盘口
            return [BookOutput(BookOutputKind.DROPPED, reason="book_invalid")]
        # LIVE / STALE
        assert self.last_u is not None
        dup = self._check_dup(evt)
        if dup is not None:
            return [dup]
        if evt.pu != self.last_u:
            self.stats.seq_gaps += 1
            return [self._invalidate("pu_mismatch", {"expected_pu": self.last_u, "got_pu": evt.pu,
                                                     "U": evt.U, "u": evt.u,
                                                     "connection_id": evt.connection_id})]
        return self._apply_live(evt, resumed=(self.state == BookState.STALE))

    def on_snapshot(self, snap: DepthSnapshot) -> list[BookOutput]:
        if self.state != BookState.SYNCING:
            return [BookOutput(BookOutputKind.DROPPED, reason="snapshot_while_not_syncing",
                               detail={"state": self.state.value})]
        self.pending_snapshot = snap
        self.snapshot_wanted = False
        return self._try_link()

    # ---------- 内部 ----------
    def _check_dup(self, evt: DepthEvent) -> BookOutput | None:
        assert self.last_u is not None
        if evt.u <= self.last_u:
            prev = self._recent_keys.get(evt.u)
            if prev is not None and prev != evt.content_key():
                self.stats.conflicts += 1
                return BookOutput(BookOutputKind.CONFLICT, reason="same_u_different_content",
                                  detail={"u": evt.u, "connection_id": evt.connection_id})
            if evt.u == self.last_u or prev is not None:
                self.stats.events_dropped_dup += 1
                return BookOutput(BookOutputKind.DROPPED, reason="duplicate", detail={"u": evt.u})
            self.stats.events_dropped_old += 1
            return BookOutput(BookOutputKind.DROPPED, reason="old", detail={"u": evt.u, "last_u": self.last_u})
        return None

    def _remember(self, evt: DepthEvent) -> None:
        self._recent_keys[evt.u] = evt.content_key()
        self._recent_order.append(evt.u)
        if len(self._recent_order) > self.conflict_window:
            old = self._recent_order.pop(0)
            self._recent_keys.pop(old, None)

    def _buffer_event(self, evt: DepthEvent) -> list[BookOutput]:
        if len(self.buffer) >= self.buffer_max_events:
            self.stats.buffer_overflows += 1
            self.buffer.clear()
            self.pending_snapshot = None
            self.snapshot_wanted = True
            return [BookOutput(BookOutputKind.NEED_SNAPSHOT, reason="buffer_overflow",
                               detail={"buffer_max": self.buffer_max_events})]
        self.buffer.append(evt)
        if self.pending_snapshot is not None:
            return self._try_link()
        return []

    def _try_link(self) -> list[BookOutput]:
        snap = self.pending_snapshot
        assert snap is not None
        last = snap.last_update_id
        kept = [e for e in self.buffer if e.u >= last]
        self.stats.events_dropped_old += len(self.buffer) - len(kept)
        self.buffer = kept
        if not self.buffer:
            return []                                    # 继续等事件
        first = self.buffer[0]
        if first.U > last:
            # 快照早于缓存最早事件，中间有事件从未收到：需要更新的快照
            self.stats.snapshot_stale += 1
            self.pending_snapshot = None
            self.snapshot_wanted = True
            return [BookOutput(BookOutputKind.NEED_SNAPSHOT, reason="snapshot_older_than_buffer",
                               detail={"lastUpdateId": last, "first_U": first.U})]
        # first.U <= last <= first.u：可以衔接
        try:
            self._load_snapshot(snap)
        except PrecisionError as exc:
            self.stats.parse_errors += 1
            return [self._invalidate("snapshot_parse_error", {"error": str(exc)})]
        outputs: list[BookOutput] = []
        self.epoch += 1
        self.last_snapshot_id = last
        self.state = BookState.LIVE
        outputs.append(BookOutput(BookOutputKind.STATE, state=BookState.LIVE, reason="synced",
                                  detail={"lastUpdateId": last, "first_U": first.U, "first_u": first.u,
                                          "epoch": self.epoch, "buffered": len(self.buffer),
                                          "snapshot_bids": len(snap.bids), "snapshot_asks": len(snap.asks),
                                          "cov_bid_floor_ticks": self.cov_bid_floor,
                                          "cov_ask_ceiling_ticks": self.cov_ask_ceiling}))
        buffered = self.buffer
        self.buffer = []
        self.pending_snapshot = None
        # 第一条：不检查 pu，整条应用
        self.last_u = first.pu   # 让 _apply_live 的 pu 校验对第一条恒成立
        for i, e in enumerate(buffered):
            if i > 0:
                dup = self._check_dup(e)
                if dup is not None:
                    outputs.append(dup)
                    continue
                if e.pu != self.last_u:
                    self.stats.seq_gaps += 1
                    outputs.append(self._invalidate("pu_mismatch_during_sync",
                                                    {"expected_pu": self.last_u, "got_pu": e.pu, "u": e.u}))
                    return outputs
            outputs.extend(self._apply_live(e, first_after_snapshot=(i == 0)))
            if self.state == BookState.INVALID:
                return outputs
        return outputs

    def _load_snapshot(self, snap: DepthSnapshot) -> None:
        self.bids = {}
        self.asks = {}
        for p, q in snap.bids:
            t, n = self.spec.price_to_ticks(p), self.spec.qty_to_steps(q)
            if n > 0:
                self.bids[t] = n
        for p, q in snap.asks:
            t, n = self.spec.price_to_ticks(p), self.spec.qty_to_steps(q)
            if n > 0:
                self.asks[t] = n
        # 覆盖边界：档数达到 limit 说明可能还有更深的档位未包含
        self.cov_bid_floor = min(self.bids) if len(snap.bids) >= snap.limit and self.bids else None
        self.cov_ask_ceiling = max(self.asks) if len(snap.asks) >= snap.limit and self.asks else None

    def _apply_live(self, evt: DepthEvent, resumed: bool = False,
                    first_after_snapshot: bool = False) -> list[BookOutput]:
        # 先整体解析，再整体应用，避免半条消息状态外泄
        try:
            b = [(self.spec.price_to_ticks(p), self.spec.qty_to_steps(q)) for p, q in evt.bids]
            a = [(self.spec.price_to_ticks(p), self.spec.qty_to_steps(q)) for p, q in evt.asks]
        except (PrecisionError, ValueError, TypeError) as exc:
            self.stats.parse_errors += 1
            return [self._invalidate("event_parse_error", {"u": evt.u, "error": str(exc)})]
        for t, n in b:
            if n == 0:
                self.bids.pop(t, None)
            else:
                self.bids[t] = n
        for t, n in a:
            if n == 0:
                self.asks.pop(t, None)
            else:
                self.asks[t] = n
        self.last_u = evt.u
        self.last_event_recv_ns = evt.recv_time_ns
        self._remember(evt)
        self.stats.events_applied += 1
        flags: list[str] = []
        if first_after_snapshot:
            flags.append("first_after_snapshot")
        if resumed:
            flags.append("resumed_after_stale")
        # 一致性检查
        if self.bids and self.asks and max(self.bids) >= min(self.asks):
            self.stats.crosses += 1
            return [self._invalidate("crossed_book", {"u": evt.u, "best_bid_ticks": max(self.bids),
                                                     "best_ask_ticks": min(self.asks)})]
        cov = self._coverage_ok()
        if cov is not None:
            self.stats.coverage_failures += 1
            return [self._invalidate("top_levels_coverage_unconfirmed", {"u": evt.u, **cov})]
        self.state = BookState.LIVE
        return [BookOutput(BookOutputKind.ROW, row=self._export(evt, flags))]

    def _coverage_ok(self) -> dict | None:
        n = self.export_levels
        bids = sorted(self.bids, reverse=True)[:n]
        asks = sorted(self.asks)[:n]
        if self.cov_bid_floor is not None:
            if len(bids) < n or bids[-1] < self.cov_bid_floor:
                return {"side": "bid", "levels_known": len(bids),
                        "nth_level_ticks": bids[-1] if bids else None, "floor_ticks": self.cov_bid_floor}
        if self.cov_ask_ceiling is not None:
            if len(asks) < n or asks[-1] > self.cov_ask_ceiling:
                return {"side": "ask", "levels_known": len(asks),
                        "nth_level_ticks": asks[-1] if asks else None, "ceiling_ticks": self.cov_ask_ceiling}
        return None

    def _export(self, evt: DepthEvent, flags: list[str]) -> BookRow:
        n = self.export_levels
        bids = sorted(self.bids, reverse=True)[:n]
        asks = sorted(self.asks)[:n]
        return BookRow(
            symbol=self.symbol, book_epoch=self.epoch, u=evt.u, U=evt.U, pu=evt.pu, E=evt.E, T=evt.T,
            recv_time_ns=evt.recv_time_ns, recv_monotonic_ns=evt.recv_monotonic_ns, recv_seq=evt.recv_seq,
            connection_id=evt.connection_id,
            bid_px=[str(self.spec.ticks_to_price(t)) for t in bids],
            bid_qty=[str(self.spec.steps_to_qty(self.bids[t])) for t in bids],
            ask_px=[str(self.spec.ticks_to_price(t)) for t in asks],
            ask_qty=[str(self.spec.steps_to_qty(self.asks[t])) for t in asks],
            n_bid_levels_known=len(self.bids), n_ask_levels_known=len(self.asks),
            is_valid=True, state=BookState.LIVE.value, flags=flags,
        )

    def top(self, n: int | None = None) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
        n = n or self.export_levels
        bids = sorted(self.bids, reverse=True)[:n]
        asks = sorted(self.asks)[:n]
        return [(t, self.bids[t]) for t in bids], [(t, self.asks[t]) for t in asks]
