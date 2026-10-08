"""质量登记：gaps、quality_events、按 (symbol, stream) 的计数与延迟分布。

纯内存 + 回调；写盘由外层完成。
"""
from __future__ import annotations

import bisect
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable

from data.collector.clock import now_ns
from data.collector.records import GapRecord, QualityEvent


class LatencyWindow:
    """recv_time - exchange_time (ms) 的滚动窗口分位数。注：包含时钟偏差，不是单向网络延迟。"""

    def __init__(self, maxlen: int = 20000):
        self._d: deque[float] = deque(maxlen=maxlen)

    def add(self, v: float) -> None:
        self._d.append(v)

    def percentiles(self) -> dict[str, float | None]:
        if not self._d:
            return {"p50": None, "p95": None, "p99": None, "n": 0}
        s = sorted(self._d)
        n = len(s)
        pick = lambda p: s[min(n - 1, int(p * n))]
        return {"p50": round(pick(0.5), 2), "p95": round(pick(0.95), 2), "p99": round(pick(0.99), 2), "n": n,
                "min": round(s[0], 2), "max": round(s[-1], 2)}


@dataclass
class StreamCounters:
    received: int = 0
    accepted: int = 0
    duplicates: int = 0
    old_or_out_of_order: int = 0
    parse_errors: int = 0
    invalid_values: int = 0
    first_recv_time_ns: int | None = None
    last_recv_time_ns: int | None = None
    last_exchange_time_ms: int | None = None
    extra: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def mark(self, recv_ns: int, ex_ms: int | None = None) -> None:
        self.received += 1
        self.first_recv_time_ns = recv_ns if self.first_recv_time_ns is None else self.first_recv_time_ns
        self.last_recv_time_ns = recv_ns
        if ex_ms is not None:
            self.last_exchange_time_ms = ex_ms


class QualityRegistry:
    def __init__(self, session_id: str, on_gap: Callable[[GapRecord], None],
                 on_event: Callable[[QualityEvent], None], latency_window: int = 20000):
        self.session_id = session_id
        self.on_gap = on_gap
        self.on_event = on_event
        self.counters: dict[tuple[str, str], StreamCounters] = defaultdict(StreamCounters)
        self.latency: dict[tuple[str, str], LatencyWindow] = defaultdict(lambda: LatencyWindow(latency_window))
        self.open_gaps: dict[str, GapRecord] = {}
        self.gap_totals: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.event_totals: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.longest_invalid_ns: dict[tuple[str, str], tuple[int, str]] = {}

    def c(self, symbol: str, stream: str) -> StreamCounters:
        return self.counters[(symbol, stream)]

    def observe_latency(self, symbol: str, stream: str, recv_ns: int, exchange_ms: int) -> None:
        self.latency[(symbol, stream)].add(recv_ns / 1e6 - exchange_ms)

    def event(self, symbol: str, stream: str, event_type: str, severity: str = "info",
              connection_id: str | None = None, **detail: Any) -> QualityEvent:
        ev = QualityEvent(now_ns(), symbol, stream, event_type, severity, self.session_id, connection_id, detail)
        self.event_totals[(symbol, stream)][event_type] += 1
        self.on_event(ev)
        return ev

    def open_gap(self, symbol: str, stream: str, kind: str, certainty: str, reason: str,
                 start_time_ns: int | None, start_exchange_time_ms: int | None, prev_known_id: int | None,
                 time_basis: str, connection_id: str | None = None, book_epoch_before: int | None = None,
                 note: str = "", end_time_ns: int | None = None, end_exchange_time_ms: int | None = None,
                 next_known_id: int | None = None, repair_status: str = "open") -> GapRecord:
        g = GapRecord(gap_id=uuid.uuid4().hex[:16], symbol=symbol, stream=stream, kind=kind, certainty=certainty,
                      reason=reason, detected_time_ns=now_ns(), start_time_ns=start_time_ns, end_time_ns=end_time_ns,
                      start_exchange_time_ms=start_exchange_time_ms, end_exchange_time_ms=end_exchange_time_ms,
                      time_basis=time_basis, prev_known_id=prev_known_id, next_known_id=next_known_id,
                      repair_status=repair_status, session_id=self.session_id, connection_id=connection_id,
                      book_epoch_before=book_epoch_before, note=note, update_time_ns=now_ns())
        self.gap_totals[(symbol, stream)][kind] += 1
        self.gap_totals[(symbol, stream)][f"status_{repair_status}"] += 1
        if repair_status == "open":
            self.open_gaps[g.gap_id] = g
        self.on_gap(g)
        return g

    def close_gap(self, gap: GapRecord, repair_status: str, end_time_ns: int | None = None,
                  end_exchange_time_ms: int | None = None, next_known_id: int | None = None,
                  book_epoch_after: int | None = None, note: str | None = None) -> GapRecord:
        """写一条新的 gap 记录（同 gap_id，新的 repair_status）。研究层按 gap_id 取最新 update_time_ns。"""
        gap.repair_status = repair_status
        gap.end_time_ns = end_time_ns if end_time_ns is not None else gap.end_time_ns
        gap.end_exchange_time_ms = end_exchange_time_ms if end_exchange_time_ms is not None else gap.end_exchange_time_ms
        gap.next_known_id = next_known_id if next_known_id is not None else gap.next_known_id
        gap.book_epoch_after = book_epoch_after if book_epoch_after is not None else gap.book_epoch_after
        if note is not None:
            gap.note = note
        gap.update_time_ns = now_ns()
        key = (gap.symbol, gap.stream)
        self.gap_totals[key]["status_open"] -= 1
        self.gap_totals[key][f"status_{repair_status}"] += 1
        self.open_gaps.pop(gap.gap_id, None)
        if gap.start_time_ns is not None and gap.end_time_ns is not None:
            dur = gap.end_time_ns - gap.start_time_ns
            if dur > self.longest_invalid_ns.get(key, (0, ""))[0]:
                self.longest_invalid_ns[key] = (dur, gap.reason)
        self.on_gap(gap)
        return gap

    def summary(self, symbol: str, stream: str) -> dict[str, Any]:
        c = self.counters[(symbol, stream)]
        key = (symbol, stream)
        lat = self.latency[key].percentiles() if key in self.latency else None
        longest = self.longest_invalid_ns.get(key)
        return {
            "received": c.received, "accepted": c.accepted, "duplicates": c.duplicates,
            "old_or_out_of_order": c.old_or_out_of_order, "parse_errors": c.parse_errors,
            "invalid_values": c.invalid_values,
            "first_recv_time_ns": c.first_recv_time_ns, "last_recv_time_ns": c.last_recv_time_ns,
            "last_exchange_time_ms": c.last_exchange_time_ms,
            "extra": dict(c.extra),
            "gaps": dict(self.gap_totals.get(key, {})),
            "open_gaps": sum(1 for g in self.open_gaps.values() if (g.symbol, g.stream) == key),
            "events": dict(self.event_totals.get(key, {})),
            "recv_minus_exchange_ms": lat,
            "latency_note": "recv_time(本机) - E(交易所)，含时钟偏差与处理排队，不是单向网络延迟",
            "longest_gap": None if not longest else {"duration_s": round(longest[0] / 1e9, 3), "reason": longest[1]},
        }
