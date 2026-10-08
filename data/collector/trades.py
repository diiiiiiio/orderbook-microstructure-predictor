"""aggTrade 处理：解析、去重、按 a 检测缺口、方向解释。

m=true  → 买方是 maker → 主动方是卖方 → aggressor_side="sell"
m=false → 买方是 taker → 主动方是买方 → aggressor_side="buy"
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from data.collector.quality import QualityRegistry
from data.collector.records import GapRecord

REQUIRED = ("a", "p", "q", "f", "l", "T", "m")
KNOWN = set(REQUIRED) | {"e", "E", "s", "nq", "st"}


def aggressor_side(m: bool) -> str:
    return "sell" if m else "buy"


def parse_agg_trade(d: dict[str, Any]) -> dict[str, Any]:
    missing = [k for k in REQUIRED if k not in d]
    if missing:
        raise ValueError(f"aggTrade 缺少字段 {missing}")
    for k in ("a", "f", "l", "T"):
        if not isinstance(d[k], int) or isinstance(d[k], bool):
            raise ValueError(f"aggTrade 字段 {k} 非整数: {d[k]!r}")
    if not isinstance(d["m"], bool):
        raise ValueError(f"aggTrade m 非布尔: {d['m']!r}")
    for k in ("p", "q", "nq"):
        if k in d and not isinstance(d[k], str):
            raise ValueError(f"aggTrade {k} 非字符串: {d[k]!r}")
    if d["f"] > d["l"]:
        raise ValueError(f"aggTrade f({d['f']}) > l({d['l']})")
    extra = {k: v for k, v in d.items() if k not in KNOWN}
    return {
        "a": d["a"], "p": d["p"], "q": d["q"], "nq": d.get("nq"), "f": d["f"], "l": d["l"],
        "T_ms": d["T"], "E_ms": d.get("E"), "m": d["m"], "aggressor_side": aggressor_side(d["m"]),
        "st": d.get("st"), "extra_json": json.dumps(extra, ensure_ascii=False) if extra else None,
    }


@dataclass
class TradeGap:
    gap: GapRecord
    from_a: int          # 缺失的第一个 a
    to_a: int            # 缺失的最后一个 a
    attempts: int = 0
    filled: set[int] = field(default_factory=set)
    sources: dict[str, int] = field(default_factory=dict)   # ws / rest_backfill 各补了多少


class TradeProcessor:
    """每个交易对一个。维护已见 a 的水位与近期集合，产出规范化行、检测缺口。"""

    def __init__(self, symbol: str, q: QualityRegistry, recent_window: int = 200000):
        self.symbol = symbol
        self.q = q
        self.last_a: int | None = None
        self.last_recv_ns: int | None = None
        self.last_T_ms: int | None = None
        self._seen: set[int] = set()
        self._seen_order: list[int] = []
        self.recent_window = recent_window
        self.pending_gaps: dict[str, TradeGap] = {}
        self.contents: dict[int, str] = {}
        self.downtime_gap: GapRecord | None = None     # 重启后由 collector 设置，首条 ws 成交到达时结案

    def _remember(self, a: int, content: str) -> None:
        self._seen.add(a)
        self._seen_order.append(a)
        self.contents[a] = content
        if len(self._seen_order) > self.recent_window:
            old = self._seen_order.pop(0)
            self._seen.discard(old)
            self.contents.pop(old, None)

    def on_ws(self, d: dict[str, Any], recv_ns: int, recv_mono_ns: int, recv_seq: int,
              connection_id: str) -> dict[str, Any] | None:
        c = self.q.c(self.symbol, "aggTrade")
        c.mark(recv_ns, d.get("E") if isinstance(d.get("E"), int) else None)
        try:
            row = parse_agg_trade(d)
        except ValueError as exc:
            c.parse_errors += 1
            self.q.event(self.symbol, "aggTrade", "parse_error", "error", connection_id, error=str(exc))
            return None
        a = row["a"]
        content = json.dumps([row["p"], row["q"], row["f"], row["l"], row["T_ms"], row["m"]])
        if a in self._seen:
            c.duplicates += 1
            if self.contents.get(a) not in (None, content):
                self.q.event(self.symbol, "aggTrade", "same_id_different_content", "error", connection_id, a=a)
            return None
        if self.downtime_gap is not None and self.last_a is None:
            dg, self.downtime_gap = self.downtime_gap, None
            self.q.close_gap(dg, "unrepairable", end_time_ns=recv_ns, next_known_id=a,
                             note=dg.note + "; no persisted aggTrade watermark before downtime: missing count unknown")
        elif self.downtime_gap is not None and self.last_a is not None and a > self.last_a:
            dg, self.downtime_gap = self.downtime_gap, None
            if a == self.last_a + 1:
                self.q.close_gap(dg, "not_applicable", end_time_ns=recv_ns, next_known_id=a,
                                 note=dg.note + "; no aggTrade id missing across downtime")
            else:
                self.q.close_gap(dg, "not_applicable", end_time_ns=recv_ns, next_known_id=a,
                                 note=dg.note + f"; missing ids tracked by aggtrade_id gap [{self.last_a + 1},{a - 1}]")
        if self.last_a is not None:
            if a <= self.last_a:
                # 迟到的老成交（可能是重叠连接补上了此前缺口的一部分）
                c.old_or_out_of_order += 1
                self._fill_pending(a)
            elif a > self.last_a + 1:
                missing_from, missing_to = self.last_a + 1, a - 1
                g = self.q.open_gap(self.symbol, "aggTrade", "aggtrade_id", "certain",
                                    f"a jumped {self.last_a}->{a}",
                                    start_time_ns=self.last_recv_ns, start_exchange_time_ms=self.last_T_ms,
                                    prev_known_id=self.last_a, next_known_id=a, end_time_ns=recv_ns,
                                    end_exchange_time_ms=row["T_ms"], connection_id=connection_id,
                                    time_basis="start/end = 相邻已收到成交的本机接收时间；exchange 口径为 T(ms)",
                                    note=f"missing a in [{missing_from},{missing_to}] count={missing_to - missing_from + 1}")
                self.pending_gaps[g.gap_id] = TradeGap(g, missing_from, missing_to)
                c.extra["suspected_missing_trades"] += missing_to - missing_from + 1
        if self.last_a is None or a > self.last_a:
            self.last_a = a
            self.last_recv_ns = recv_ns
            self.last_T_ms = row["T_ms"]
        self._remember(a, content)
        c.accepted += 1
        if row["E_ms"] is not None:
            self.q.observe_latency(self.symbol, "aggTrade", recv_ns, row["E_ms"])
        row.update({"symbol": self.symbol, "source": "ws", "recv_time_ns": recv_ns, "known_time_ns": recv_ns,
                    "recv_seq": recv_seq, "connection_id": connection_id})
        return row

    def _fill_pending(self, a: int, source: str = "ws") -> None:
        for tg in list(self.pending_gaps.values()):
            if tg.from_a <= a <= tg.to_a:
                tg.filled.add(a)
                tg.sources[source] = tg.sources.get(source, 0) + 1
                if len(tg.filled) == tg.to_a - tg.from_a + 1:
                    src = ", ".join(f"{k}={v}" for k, v in sorted(tg.sources.items()))
                    self.q.close_gap(tg.gap, "repaired", note=tg.gap.note + f"; filled ({src})")
                    self.pending_gaps.pop(tg.gap.gap_id, None)

    def on_backfill(self, items: list[dict[str, Any]], known_time_ns: int, request_id: str) -> list[dict[str, Any]]:
        """REST /aggTrades 返回的条目。REST 没有 E、没有实时接收时间：留空，不伪造。"""
        out = []
        c = self.q.c(self.symbol, "aggTrade")
        for d in items:
            try:
                row = parse_agg_trade(d)
            except ValueError as exc:
                c.parse_errors += 1
                self.q.event(self.symbol, "aggTrade", "backfill_parse_error", "error", error=str(exc))
                continue
            a = row["a"]
            if a in self._seen:
                continue
            self._remember(a, json.dumps([row["p"], row["q"], row["f"], row["l"], row["T_ms"], row["m"]]))
            self._fill_pending(a, "rest_backfill")
            c.extra["backfilled"] += 1
            row.update({"symbol": self.symbol, "source": "rest_backfill", "recv_time_ns": None,
                        "known_time_ns": known_time_ns, "recv_seq": None, "connection_id": request_id})
            out.append(row)
        return out

    def resume_from_checkpoint(self, last_a: int, last_T_ms: int | None, last_recv_ns: int | None) -> None:
        """重启：以持久化的最后 a 为水位，下一条 ws 成交若跳号则形成缺口，触发补数。"""
        self.last_a = last_a
        self.last_T_ms = last_T_ms
        self.last_recv_ns = last_recv_ns
