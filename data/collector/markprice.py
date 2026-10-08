"""markPrice@1s 处理：解析、按 E 去重/乱序、静默检测。标记价格不是可成交价格；r 是实时预估费率，不是已扣收资金费。"""
from __future__ import annotations

import json
from typing import Any

from data.collector.quality import QualityRegistry

REQUIRED = ("E", "p", "r", "T")
KNOWN = set(REQUIRED) | {"e", "s", "ap", "i", "P", "st"}


def parse_mark_price(d: dict[str, Any]) -> dict[str, Any]:
    missing = [k for k in REQUIRED if k not in d]
    if missing:
        raise ValueError(f"markPriceUpdate 缺少字段 {missing}")
    for k in ("E", "T"):
        if not isinstance(d[k], int) or isinstance(d[k], bool):
            raise ValueError(f"markPriceUpdate {k} 非整数")
    for k in ("p", "ap", "i", "P", "r"):
        if k in d and not isinstance(d[k], str):
            raise ValueError(f"markPriceUpdate {k} 非字符串: {d[k]!r}")
    extra = {k: v for k, v in d.items() if k not in KNOWN}
    return {"E_ms": d["E"], "p": d["p"], "ap": d.get("ap"), "i": d.get("i"), "P": d.get("P"), "r": d["r"],
            "T_next_funding_ms": d["T"], "st": d.get("st"),
            "extra_json": json.dumps(extra, ensure_ascii=False) if extra else None}


class MarkPriceProcessor:
    def __init__(self, symbol: str, q: QualityRegistry, expected_interval_ms: int = 1000, gap_factor: float = 5.0):
        self.symbol = symbol
        self.q = q
        self.last_E: int | None = None
        self.last_recv_ns: int | None = None
        self.expected_interval_ms = expected_interval_ms
        self.gap_factor = gap_factor

    def on_ws(self, d: dict[str, Any], recv_ns: int, recv_mono_ns: int, recv_seq: int,
              connection_id: str) -> dict[str, Any] | None:
        c = self.q.c(self.symbol, "markPrice")
        c.mark(recv_ns, d.get("E") if isinstance(d.get("E"), int) else None)
        try:
            row = parse_mark_price(d)
        except ValueError as exc:
            c.parse_errors += 1
            self.q.event(self.symbol, "markPrice", "parse_error", "error", connection_id, error=str(exc))
            return None
        E = row["E_ms"]
        if self.last_E is not None:
            if E == self.last_E:
                c.duplicates += 1
                return None
            if E < self.last_E:
                c.old_or_out_of_order += 1
                return None
            if E - self.last_E > self.expected_interval_ms * self.gap_factor:
                self.q.open_gap(self.symbol, "markPrice", "markprice_silence", "suspected",
                                f"E jumped {E - self.last_E}ms (> {self.gap_factor}x interval)",
                                start_time_ns=self.last_recv_ns, start_exchange_time_ms=self.last_E,
                                prev_known_id=self.last_E, next_known_id=E, end_time_ns=recv_ns,
                                end_exchange_time_ms=E, connection_id=connection_id, repair_status="unrepairable",
                                time_basis="exchange E(ms)；markPrice 无历史补数接口，缺失即缺失",
                                note="服务端推送间隔本身可能波动，属疑似而非确定丢包")
        self.last_E = E
        self.last_recv_ns = recv_ns
        c.accepted += 1
        self.q.observe_latency(self.symbol, "markPrice", recv_ns, E)
        row.update({"symbol": self.symbol, "recv_time_ns": recv_ns, "recv_monotonic_ns": recv_mono_ns,
                    "recv_seq": recv_seq, "connection_id": connection_id})
        return row
