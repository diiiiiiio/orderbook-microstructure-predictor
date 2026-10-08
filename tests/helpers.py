"""测试用的事件/快照构造器。"""
from __future__ import annotations

from decimal import Decimal

from data.collector.numeric import SymbolSpec
from data.collector.records import DepthEvent, DepthSnapshot

SPEC = SymbolSpec("BTCUSDT", Decimal("0.10"), Decimal("0.001"))


def ev(U: int, u: int, pu: int, bids=None, asks=None, recv=1_000_000_000, seq=0, cid="c1", E=None) -> DepthEvent:
    return DepthEvent(symbol="BTCUSDT", E=E if E is not None else 1_700_000_000_000 + u, T=1_700_000_000_000 + u,
                      U=U, u=u, pu=pu, bids=bids or [], asks=asks or [],
                      recv_time_ns=recv, recv_monotonic_ns=recv, recv_seq=seq, connection_id=cid)


def snap(last: int, bids=None, asks=None, limit=1000) -> DepthSnapshot:
    return DepthSnapshot(symbol="BTCUSDT", last_update_id=last, E=1, T=1,
                         bids=bids if bids is not None else [["100.0", "1"], ["99.9", "2"]],
                         asks=asks if asks is not None else [["100.1", "1"], ["100.2", "2"]],
                         limit=limit, sent_time_ns=0, recv_time_ns=0, recv_monotonic_ns=0, request_id="r1")
