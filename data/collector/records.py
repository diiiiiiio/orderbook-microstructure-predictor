"""跨模块共享的数据结构：解析后的事件、盘口输出、缺口与质量事件。"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class BookState(str, Enum):
    SYNCING = "SYNCING"
    LIVE = "LIVE"
    STALE = "STALE"
    INVALID = "INVALID"


@dataclass
class DepthEvent:
    """一条 depthUpdate。价格/数量保留原始字符串。"""
    symbol: str
    E: int
    T: int
    U: int
    u: int
    pu: int
    bids: list[list[str]]
    asks: list[list[str]]
    recv_time_ns: int
    recv_monotonic_ns: int
    recv_seq: int
    connection_id: str
    extra: dict[str, Any] = field(default_factory=dict)   # ps 等其他字段

    @staticmethod
    def from_payload(d: dict, recv_time_ns: int, recv_monotonic_ns: int,
                     recv_seq: int, connection_id: str) -> "DepthEvent":
        required = ("E", "T", "s", "U", "u", "pu", "b", "a")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"depthUpdate 缺少字段 {missing}")
        for k in ("E", "T", "U", "u", "pu"):
            if not isinstance(d[k], int) or isinstance(d[k], bool):
                raise ValueError(f"depthUpdate 字段 {k} 不是整数: {d[k]!r}")
        if d["U"] > d["u"]:
            raise ValueError(f"depthUpdate U({d['U']}) > u({d['u']})")
        if not isinstance(d["b"], list) or not isinstance(d["a"], list):
            raise ValueError("depthUpdate b/a 不是数组")
        extra = {k: v for k, v in d.items() if k not in required and k != "e"}
        return DepthEvent(symbol=d["s"], E=d["E"], T=d["T"], U=d["U"], u=d["u"], pu=d["pu"],
                          bids=d["b"], asks=d["a"], recv_time_ns=recv_time_ns,
                          recv_monotonic_ns=recv_monotonic_ns, recv_seq=recv_seq,
                          connection_id=connection_id, extra=extra)

    def content_key(self) -> str:
        """同一 u 内容冲突检测用的内容摘要。"""
        return json.dumps([self.E, self.T, self.U, self.pu, self.bids, self.asks], separators=(",", ":"))


@dataclass
class DepthSnapshot:
    """REST /fapi/v1/depth 响应。"""
    symbol: str
    last_update_id: int
    E: int | None
    T: int | None
    bids: list[list[str]]
    asks: list[list[str]]
    limit: int
    sent_time_ns: int
    recv_time_ns: int
    recv_monotonic_ns: int
    request_id: str

    @staticmethod
    def from_payload(symbol: str, d: dict, limit: int, sent_time_ns: int, recv_time_ns: int,
                     recv_monotonic_ns: int, request_id: str) -> "DepthSnapshot":
        if "lastUpdateId" not in d or "bids" not in d or "asks" not in d:
            raise ValueError("depth 快照缺少 lastUpdateId/bids/asks")
        return DepthSnapshot(symbol=symbol, last_update_id=int(d["lastUpdateId"]),
                             E=d.get("E"), T=d.get("T"), bids=d["bids"], asks=d["asks"], limit=limit,
                             sent_time_ns=sent_time_ns, recv_time_ns=recv_time_ns,
                             recv_monotonic_ns=recv_monotonic_ns, request_id=request_id)


@dataclass
class BookRow:
    """一次有效深度更新处理完成后的前 N 档。价格/数量为 Decimal 字符串（无损）。"""
    symbol: str
    book_epoch: int
    u: int
    U: int
    pu: int
    E: int
    T: int
    recv_time_ns: int
    recv_monotonic_ns: int
    recv_seq: int
    connection_id: str
    bid_px: list[str]
    bid_qty: list[str]
    ask_px: list[str]
    ask_qty: list[str]
    n_bid_levels_known: int
    n_ask_levels_known: int
    is_valid: bool
    state: str
    flags: list[str] = field(default_factory=list)
    available_time_ns: int | None = None   # 处理完成、可供下游使用的本机时间


class BookOutputKind(str, Enum):
    ROW = "row"
    STATE = "state"          # 状态变化
    DROPPED = "dropped"      # 重复/过旧
    CONFLICT = "conflict"    # 同 u 内容不同
    NEED_SNAPSHOT = "need_snapshot"


@dataclass
class BookOutput:
    kind: BookOutputKind
    row: BookRow | None = None
    state: BookState | None = None
    reason: str = ""
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class GapRecord:
    gap_id: str
    symbol: str
    stream: str
    kind: str                       # depth_sequence / depth_sync / depth_coverage / aggtrade_id / markprice_silence / connection / queue_overflow / downtime ...
    certainty: str                  # certain / suspected
    reason: str
    detected_time_ns: int
    start_time_ns: int | None       # 本机时间口径（最后一条正常记录接收时间）
    end_time_ns: int | None
    start_exchange_time_ms: int | None
    end_exchange_time_ms: int | None
    time_basis: str                 # "recv_time_ns(local) / exchange E(ms)" 等说明
    prev_known_id: int | None
    next_known_id: int | None
    repair_status: str              # open / repaired / partial / unrepairable / not_applicable
    session_id: str
    connection_id: str | None = None
    book_epoch_before: int | None = None
    book_epoch_after: int | None = None
    note: str = ""
    update_time_ns: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class QualityEvent:
    time_ns: int
    symbol: str
    stream: str
    event_type: str
    severity: str                   # info / warning / error
    session_id: str
    connection_id: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["detail"] = json.dumps(self.detail, ensure_ascii=False, default=str)
        return d
