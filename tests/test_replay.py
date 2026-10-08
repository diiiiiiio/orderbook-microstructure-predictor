"""12. 原始日志离线回放与在线结果一致（合成数据端到端），且重复回放确定。"""
import json
from decimal import Decimal
from pathlib import Path

from data.collector import SCHEMA_VERSION
from data.collector.numeric import SymbolSpec
from data.collector.replay import compare, load_inputs, replay_rows
from data.collector.storage.raw import RawWriter
from data.collector.orderbook import OrderBook
from data.collector.records import BookOutputKind, DepthEvent, DepthSnapshot

SPEC = SymbolSpec("BTCUSDT", Decimal("0.10"), Decimal("0.001"))
DAY = "20260101"
T0 = 1_767_225_600_000_000_000     # 2026-01-01T00:00:00Z ns


def depth_raw(U, u, pu, b, a, recv):
    data = {"e": "depthUpdate", "E": recv // 1_000_000, "T": recv // 1_000_000, "s": "BTCUSDT", "U": U, "u": u, "pu": pu, "b": b, "a": a}
    return {"schema_version": SCHEMA_VERSION, "symbol": "BTCUSDT", "stream": "btcusdt@depth@100ms", "source": "ws",
            "session_id": "s1", "connection_id": "c1", "recv_seq": u, "recv_time_ns": recv, "recv_monotonic_ns": recv,
            "id": u, "raw": json.dumps({"stream": "btcusdt@depth@100ms", "data": data})}


def snap_raw(last, bids, asks, recv):
    body = {"lastUpdateId": last, "E": recv // 1_000_000, "T": recv // 1_000_000, "bids": bids, "asks": asks}
    return {"schema_version": SCHEMA_VERSION, "source": "rest", "request_id": f"r{last}", "endpoint": "/fapi/v1/depth",
            "params": {"symbol": "BTCUSDT", "limit": 1000}, "status": 200, "headers": {}, "sent_time_ns": recv - 1000,
            "recv_time_ns": recv, "recv_monotonic_ns": recv, "session_id": "s1", "symbol": "BTCUSDT", "raw": json.dumps(body)}


def build_dataset(tmp_path: Path):
    """在线过程：事件 1..3 缓存 → 快照(last=2) → LIVE → 事件 4,5 → pu 断档(事件 7) → 重同步 快照(last=7) → 事件 8。"""
    w = RawWriter(tmp_path, "s1", SCHEMA_VERSION, 1 << 30, 1e9, 0, 0, compress=True)
    recs = []
    t = T0
    def ev(U, u, pu, b=(), a=()):
        nonlocal t
        t += 100_000_000
        r = depth_raw(U, u, pu, list(b), list(a), t)
        recs.append(("depth", r))
        return r
    def sn(last, bids, asks):
        nonlocal t
        t += 50_000_000
        r = snap_raw(last, bids, asks, t)
        recs.append(("rest", r))
        return r
    ev(1, 1, 0, [["100.0", "1"]])
    ev(2, 2, 1, [["99.9", "2"]])
    sn(2, [["100.0", "1"], ["99.9", "2"]], [["100.1", "1"]])
    ev(3, 3, 2, [["99.8", "3"]])
    ev(4, 4, 3, a=[["100.1", "0"], ["100.2", "5"]])
    ev(5, 5, 4, [["100.0", "0.5"]])
    ev(7, 7, 6, [["100.0", "9"]])                 # pu 断档 → INVALID
    ev(8, 8, 7, [["97.0", "1"]])                  # 缓存
    sn(8, [["100.0", "9"], ["97.0", "1"]], [["100.2", "5"]])
    ev(9, 9, 8, a=[["100.3", "1"]])
    for kind, r in recs:
        w.write(kind, "BTCUSDT", r, r.get("id"))
    w.maybe_flush(force=True)
    w.close_all()
    return recs


def online_rows(recs):
    """模拟在线：同一状态机顺序处理，得到"在线"结果集。"""
    ob = OrderBook("BTCUSDT", SPEC, 20)
    ob.start_sync("x")
    out = {}
    for kind, r in recs:
        if kind == "depth":
            data = json.loads(r["raw"])["data"]
            outs = ob.on_event(DepthEvent.from_payload(data, r["recv_time_ns"], 0, 0, "c1"))
        else:
            body = json.loads(r["raw"])
            outs = ob.on_snapshot(DepthSnapshot.from_payload("BTCUSDT", body, 1000, 0, r["recv_time_ns"], 0, "r"))
        for o in outs:
            if o.kind == BookOutputKind.ROW:
                out[(o.row.book_epoch, o.row.u)] = {"bid_px": o.row.bid_px, "bid_qty": o.row.bid_qty,
                                                    "ask_px": o.row.ask_px, "ask_qty": o.row.ask_qty, "session_id": "s1"}
        if ob.state.value == "INVALID":
            ob.start_sync("resync")
    return out


def test_replay_matches_online_and_is_deterministic(tmp_path):
    recs = build_dataset(tmp_path)
    online = online_rows(recs)
    assert sorted(online) == [(1, 2), (1, 3), (1, 4), (1, 5), (2, 8), (2, 9)]   # u=2 满足 U<=last<=u，是第一条
    assert online[(1, 5)]["bid_px"][0] == "100.00" and online[(1, 5)]["bid_qty"][0] == "0.500"
    items = load_inputs(tmp_path, "BTCUSDT", DAY)
    assert len(items) == 10
    rows1, stats1 = replay_rows(items, "BTCUSDT", SPEC, 20, 1000, 5000, 1000)
    rows2, _ = replay_rows(items, "BTCUSDT", SPEC, 20, 1000, 5000, 1000)
    assert rows1 == rows2
    assert stats1["invalid_pu_mismatch"] == 1 and stats1["final_epoch"] == 2
    cmp = compare(rows1, online)
    assert cmp["ok"] and cmp["matched"] == 6 and cmp["only_in_replay"] == 0 and cmp["only_in_online"] == 0


def test_compare_reports_mismatch_and_missing():
    rep = [{"book_epoch": 1, "u": 1, "bid_px": ["1"], "bid_qty": ["1"], "ask_px": [], "ask_qty": []},
           {"book_epoch": 1, "u": 2, "bid_px": ["1"], "bid_qty": ["1"], "ask_px": [], "ask_qty": []}]
    online = {(1, 1): {"bid_px": ["1"], "bid_qty": ["2"], "ask_px": [], "ask_qty": []},
              (1, 3): {"bid_px": [], "bid_qty": [], "ask_px": [], "ask_qty": []}}
    c = compare(rep, online)
    assert not c["ok"] and c["mismatched"] == 1 and c["only_in_replay"] == 1 and c["only_in_online"] == 1
    assert c["mismatch_examples"][0]["field"] == "bid_qty"
