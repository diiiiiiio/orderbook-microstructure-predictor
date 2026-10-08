"""离线回放：用 raw 层的深度增量 + REST 快照重建盘口，与在线 book_top20 在 (book_epoch, u) 上精确比较。

回放输入按本机接收时间排序，重放与在线相同的状态机（同一份 OrderBook 代码）。
判定口径：同 (symbol, book_epoch, u) 的前 N 档价格与数量必须完全一致；
在线有而回放没有、回放有而在线没有的键都报告出来。
同一原始数据重复回放结果确定（无时钟、无随机）。
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from data.collector.config import CollectorConfig
from data.collector.numeric import SymbolSpec
from data.collector.orderbook import OrderBook
from data.collector.records import BookOutputKind, BookState, DepthEvent, DepthSnapshot
from data.collector.storage.raw import iter_jsonl


def _raw_files(root: Path, kind: str, symbol: str, day: str) -> list[Path]:
    d = root / "raw" / kind / symbol / day
    if not d.exists():
        return []
    return sorted(p for p in d.iterdir() if p.suffix in (".jsonl", ".gz") and not p.name.endswith(".tmp"))


def load_inputs(root: Path, symbol: str, day: str) -> list[tuple[int, str, dict[str, Any]]]:
    """返回 (recv_time_ns, kind, rec) 列表，kind ∈ {depth, snapshot}，按接收时间排序。"""
    items: list[tuple[int, str, dict[str, Any]]] = []
    for p in _raw_files(root, "depth", symbol, day):
        for rec in iter_jsonl(p):
            items.append((rec["recv_time_ns"], "depth", rec))
    for p in _raw_files(root, "rest", symbol, day):
        for rec in iter_jsonl(p):
            if rec.get("endpoint") == "/fapi/v1/depth" and rec.get("status") == 200:
                items.append((rec["recv_time_ns"], "snapshot", rec))
    items.sort(key=lambda x: (x[0], 0 if x[1] == "snapshot" else 1))
    return items


def load_spec(root: Path, symbol: str, cfg: CollectorConfig) -> SymbolSpec:
    spec_dir = root / "state" / "specs"
    files = sorted(spec_dir.glob("exchangeInfo_*.json"), key=lambda p: p.stat().st_mtime) if spec_dir.exists() else []
    if not files:
        raise FileNotFoundError("缺少 state/specs/exchangeInfo_*.json，无法确定 tickSize/stepSize")
    info = json.loads(files[-1].read_text(encoding="utf-8"))
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            return SymbolSpec.from_exchange_info_symbol(s)
    raise KeyError(symbol)


def replay_rows(items: list[tuple[int, str, dict[str, Any]]], symbol: str, spec: SymbolSpec,
                levels: int, buffer_max: int, stale_after_ms: int, depth_limit: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ob = OrderBook(symbol, spec, levels, buffer_max, stale_after_ms)
    ob.start_sync("replay")
    rows: list[dict[str, Any]] = []
    stats: dict[str, int] = defaultdict(int)
    for recv_ns, kind, rec in items:
        if kind == "depth":
            msg = json.loads(rec["raw"])
            data = msg.get("data", msg)
            try:
                evt = DepthEvent.from_payload(data, rec["recv_time_ns"], rec.get("recv_monotonic_ns", 0),
                                              rec.get("recv_seq", 0), rec.get("connection_id", ""))
            except (ValueError, KeyError, TypeError):
                stats["parse_errors"] += 1
                continue
            outs = ob.on_event(evt)
            ob.check_stale(recv_ns)
        else:
            if ob.state != BookState.SYNCING:
                stats["snapshots_ignored"] += 1
                continue
            body = json.loads(rec["raw"])
            try:
                snap = DepthSnapshot.from_payload(symbol, body, int(rec.get("params", {}).get("limit", depth_limit)),
                                                  rec.get("sent_time_ns", 0), rec["recv_time_ns"],
                                                  rec.get("recv_monotonic_ns", 0), rec.get("request_id", ""))
            except (ValueError, KeyError):
                stats["snapshot_parse_errors"] += 1
                continue
            outs = ob.on_snapshot(snap)
            stats["snapshots_used"] += 1
        for o in outs:
            if o.kind == BookOutputKind.ROW and o.row is not None:
                r = o.row
                rows.append({"symbol": symbol, "book_epoch": r.book_epoch, "u": r.u, "E_ms": r.E,
                             "recv_time_ns": r.recv_time_ns, "bid_px": r.bid_px, "bid_qty": r.bid_qty,
                             "ask_px": r.ask_px, "ask_qty": r.ask_qty})
            elif o.kind == BookOutputKind.STATE and o.state == BookState.INVALID:
                stats[f"invalid_{o.reason}"] += 1
                ob.start_sync("replay_resync")
            elif o.kind == BookOutputKind.NEED_SNAPSHOT:
                stats[f"need_snapshot_{o.reason}"] += 1
        if ob.state == BookState.INVALID:
            ob.start_sync("replay_resync")
    stats["rows"] = len(rows)
    stats["final_epoch"] = ob.epoch
    return rows, dict(stats)


def load_online_rows(root: Path, symbol: str, day: str, levels: int,
                     session_id: str | None = None) -> dict[tuple[int, int], dict[str, Any]]:
    d = root / "normalized" / "book_top20" / symbol / day
    out: dict[tuple[int, int], dict[str, Any]] = {}
    if not d.exists():
        return out
    files = sorted(d.glob("*.parquet"))
    if not files:
        return out
    tbl = pa.concat_tables([pq.read_table(f) for f in files])
    for r in tbl.to_pylist():
        if not r["is_valid"]:
            continue
        if session_id is not None and r["session_id"] != session_id:
            continue
        key = (r["book_epoch"], r["u"])
        out[key] = {
            "bid_px": [r[f"bid_px_{i}"] for i in range(levels) if r[f"bid_px_{i}"] is not None],
            "bid_qty": [r[f"bid_qty_{i}"] for i in range(levels) if r[f"bid_qty_{i}"] is not None],
            "ask_px": [r[f"ask_px_{i}"] for i in range(levels) if r[f"ask_px_{i}"] is not None],
            "ask_qty": [r[f"ask_qty_{i}"] for i in range(levels) if r[f"ask_qty_{i}"] is not None],
            "session_id": r["session_id"],
        }
    return out


def compare(replayed: list[dict[str, Any]], online: dict[tuple[int, int], dict[str, Any]],
            max_examples: int = 10) -> dict[str, Any]:
    rep = {(r["book_epoch"], r["u"]): r for r in replayed}
    both = set(rep) & set(online)
    mismatches = []
    for k in sorted(both):
        a, b = rep[k], online[k]
        for f in ("bid_px", "bid_qty", "ask_px", "ask_qty"):
            if a[f] != b[f]:
                mismatches.append({"book_epoch": k[0], "u": k[1], "field": f, "replay": a[f][:3], "online": b[f][:3]})
                break
    only_rep = sorted(set(rep) - set(online))
    only_on = sorted(set(online) - set(rep))
    return {
        "compared": len(both), "matched": len(both) - len(mismatches), "mismatched": len(mismatches),
        "only_in_replay": len(only_rep), "only_in_online": len(only_on),
        "mismatch_examples": mismatches[:max_examples],
        "only_in_replay_examples": only_rep[:max_examples], "only_in_online_examples": only_on[:max_examples],
        "ok": len(mismatches) == 0 and len(both) > 0,
        "note": "键 = (book_epoch, u)。多会话/多次重同步会形成多个 epoch；跨 session 的 epoch 编号不可比，需按 session 分开看。",
    }


def replay_and_compare(cfg: CollectorConfig, symbol: str, day: str, write_parquet: str | None = None,
                       session_id: str | None = None) -> dict[str, Any]:
    """按 session 分别回放并比较（epoch 编号只在 session 内有意义）。"""
    root = cfg.data_path
    spec = load_spec(root, symbol, cfg)
    all_items = load_inputs(root, symbol, day)
    sessions = sorted({rec.get("session_id") for _, _, rec in all_items if rec.get("session_id")})
    if session_id is not None:
        sessions = [s for s in sessions if s == session_id]
    per_session: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    overall_ok = bool(sessions)
    deterministic_all = True
    for sid in sessions:
        items = [it for it in all_items if it[2].get("session_id") == sid]
        args = (items, symbol, spec, cfg.orderbook.export_levels, cfg.orderbook.buffer_max_events,
                cfg.orderbook.stale_after_ms, cfg.rest.depth_limit)
        rows, stats = replay_rows(*args)
        rows2, _ = replay_rows(*args)
        det = rows == rows2
        online = load_online_rows(root, symbol, day, cfg.orderbook.export_levels, sid)
        cmp = compare(rows, online)
        ok = cmp["ok"] and det
        overall_ok &= ok
        deterministic_all &= det
        for r in rows:
            r["session_id"] = sid
        all_rows.extend(rows)
        per_session.append({"session_id": sid, "inputs": len(items), "replay_stats": stats,
                            "online_valid_rows": len(online), "deterministic": det, "compare": cmp, "ok": ok})
    if write_parquet:
        levels = cfg.orderbook.export_levels
        flat = []
        for r in all_rows:
            d = {k: r[k] for k in ("symbol", "session_id", "book_epoch", "u", "E_ms", "recv_time_ns")}
            for side in ("bid", "ask"):
                for i in range(levels):
                    d[f"{side}_px_{i}"] = r[f"{side}_px"][i] if i < len(r[f"{side}_px"]) else None
                    d[f"{side}_qty_{i}"] = r[f"{side}_qty"][i] if i < len(r[f"{side}_qty"]) else None
            flat.append(d)
        pq.write_table(pa.Table.from_pylist(flat), write_parquet)
    totals = {
        "compared": sum(p["compare"]["compared"] for p in per_session),
        "matched": sum(p["compare"]["matched"] for p in per_session),
        "mismatched": sum(p["compare"]["mismatched"] for p in per_session),
        "only_in_replay": sum(p["compare"]["only_in_replay"] for p in per_session),
        "only_in_online": sum(p["compare"]["only_in_online"] for p in per_session),
    }
    return {"rows": all_rows, "summary": {"symbol": symbol, "day": day, "sessions": len(sessions),
                                          "inputs": len(all_items), "deterministic": deterministic_all,
                                          "totals": totals, "per_session": per_session, "ok": overall_ok,
                                          "note": "键 = (session_id, book_epoch, u)；无 session 或无可比较行时 ok=false"}}
