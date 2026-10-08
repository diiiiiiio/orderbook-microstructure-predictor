"""按日期校验：raw 分片 manifest/SHA-256/行数、未关闭分片、normalized 可读性与去重。"""
from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from data.collector.storage.raw import manifest_path, verify_manifest


def verify_day(data_dir: Path, day: str, symbol: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"day": day, "raw": [], "normalized": {}, "ok": True}
    raw_root = data_dir / "raw"
    for p in sorted(raw_root.rglob(f"*/{day}/*.jsonl*")) if raw_root.exists() else []:
        if p.name.endswith(".manifest.json") or p.name.endswith(".tmp") or p.name.endswith(".corrupt"):
            continue
        if symbol and symbol not in p.parts:
            continue
        if not manifest_path(p).exists():
            out["raw"].append({"path": str(p), "ok": None, "note": "活动分片或未恢复分片（无 manifest）"})
            continue
        v = verify_manifest(p)
        out["raw"].append(v)
        out["ok"] &= bool(v["ok"])
    norm_root = data_dir / "normalized"
    for table in ("book_top20", "agg_trades", "mark_price", "gaps", "quality_events"):
        rows = 0
        files = 0
        dup_info: dict[str, Any] = {}
        keys: Counter = Counter()
        for p in sorted(norm_root.glob(f"{table}/*/{day}/*.parquet")) if norm_root.exists() else []:
            if symbol and symbol not in p.parts:
                continue
            try:
                t = pq.read_table(p)
            except Exception as exc:              # noqa: BLE001
                out["normalized"].setdefault(table, {}).setdefault("errors", []).append({"path": str(p), "error": str(exc)})
                out["ok"] = False
                continue
            rows += t.num_rows
            files += 1
            if table == "agg_trades":
                for s, a in zip(t.column("symbol").to_pylist(), t.column("a").to_pylist()):
                    keys[(s, a)] += 1
            elif table == "book_top20":
                for s, e, u in zip(t.column("symbol").to_pylist(), t.column("book_epoch").to_pylist(), t.column("u").to_pylist()):
                    keys[(s, e, u)] += 1
        if keys:
            dups = sum(1 for k, n in keys.items() if n > 1)
            dup_info = {"unique_keys": len(keys), "duplicate_keys": dups}
            if dups:
                out["ok"] = False
        out["normalized"][table] = {"files": files, "rows": rows, **dup_info}
    return out
