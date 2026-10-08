"""只读监控面板：aiohttp 提供 JSON API 与单页前端，读取 status.json、reports 与 Parquet 研究层。

    python -m data.collector -c deploy/config/collector.yaml dashboard --port 8787
默认只监听 127.0.0.1；远程查看用 ssh -L 8787:127.0.0.1:8787。不写任何数据。
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from aiohttp import web

from data.collector.config import CollectorConfig
from data.collector.monitor import disk_free_gb

log = logging.getLogger("collector.dashboard")
STATIC = Path(__file__).resolve().parent / "static"
TABLES = ("book_top20", "agg_trades", "mark_price", "gaps", "quality_events")
TIME_COL = {"book_top20": "recv_time_ns", "agg_trades": "known_time_ns", "mark_price": "recv_time_ns",
            "gaps": "detected_time_ns", "quality_events": "time_ns"}


def _json(obj: Any, status: int = 200) -> web.Response:
    return web.Response(text=json.dumps(obj, ensure_ascii=False, default=str), status=status,
                        content_type="application/json")


class ParquetIndex:
    """按 (path, mtime, size) 缓存 parquet 页脚信息：行数、时间范围。只读页脚与列统计，不读数据。"""

    def __init__(self, root: Path):
        self.root = root
        self._cache: dict[str, tuple[tuple[float, int], dict[str, Any]]] = {}

    def file_info(self, p: Path, time_col: str) -> dict[str, Any]:
        st = p.stat()
        key = (st.st_mtime, st.st_size)
        hit = self._cache.get(str(p))
        if hit and hit[0] == key:
            return hit[1]
        info: dict[str, Any] = {"rows": 0, "size": st.st_size, "t_min": None, "t_max": None}
        try:
            md = pq.ParquetFile(p).metadata
            info["rows"] = md.num_rows
            idx = md.schema.to_arrow_schema().get_field_index(time_col)
            if idx >= 0:
                for rg in range(md.num_row_groups):
                    s = md.row_group(rg).column(idx).statistics
                    if s and s.has_min_max and s.min is not None:
                        info["t_min"] = s.min if info["t_min"] is None else min(info["t_min"], s.min)
                        info["t_max"] = s.max if info["t_max"] is None else max(info["t_max"], s.max)
        except Exception as exc:                  # noqa: BLE001
            info["error"] = str(exc)
        self._cache[str(p)] = (key, info)
        return info

    def inventory(self) -> dict[str, Any]:
        out: dict[str, Any] = {"tables": {}, "days": set()}
        norm = self.root / "normalized"
        for table in TABLES:
            tdir = norm / table
            per_sym: dict[str, dict[str, Any]] = {}
            if tdir.exists():
                for sym_dir in sorted(p for p in tdir.iterdir() if p.is_dir()):
                    days: dict[str, Any] = {}
                    for day_dir in sorted(p for p in sym_dir.iterdir() if p.is_dir()):
                        files = [f for f in day_dir.iterdir() if f.suffix == ".parquet"]
                        agg = {"files": len(files), "rows": 0, "size": 0, "t_min": None, "t_max": None, "errors": 0}
                        for f in files:
                            i = self.file_info(f, TIME_COL[table])
                            agg["rows"] += i["rows"]
                            agg["size"] += i["size"]
                            agg["errors"] += 1 if i.get("error") else 0
                            for k, fn in (("t_min", min), ("t_max", max)):
                                if i[k] is not None:
                                    agg[k] = i[k] if agg[k] is None else fn(agg[k], i[k])
                        days[day_dir.name] = agg
                        out["days"].add(day_dir.name)
                    per_sym[sym_dir.name] = {"days": days, "rows": sum(d["rows"] for d in days.values()),
                                             "files": sum(d["files"] for d in days.values()),
                                             "size": sum(d["size"] for d in days.values())}
            out["tables"][table] = per_sym
        raw_root = self.root / "raw"
        raw: dict[str, Any] = {}
        if raw_root.exists():
            for kind_dir in sorted(p for p in raw_root.iterdir() if p.is_dir()):
                files = active = size = 0
                for f in kind_dir.rglob("*"):
                    if f.is_file() and (f.suffix in (".jsonl", ".gz")):
                        files += 1
                        size += f.stat().st_size
                        if f.suffix == ".jsonl":
                            active += 1
                raw[kind_dir.name] = {"files": files, "active_uncompressed": active, "size": size}
        out["raw"] = raw
        out["days"] = sorted(out["days"])
        return out

    def latest_files(self, table: str, symbol: str, n: int) -> list[Path]:
        tdir = self.root / "normalized" / table / symbol
        if not tdir.exists():
            return []
        files: list[Path] = []
        for day_dir in sorted((p for p in tdir.iterdir() if p.is_dir()), reverse=True):
            files = sorted(day_dir.glob("*.parquet"))[-n:] + files
            if len(files) >= n:
                break
        return files[-n:]

    def day_files(self, table: str, symbol: str, day: str) -> list[Path]:
        d = self.root / "normalized" / table / symbol / day
        return sorted(d.glob("*.parquet")) if d.exists() else []


def read_columns(files: list[Path], columns: list[str] | None) -> pa.Table | None:
    tabs = []
    for f in files:
        try:
            tabs.append(pq.read_table(f, columns=columns))
        except Exception as exc:                  # noqa: BLE001
            log.warning("读取失败 %s: %s", f, exc)
    if not tabs:
        return None
    return pa.concat_tables(tabs, promote_options="default")


class Dashboard:
    def __init__(self, cfg: CollectorConfig):
        self.cfg = cfg
        self.root = cfg.data_path
        self.index = ParquetIndex(self.root)
        self._inv_cache: tuple[float, dict[str, Any]] | None = None

    # ---------- handlers ----------
    async def index_html(self, _req: web.Request) -> web.Response:
        return web.FileResponse(STATIC / "index.html")

    async def api_status(self, _req: web.Request) -> web.Response:
        p = self.root / "state" / "status.json"
        if not p.exists():
            return _json({"exists": False, "note": "status.json 不存在：采集器从未运行或 data_dir 不对"})
        try:
            st = json.loads(p.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return _json({"exists": True, "error": f"status.json 解析失败: {exc}"})
        age_s = (time.time_ns() - st.get("time_ns", 0)) / 1e9
        # 采集器是否还活着：status 每 status_interval_s 刷新
        st["_meta"] = {"exists": True, "age_s": round(age_s, 1),
                       "collector_alive": age_s < self.cfg.monitoring.status_interval_s * 4,
                       "data_dir": str(self.root), "disk_free_gb": round(disk_free_gb(self.root), 2)}
        cp = self.root / "state" / "checkpoint.json"
        if cp.exists():
            try:
                st["_checkpoint"] = json.loads(cp.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                st["_checkpoint"] = {"error": "unreadable"}
        return _json(st)

    async def api_inventory(self, _req: web.Request) -> web.Response:
        now = time.monotonic()
        if self._inv_cache and now - self._inv_cache[0] < 10:
            return _json(self._inv_cache[1])
        inv = await asyncio.get_running_loop().run_in_executor(None, self.index.inventory)
        inv["symbols"] = self.cfg.symbols
        self._inv_cache = (now, inv)
        return _json(inv)

    async def api_gaps(self, req: web.Request) -> web.Response:
        day = req.query.get("day")
        limit = int(req.query.get("limit", "300"))
        files: list[Path] = []
        for sym_dir in (self.root / "normalized" / "gaps").glob("*"):
            if day:
                files += self.index.day_files("gaps", sym_dir.name, day)
            else:
                files += self.index.latest_files("gaps", sym_dir.name, 50)
        t = await asyncio.get_running_loop().run_in_executor(None, read_columns, files, None)
        rows = t.to_pylist() if t is not None else []
        latest: dict[str, dict[str, Any]] = {}
        for r in rows:
            g = latest.get(r["gap_id"])
            if g is None or (r["update_time_ns"] or 0) > (g["update_time_ns"] or 0):
                latest[r["gap_id"]] = r
        out = sorted(latest.values(), key=lambda r: r["detected_time_ns"] or 0, reverse=True)[:limit]
        summary: dict[str, int] = defaultdict(int)
        for r in latest.values():
            summary[f"{r['repair_status']}"] += 1
            summary[f"kind:{r['kind']}"] += 1
        return _json({"gaps": out, "summary": dict(summary), "total": len(latest)})

    async def api_quality(self, req: web.Request) -> web.Response:
        limit = int(req.query.get("limit", "200"))
        sev = req.query.get("severity")
        files: list[Path] = []
        for sym_dir in (self.root / "normalized" / "quality_events").glob("*"):
            files += self.index.latest_files("quality_events", sym_dir.name, 20)
        t = await asyncio.get_running_loop().run_in_executor(None, read_columns, files, None)
        rows = t.to_pylist() if t is not None else []
        if sev:
            rows = [r for r in rows if r["severity"] == sev]
        rows.sort(key=lambda r: r["time_ns"], reverse=True)
        return _json({"events": rows[:limit]})

    async def api_book(self, req: web.Request) -> web.Response:
        sym = req.query.get("symbol", self.cfg.symbols[0]).upper()
        levels = self.cfg.orderbook.export_levels
        files = self.index.latest_files("book_top20", sym, 1)
        if not files:
            return _json({"symbol": sym, "row": None})
        t = await asyncio.get_running_loop().run_in_executor(None, read_columns, files, None)
        if t is None or t.num_rows == 0:
            return _json({"symbol": sym, "row": None})
        r = t.slice(t.num_rows - 1, 1).to_pylist()[0]
        book = {"bids": [(r[f"bid_px_{i}"], r[f"bid_qty_{i}"]) for i in range(levels) if r[f"bid_px_{i}"]],
                "asks": [(r[f"ask_px_{i}"], r[f"ask_qty_{i}"]) for i in range(levels) if r[f"ask_px_{i}"]]}
        meta = {k: r[k] for k in ("book_epoch", "u", "E_ms", "recv_time_ns", "state", "is_valid", "flags",
                                  "n_bid_levels_known", "n_ask_levels_known", "session_id", "connection_id")}
        return _json({"symbol": sym, "row": meta, "book": book, "file": str(files[-1])})

    async def api_trades(self, req: web.Request) -> web.Response:
        sym = req.query.get("symbol", self.cfg.symbols[0]).upper()
        n = int(req.query.get("n", "50"))
        files = self.index.latest_files("agg_trades", sym, 2)
        t = await asyncio.get_running_loop().run_in_executor(None, read_columns, files,
                                                               ["a", "p", "q", "nq", "T_ms", "E_ms", "m", "aggressor_side", "source", "recv_time_ns", "known_time_ns"])
        if t is None:
            return _json({"symbol": sym, "trades": []})
        rows = sorted(t.to_pylist(), key=lambda r: r["a"])[-n:]
        return _json({"symbol": sym, "trades": rows[::-1]})

    async def api_series(self, req: web.Request) -> web.Response:
        """中间价/价差时间序列（最近 n 个 parquet 文件，或指定日期），按 recv_time 排序，等距抽样到 max_points。"""
        sym = req.query.get("symbol", self.cfg.symbols[0]).upper()
        day = req.query.get("day")
        max_points = int(req.query.get("max_points", "1500"))
        files = self.index.day_files("book_top20", sym, day) if day else self.index.latest_files("book_top20", sym, 12)
        cols = ["recv_time_ns", "E_ms", "u", "book_epoch", "bid_px_0", "ask_px_0", "bid_qty_0", "ask_qty_0"]
        t = await asyncio.get_running_loop().run_in_executor(None, read_columns, files, cols)
        if t is None or t.num_rows == 0:
            return _json({"symbol": sym, "points": [], "files": len(files)})
        t = t.sort_by("recv_time_ns")
        step = max(1, t.num_rows // max_points)
        rows = t.to_pylist()[::step]
        pts = []
        for r in rows:
            if r["bid_px_0"] is None or r["ask_px_0"] is None:
                continue
            b, a = float(r["bid_px_0"]), float(r["ask_px_0"])
            pts.append({"t": r["recv_time_ns"] // 1_000_000, "mid": (a + b) / 2, "spread": a - b,
                        "epoch": r["book_epoch"], "u": r["u"]})
        return _json({"symbol": sym, "points": pts, "rows_total": t.num_rows, "files": len(files), "step": step})

    async def api_hourly(self, req: web.Request) -> web.Response:
        """指定日期每小时行数（按表、按交易对），用行组统计近似：直接读时间列计数。"""
        day = req.query.get("day") or datetime.now(timezone.utc).strftime("%Y%m%d")
        sym = req.query.get("symbol", self.cfg.symbols[0]).upper()
        out: dict[str, list[int]] = {}
        loop = asyncio.get_running_loop()
        for table in ("book_top20", "agg_trades", "mark_price"):
            files = self.index.day_files(table, sym, day)
            t = await loop.run_in_executor(None, read_columns, files, [TIME_COL[table]])
            counts = [0] * 24
            if t is not None:
                for v in t.column(TIME_COL[table]).to_pylist():
                    if v is not None:
                        counts[datetime.fromtimestamp(v / 1e9, tz=timezone.utc).hour] += 1
            out[table] = counts
        return _json({"day": day, "symbol": sym, "hourly": out})

    async def api_reports(self, _req: web.Request) -> web.Response:
        rd = self.root / "reports"
        items = []
        if rd.exists():
            for p in sorted(rd.glob("*.md"), reverse=True)[:60]:
                items.append({"day": p.stem, "md": p.name, "json": p.with_suffix(".json").name,
                              "mtime_ns": int(p.stat().st_mtime * 1e9)})
        return _json({"reports": items})

    async def api_report_md(self, req: web.Request) -> web.Response:
        day = req.match_info["day"]
        if not day.isdigit():
            raise web.HTTPBadRequest()
        p = self.root / "reports" / f"{day}.md"
        if not p.exists():
            raise web.HTTPNotFound()
        return web.Response(text=p.read_text(encoding="utf-8"), content_type="text/plain", charset="utf-8")

    # ---------- 回测（读 data/store/reports/backtest/*.json；运行请用 python -m backtest.run） ----------
    def _bt_dir(self) -> Path:
        return self.root / "reports" / "backtest"

    async def api_bt_list(self, _req: web.Request) -> web.Response:
        d = self._bt_dir()
        if not d.is_dir():
            return _json({"reports": [], "note": "还没有回测报告：python -m backtest.run BTCUSDT <start> <end>"})
        out = []
        for f in sorted(d.glob("*.json"), key=lambda q: q.stat().st_mtime, reverse=True):
            st = f.stat()
            out.append({"name": f.stem, "size": st.st_size,
                        "mtime": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(timespec="seconds")})
        return _json({"reports": out})

    async def api_bt_report(self, req: web.Request) -> web.Response:
        name = req.match_info["name"]
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
            raise web.HTTPBadRequest()
        f = self._bt_dir() / f"{name}.json"
        if not f.exists():
            raise web.HTTPNotFound()
        return web.Response(body=f.read_bytes(), content_type="application/json", charset="utf-8")

    async def bt_html(self, _req: web.Request) -> web.Response:
        return web.FileResponse(STATIC / "backtest.html")

    def app(self) -> web.Application:
        app = web.Application()
        app.add_routes([
            web.get("/", self.index_html),
            web.get("/api/status", self.api_status),
            web.get("/api/inventory", self.api_inventory),
            web.get("/api/gaps", self.api_gaps),
            web.get("/api/quality", self.api_quality),
            web.get("/api/book", self.api_book),
            web.get("/api/trades", self.api_trades),
            web.get("/api/series", self.api_series),
            web.get("/api/hourly", self.api_hourly),
            web.get("/api/reports", self.api_reports),
            web.get("/api/report/{day}", self.api_report_md),
            web.get("/backtest", self.bt_html),
            web.get("/api/bt/list", self.api_bt_list),
            web.get("/api/bt/report/{name}", self.api_bt_report),
        ])
        return app


def run_dashboard(cfg: CollectorConfig, host: str, port: int) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    d = Dashboard(cfg)
    log.info("监控面板 http://%s:%d  数据目录 %s", host, port, cfg.data_path)
    web.run_app(d.app(), host=host, port=port, print=None)
