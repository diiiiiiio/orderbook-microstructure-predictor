"""编排器：会话、连接、分发、盘口/成交/标记价格处理、写线程、状态、checkpoint、优雅停止。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import platform
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data.collector import SCHEMA_VERSION
from data.collector.backfill import TradeBackfiller
from data.collector.clock import ClockMonitor, mono_ns, now_ns
from data.collector.config import CollectorConfig, config_to_dict
from data.collector.depth import DepthPipeline
from data.collector.markprice import MarkPriceProcessor
from data.collector.monitor import atomic_write_json, dir_size_bytes, disk_free_gb, render_markdown
from data.collector.numeric import PrecisionError, SymbolSpec, validate_usdt_perpetual
from data.collector.quality import QualityRegistry
from data.collector.records import BookRow, BookState, DepthSnapshot, GapRecord, QualityEvent
from data.collector.rest import Forbidden, RestClient
from data.collector.sdnotify import SdNotify
from data.collector.storage.checkpoint import Checkpoint
from data.collector.storage.normalized import NormalizedWriter
from data.collector.storage.raw import RawWriter, manifest_path, recover_open_shard
from data.collector.trades import TradeProcessor
from data.collector.writer import WriterThread
from data.collector.ws import ConnectionManager, RawMessage

log = logging.getLogger("collector")


def new_session_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


class Collector:
    def __init__(self, cfg: CollectorConfig, run_seconds: float | None = None):
        self.cfg = cfg
        self.run_seconds = run_seconds
        self.session_id = new_session_id()
        self.data_dir = cfg.data_path
        self.state_dir = self.data_dir / "state"
        self.reports_dir = self.data_dir / "reports"
        self.started_ns = now_ns()
        self.started_mono = time.monotonic()
        self.stopping = asyncio.Event()
        self.stop_reason = ""
        self.health_reasons: list[str] = []
        self.rest_forbidden: str | None = None
        self.specs: dict[str, SymbolSpec] = {}
        self.spec_versions: list[dict[str, Any]] = []
        self.clock = ClockMonitor()
        self.checkpoint = Checkpoint(self.state_dir / "checkpoint.json")
        self.ws_queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.storage.queue_max)
        self.ws_queue_overflow = 0
        # 写线程
        raw = RawWriter(self.data_dir, self.session_id, SCHEMA_VERSION, cfg.storage.raw_shard_max_bytes,
                        cfg.storage.raw_shard_max_seconds, cfg.storage.raw_flush_interval_s,
                        cfg.storage.raw_fsync_interval_s, cfg.storage.compress_closed_shards,
                        {"exchange": cfg.exchange, "market": cfg.market})
        norm = NormalizedWriter(self.data_dir, self.session_id, cfg.orderbook.export_levels, SCHEMA_VERSION,
                                cfg.storage.parquet_batch_rows, cfg.storage.parquet_flush_interval_s,
                                cfg.storage.parquet_compression)
        raw.inject_error_after = cfg.storage.inject_write_error_after_records
        self.writer = WriterThread(raw, norm, cfg.storage.queue_max)
        self.q = QualityRegistry(self.session_id, self._on_gap, self._on_quality, cfg.monitoring.latency_window)
        self.rest = RestClient(cfg.endpoints.rest_base, cfg.rest, cfg.proxy, self._rest_raw_sink, self.session_id)
        self.depth: dict[str, DepthPipeline] = {}
        self.trades: dict[str, TradeProcessor] = {}
        self.marks: dict[str, MarkPriceProcessor] = {}
        self.managers: dict[str, ConnectionManager] = {}
        self.backfiller: TradeBackfiller | None = None
        self.tasks: list[asyncio.Task] = []
        self.recovered_shards: list[dict[str, Any]] = []
        self.messages_dispatched = 0
        self.last_status_ns = 0
        self.dispatch_lag_ns = 0
        self.sd = SdNotify()

    # ---------- 写入回调 ----------
    def _put(self, op: tuple) -> None:
        if not self.writer.put(op):
            self._note_write_overflow(op)

    def _note_write_overflow(self, op: tuple) -> None:
        # 写队列满：确定性丢失，记录（记录本身也可能失败，所以同时记到内存）
        self.health_reasons.append("writer_queue_overflow")
        sym = "_"
        if op[0] == "raw":
            sym = op[2]
        if not hasattr(self, "_overflow_gap_open") or not self._overflow_gap_open:
            self._overflow_gap_open = True
            self.q.open_gap(sym, "all", "queue_overflow", "certain", "writer queue full",
                            start_time_ns=now_ns(), start_exchange_time_ms=None, prev_known_id=None,
                            time_basis="本机时间", repair_status="unrepairable",
                            note="写队列溢出，期间投递失败的记录已丢失；见 writer overflow_total")

    def _on_gap(self, g: GapRecord) -> None:
        self._put(("gap", g))
        self._event_log("gap", g.to_dict())

    def _on_quality(self, e: QualityEvent) -> None:
        self._put(("qe", e))
        if e.severity != "info" or e.event_type.startswith("state_"):
            self._event_log("quality", e.to_dict())

    def _event_log(self, kind: str, payload: dict[str, Any]) -> None:
        rec = {"schema_version": SCHEMA_VERSION, "exchange": self.cfg.exchange, "market": self.cfg.market,
               "symbol": payload.get("symbol", "_"), "stream": kind, "source": "collector",
               "session_id": self.session_id, "recv_time_ns": now_ns(), "recv_monotonic_ns": mono_ns(),
               "raw": json.dumps(payload, ensure_ascii=False, default=str)}
        self._put(("raw", "events", "_", rec, None))

    def _ws_event(self, kind: str, detail: dict[str, Any]) -> None:
        self._event_log("ws_" + kind if not kind.startswith("ws_") else kind, dict(detail, kind=kind))
        sev = "info" if kind in ("ws_open", "ws_rotation_start", "ws_rotation_done") else "warning"
        self.q.event("_", "connection", kind, sev, detail.get("connection_id"), **{k: v for k, v in detail.items() if k != "connection_id"})
        if kind == "queue_overflow":
            self.ws_queue_overflow += 1
            self.health_reasons.append("ws_queue_overflow")
            self.q.open_gap("_", "all", "queue_overflow", "certain", "ws receive queue full",
                            start_time_ns=detail.get("recv_time_ns"), start_exchange_time_ms=None, prev_known_id=None,
                            time_basis="本机接收时间", connection_id=detail.get("connection_id"), repair_status="unrepairable",
                            note="接收队列满，该消息未进入处理；盘口层随后会因 pu 断档进入 INVALID 并重同步")

    def _rest_raw_sink(self, kind: str, rec: dict[str, Any]) -> None:
        sym = str(rec.get("params", {}).get("symbol", "_"))
        rec.update({"exchange": self.cfg.exchange, "market": self.cfg.market, "symbol": sym,
                    "stream": rec.get("endpoint", "rest")})
        self._put(("raw", "rest", sym, rec, None))

    def _book_row_sink(self, row: BookRow) -> None:
        self._put(("book", row))

    # ---------- 启动 ----------
    async def _fetch_snapshot(self, symbol: str, limit: int) -> DepthSnapshot | None:
        try:
            rr = await self.rest.depth(symbol, limit)
        except Forbidden as exc:
            self.rest_forbidden = str(exc)
            self.q.event(symbol, "depth", "rest_forbidden", "error", error=str(exc))
            return None
        try:
            return DepthSnapshot.from_payload(symbol, rr.json(), limit, rr.sent_time_ns, rr.recv_time_ns,
                                              rr.recv_monotonic_ns, rr.request_id)
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            self.q.event(symbol, "depth", "snapshot_parse_error", "error", error=str(exc))
            return None

    def _recover_shards(self) -> None:
        raw_root = self.data_dir / "raw"
        if raw_root.exists():
            for p in raw_root.rglob("*.jsonl"):
                if manifest_path(p).exists():
                    continue
                try:
                    r = recover_open_shard(p)
                    self.recovered_shards.append(r)
                    log.warning("恢复未关闭分片 %s: %d 行, 截断 %d 字节", p, r["records"], r["truncated_bytes"])
                except Exception as exc:              # noqa: BLE001
                    log.exception("恢复分片失败 %s", p)
                    self.recovered_shards.append({"path": str(p), "error": str(exc)})

    def _recover_previous_run(self) -> None:
        prev = self.checkpoint.data
        if prev.get("session_id"):
            end_ns = prev.get("committed_time_ns")
            clean = prev.get("clean_shutdown", False)
            for sym in self.cfg.symbols:
                prev_a = prev.get("trades", {}).get(sym, {}).get("last_a")
                for stream in ("depth", "aggTrade", "markPrice"):
                    g = self.q.open_gap(sym, stream, "downtime", "certain",
                                    "process not running" + ("" if clean else " (previous run did not shut down cleanly)"),
                                    start_time_ns=end_ns, end_time_ns=self.started_ns, start_exchange_time_ms=None,
                                    prev_known_id=(prev_a if stream == "aggTrade" else None),
                                    time_basis="start = 上次 checkpoint 提交时间(本机)，实际停止可能更晚；end = 本次启动时间",
                                    repair_status="open" if (stream == "aggTrade" and prev_a is not None) else "unrepairable",
                                    note=f"prev_session={prev.get('session_id')}")
                    if stream == "aggTrade" and sym in self.trades:
                        self.trades[sym].downtime_gap = g
        for sym in self.cfg.symbols:
            t = prev.get("trades", {}).get(sym)
            if t and t.get("last_a") is not None and sym in self.trades:
                self.trades[sym].resume_from_checkpoint(t["last_a"], t.get("last_T_ms"), t.get("last_recv_ns"))

    async def _load_exchange_info(self, initial: bool) -> None:
        rr = await self.rest.exchange_info()
        info = rr.json()
        if info.get("futuresType") not in (None, "U_MARGINED"):
            raise PrecisionError(f"exchangeInfo futuresType={info.get('futuresType')!r} 不是 U 本位")
        version = hashlib.sha256(rr.text.encode()).hexdigest()[:16]
        by_symbol = {s["symbol"]: s for s in info.get("symbols", [])}
        changed = []
        for sym in self.cfg.symbols:
            if sym not in by_symbol:
                raise PrecisionError(f"{sym} 不在 exchangeInfo 中")
            spec = SymbolSpec.from_exchange_info_symbol(by_symbol[sym], version)
            validate_usdt_perpetual(spec)
            old = self.specs.get(sym)
            if old is not None and (old.tick_size != spec.tick_size or old.step_size != spec.step_size):
                changed.append(sym)
                self.q.event(sym, "depth", "spec_changed", "error", old_tick=str(old.tick_size),
                             new_tick=str(spec.tick_size), old_step=str(old.step_size), new_step=str(spec.step_size))
            self.specs[sym] = spec
        self.spec_versions.append({"version": version, "time_ns": rr.recv_time_ns,
                                   "symbols": {s: {"tickSize": str(self.specs[s].tick_size), "stepSize": str(self.specs[s].step_size)}
                                               for s in self.cfg.symbols}})
        spec_dir = self.state_dir / "specs"
        spec_dir.mkdir(parents=True, exist_ok=True)
        (spec_dir / f"exchangeInfo_{version}.json").write_text(rr.text, encoding="utf-8")
        for sym in changed:
            # 规格变了：旧定点数不再适用，重建盘口（新 epoch），不按旧规格截断
            if sym in self.depth:
                self.depth[sym].book.spec = self.specs[sym]
                self.depth[sym]._resync("spec_changed")

    async def _sync_time(self) -> None:
        try:
            sample, _ = await self.rest.server_time()
            self.clock.add_sample(sample)
            j = self.clock.check_jump()
            if j:
                self.q.event("_", "clock", "clock_jump", "error", **j)
        except Exception as exc:                      # noqa: BLE001
            self.q.event("_", "clock", "time_sync_error", "warning", error=str(exc))

    # ---------- 主循环 ----------
    async def run(self) -> int:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self._recover_shards()
        self.writer.start()
        (self.state_dir / f"config_{self.session_id}.json").write_text(
            json.dumps(config_to_dict(self.cfg), ensure_ascii=False, indent=1, default=str), encoding="utf-8")
        self._event_log("session_start", {"session_id": self.session_id, "pid": os.getpid(),
                                          "host": platform.node(), "python": platform.python_version(),
                                          "config": config_to_dict(self.cfg)})
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: self.request_stop(f"signal_{s.name}"))
            except NotImplementedError:
                pass
        rc = 0
        async with self.rest:
            try:
                await self._load_exchange_info(initial=True)
                await self._sync_time()
            except Forbidden as exc:
                log.error("REST 访问受限，无法启动: %s", exc)
                self._event_log("startup_failed", {"error": str(exc)})
                rc = 3
            except Exception as exc:                  # noqa: BLE001
                log.exception("启动失败")
                self._event_log("startup_failed", {"error": str(exc)})
                rc = 2
            if rc == 0:
                for sym in self.cfg.symbols:
                    self.trades[sym] = TradeProcessor(sym, self.q)
                    self.marks[sym] = MarkPriceProcessor(sym, self.q)
                    self.depth[sym] = DepthPipeline(sym, self.specs[sym], self.cfg.orderbook, self.q,
                                                    self._fetch_snapshot, self._book_row_sink, self.cfg.rest.depth_limit)
                self._recover_previous_run()
                self.managers["public"] = ConnectionManager("public", self.cfg.public_url(), self.cfg.ws, self.ws_queue,
                                                            self.cfg.proxy, self._ws_event, self.cfg.public_streams())
                if self.cfg.market_streams():
                    self.managers["market"] = ConnectionManager("market", self.cfg.market_url(), self.cfg.ws, self.ws_queue,
                                                                self.cfg.proxy, self._ws_event, self.cfg.market_streams())
                if self.cfg.backfill.enabled:
                    self.backfiller = TradeBackfiller(self.rest, self.cfg.backfill, self.trades,
                                                      lambda rows: self._put(("trades", rows)))
                for d in self.depth.values():
                    d.start()
                self.tasks = [asyncio.create_task(m.run(), name=f"mgr-{r}") for r, m in self.managers.items()]
                self.tasks.append(asyncio.create_task(self._dispatch(), name="dispatch"))
                self.tasks.append(asyncio.create_task(self._housekeeping(), name="housekeeping"))
                if self.backfiller:
                    self.tasks.append(asyncio.create_task(self.backfiller.run(), name="backfill"))
                if self.run_seconds:
                    self.tasks.append(asyncio.create_task(self._timer(), name="timer"))
                self.sd.ready()
                self.sd.status("starting: waiting for first depth events")
                await self.stopping.wait()
                self.sd.stopping()
                log.info("停止中: %s", self.stop_reason)
                await self._shutdown()
        return rc

    async def _timer(self) -> None:
        await asyncio.sleep(self.run_seconds or 0)
        self.request_stop("run_seconds_elapsed")

    def request_stop(self, reason: str) -> None:
        if not self.stopping.is_set():
            self.stop_reason = reason
            self.stopping.set()

    async def _dispatch(self) -> None:
        while not self.stopping.is_set():
            try:
                msg: RawMessage = await asyncio.wait_for(self.ws_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            self._handle(msg)

    def _handle(self, msg: RawMessage) -> None:
        self.messages_dispatched += 1
        self.dispatch_lag_ns = mono_ns() - msg.recv_monotonic_ns
        data = msg.data
        stream = msg.stream or ""
        sym = (data.get("s") if data else None) or (stream.split("@")[0].upper() if "@" in stream else "_unknown")
        if data is None:
            kind, id_value = "unparsed", None
        elif "@depth" in stream or data.get("e") == "depthUpdate":
            kind, id_value = "depth", data.get("u")
        elif "aggTrade" in stream or data.get("e") == "aggTrade":
            kind, id_value = "aggTrade", data.get("a")
        elif "markPrice" in stream or data.get("e") == "markPriceUpdate":
            kind, id_value = "markPrice", data.get("E")
        else:
            kind, id_value = "other", None
        rec = {"schema_version": SCHEMA_VERSION, "exchange": self.cfg.exchange, "market": self.cfg.market,
               "symbol": sym, "stream": stream or (data.get("e") if data else None), "source": "ws",
               "session_id": self.session_id, "connection_id": msg.connection_id, "recv_seq": msg.recv_seq,
               "recv_time_ns": msg.recv_time_ns, "recv_monotonic_ns": msg.recv_monotonic_ns,
               "id": id_value if isinstance(id_value, int) else None, "raw": msg.text}
        if msg.parse_error:
            rec["parse_error"] = msg.parse_error
        self._put(("raw", kind, sym, rec, rec["id"]))
        if data is None:
            self.q.c("_", "unparsed").parse_errors += 1
            self.q.event("_", "unparsed", "ws_parse_error", "error", msg.connection_id, error=msg.parse_error)
            return
        if kind == "depth" and sym in self.depth:
            self.depth[sym].on_message(data, msg.recv_time_ns, msg.recv_monotonic_ns, msg.recv_seq, msg.connection_id)
        elif kind == "aggTrade" and sym in self.trades:
            row = self.trades[sym].on_ws(data, msg.recv_time_ns, msg.recv_monotonic_ns, msg.recv_seq, msg.connection_id)
            if row:
                self._put(("trade", row))
        elif kind == "markPrice" and sym in self.marks:
            row = self.marks[sym].on_ws(data, msg.recv_time_ns, msg.recv_monotonic_ns, msg.recv_seq, msg.connection_id)
            if row:
                self._put(("mark", row))
        else:
            self.q.c(sym, "other").mark(msg.recv_time_ns)

    async def _housekeeping(self) -> None:
        last_time_sync = time.monotonic()
        last_info = time.monotonic()
        last_ckpt = time.monotonic()
        last_status = 0.0
        last_report_day = None
        while not self.stopping.is_set():
            await asyncio.sleep(1.0)
            now = time.monotonic()
            for d in self.depth.values():
                d.on_tick()
            self.clock.check_jump()
            if now - last_time_sync >= self.cfg.rest.time_sync_interval_s:
                last_time_sync = now
                asyncio.create_task(self._sync_time())
            if now - last_info >= self.cfg.rest.exchange_info_interval_s:
                last_info = now
                asyncio.create_task(self._refresh_info())
            if now - last_ckpt >= 10:
                last_ckpt = now
                self._commit_checkpoint(clean=False)
            if now - last_status >= self.cfg.monitoring.status_interval_s:
                last_status = now
                st = self.build_status()
                atomic_write_json(self.state_dir / "status.json", st)
                books = ",".join(f"{sym}:{d.book.state.value}" for sym, d in self.depth.items())
                self.sd.status(f"{st['health']['level']} {books} raw={self.writer.raw.records_written} "
                               f"fsync={self.writer.raw.records_fsynced} q={self.ws_queue.qsize()} "
                               f"reasons={','.join(st['health']['reasons'][:3])}")
                day = datetime.now(timezone.utc).strftime("%Y%m%d")
                if last_report_day is None:
                    last_report_day = day
                if day != last_report_day or int(now) % 300 < 1:
                    self._write_report(st, last_report_day if day != last_report_day else day)
                    last_report_day = day
            if self.writer.error and not self.stopping.is_set():
                self.health_reasons.append("writer_failed")
            # systemd watchdog：写线程活着且没失败才喂狗；否则让 systemd 重启
            if self.writer.is_alive() and not self.writer.error:
                self.sd.watchdog()

            free = disk_free_gb(self.data_dir)
            if free < self.cfg.monitoring.disk_fail_free_gb:
                self.health_reasons.append("disk_below_fail_threshold")
                if not self.stopping.is_set():
                    self.q.event("_", "disk", "disk_fail_threshold", "error", free_gb=free)
                    self.request_stop("disk_below_fail_threshold")

    async def _refresh_info(self) -> None:
        try:
            await self._load_exchange_info(initial=False)
        except Exception as exc:                      # noqa: BLE001
            self.q.event("_", "rest", "exchange_info_refresh_error", "warning", error=str(exc))

    def _commit_checkpoint(self, clean: bool) -> None:
        # 只提交已 fsync 的位置：先请求 writer 强制 flush，再读取
        self._put(("flush",))
        pos = self.writer.raw.fsynced_positions()
        trades = {}
        for sym, tp in self.trades.items():
            key = f"aggTrade/{sym}"
            p = pos.get(key)
            prev = self.checkpoint.get("trades", {}).get(sym, {})
            # raw 层已 fsync 的最后 a；本次还没 fsync 过则沿用上次 checkpoint 的值
            last_a = p["last_id"] if p and p.get("last_id") is not None else prev.get("last_a")
            trades[sym] = {"last_a": last_a, "last_recv_ns": p["last_recv_time_ns"] if p else prev.get("last_recv_ns"),
                           "last_T_ms": tp.last_T_ms if (p and p.get("last_id") == tp.last_a) else prev.get("last_T_ms")}
        self.checkpoint.commit({
            "session_id": self.session_id, "clean_shutdown": clean, "fsynced_positions": pos, "trades": trades,
            "book_epochs": {s: d.book.epoch for s, d in self.depth.items()},
            "note": "位置仅代表已 fsync 到磁盘的记录；入队/已写未 fsync 的不算",
        })

    def _write_report(self, st: dict[str, Any], day: str) -> None:
        st = dict(st, report_day=day)
        atomic_write_json(self.reports_dir / f"{day}.json", st)
        (self.reports_dir / f"{day}.md").write_text(render_markdown(st), encoding="utf-8")

    def build_status(self) -> dict[str, Any]:
        reasons = sorted(set(self.health_reasons[-50:]))
        wstatus = self.writer.status()
        free = disk_free_gb(self.data_dir)
        if self.rest_forbidden:
            reasons.append("rest_forbidden")
        for sym, d in self.depth.items():
            if d.book.state != BookState.LIVE:
                reasons.append(f"book_{sym}_{d.book.state.value}")
        for role, m in self.managers.items():
            if m.state != "connected":
                reasons.append(f"conn_{role}_{m.state}")
        if free < self.cfg.monitoring.disk_warn_free_gb:
            reasons.append("disk_below_warn_threshold")
        if not wstatus["alive"] and not self.stopping.is_set():
            reasons.append("writer_thread_dead")
        level = "ok"
        if wstatus["error"] or "disk_below_fail_threshold" in reasons or "writer_thread_dead" in reasons:
            level = "failed"
        elif reasons:
            level = "degraded"
        symbols: dict[str, Any] = {}
        for sym in self.cfg.symbols:
            symbols[sym] = {
                "book": self.depth[sym].status() if sym in self.depth else None,
                "depth": self.q.summary(sym, "depth"),
                "aggTrade": self.q.summary(sym, "aggTrade"),
                "markPrice": self.q.summary(sym, "markPrice"),
                "trade_pending_gaps": len(self.trades[sym].pending_gaps) if sym in self.trades else None,
            }
        return {
            "time_ns": now_ns(), "session_id": self.session_id, "uptime_s": round(time.monotonic() - self.started_mono),
            "health": {"level": level, "reasons": reasons, "stop_reason": self.stop_reason},
            "connections": {r: m.status() for r, m in self.managers.items()},
            "pipeline": {
                "queue_size": self.ws_queue.qsize(), "queue_max": self.ws_queue.maxsize,
                "queue_overflow_total": self.ws_queue_overflow, "messages_dispatched": self.messages_dispatched,
                "dispatch_lag_ms": round(self.dispatch_lag_ns / 1e6, 2),
                **{k: v for k, v in wstatus.items() if k not in ("queue_size", "queue_max")},
                "writer_queue_size": wstatus["queue_size"], "writer_queue_max": wstatus["queue_max"],
            },
            "symbols": symbols,
            "clock": self.clock.summary(),
            "rest": dict(self.rest.stats, weight_used_1m=self.rest.budget.used(), forbidden=self.rest_forbidden),
            "backfill": self.backfiller.stats | {"current": self.backfiller.current} if self.backfiller else None,
            "disk": {"free_gb": round(disk_free_gb(self.data_dir), 2), "data_dir_bytes": dir_size_bytes(self.data_dir)},
            "specs": self.spec_versions[-1] if self.spec_versions else None,
            "recovered_shards": self.recovered_shards,
            "schema_version": SCHEMA_VERSION,
        }

    async def _shutdown(self) -> None:
        for m in self.managers.values():
            m.stop()
        if self.backfiller:
            self.backfiller.stop()
        await asyncio.gather(*[t for t in self.tasks if t.get_name() != "dispatch"], return_exceptions=True)
        # 排空接收队列
        while not self.ws_queue.empty():
            self._handle(self.ws_queue.get_nowait())
        for t in self.tasks:
            if t.get_name() == "dispatch":
                t.cancel()
        st = self.build_status()
        st["health"]["stop_reason"] = self.stop_reason
        self._event_log("session_stop", {"reason": self.stop_reason, "status": st["pipeline"]})
        self.writer.stop()
        await asyncio.get_running_loop().run_in_executor(None, self.writer.join, 120)
        if self.writer.is_alive():
            log.error("写线程未在 120s 内结束")
        self._commit_checkpoint(clean=(not self.writer.is_alive() and self.writer.error is None))
        st = self.build_status()
        atomic_write_json(self.state_dir / "status.json", st)
        self._write_report(st, datetime.now(timezone.utc).strftime("%Y%m%d"))
        log.info("已停止。raw %d 行(fsync %d)，写错误=%s", self.writer.raw.records_written,
                 self.writer.raw.records_fsynced, self.writer.error)
