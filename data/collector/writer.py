"""写线程：接收 dispatcher 投递的写操作，串行执行 raw/normalized 写入。

与 asyncio 接收循环分离，压缩和 Parquet 写入不阻塞 WebSocket。
队列有界；投递失败（Full）由调用方计数并记录，不静默丢。
任何写入异常 → error 状态，供 health 报告为 failed。
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from pathlib import Path
from typing import Any

from data.collector.records import BookRow, GapRecord, QualityEvent
from data.collector.storage.normalized import NormalizedWriter
from data.collector.storage.raw import RawWriter

log = logging.getLogger("collector.writer")


class WriterThread(threading.Thread):
    def __init__(self, raw: RawWriter, norm: NormalizedWriter, maxsize: int, tick_s: float = 0.5):
        super().__init__(name="writer", daemon=True)
        self.raw = raw
        self.norm = norm
        self.q: queue.Queue = queue.Queue(maxsize=maxsize)
        self.tick_s = tick_s
        self.error: str | None = None
        self.error_time_ns: int | None = None
        self.ops_done = 0
        self.overflow_count = 0
        self.last_op_recv_ns: int | None = None       # 最近处理的记录的接收时间（用于估算落盘滞后）
        self.last_loop_mono = time.monotonic()
        self._stop_evt = threading.Event()
        self.lock = threading.Lock()
        self.last_fsync_mono: float | None = None

    # ---------- 投递（在事件循环线程调用） ----------
    def put(self, op: tuple) -> bool:
        try:
            self.q.put_nowait(op)
            return True
        except queue.Full:
            self.overflow_count += 1
            return False

    def stop(self) -> None:
        self._stop_evt.set()
        try:
            self.q.put_nowait(("stop",))
        except queue.Full:
            pass

    # ---------- 线程主体 ----------
    def run(self) -> None:
        while True:
            try:
                op = self.q.get(timeout=self.tick_s)
            except queue.Empty:
                op = None
            try:
                if op is None:
                    self._periodic()
                    if self._stop_evt.is_set() and self.q.empty():
                        break
                    continue
                if op[0] == "stop":
                    self._drain_remaining()
                    break
                self._apply(op)
                self.ops_done += 1
                if self.q.empty():
                    self._periodic()
            except Exception as exc:                  # noqa: BLE001
                self.error = f"{type(exc).__name__}: {exc}"
                self.error_time_ns = time.time_ns()
                log.exception("写入失败，进入失败状态")
                # 不再吞掉后续数据：继续消费队列但直接丢弃并计数，由 health 标记 failed
                self._fail_loop()
                return
        try:
            self.raw.close_all()
            self.norm.close()
        except Exception as exc:                      # noqa: BLE001
            self.error = self.error or f"close: {type(exc).__name__}: {exc}"
            log.exception("关闭文件失败")

    def _fail_loop(self) -> None:
        dropped = 0
        while not self._stop_evt.is_set():
            try:
                op = self.q.get(timeout=0.5)
            except queue.Empty:
                continue
            if op[0] == "stop":
                break
            dropped += 1
        with self.lock:
            self.overflow_count += dropped
        log.error("失败状态下丢弃 %d 个写操作", dropped)

    def _drain_remaining(self) -> None:
        while True:
            try:
                op = self.q.get_nowait()
            except queue.Empty:
                return
            if op[0] != "stop":
                self._apply(op)
                self.ops_done += 1

    def _periodic(self) -> None:
        self.raw.maybe_flush()
        self.norm.maybe_flush()
        self.last_loop_mono = time.monotonic()

    def _apply(self, op: tuple) -> None:
        kind = op[0]
        if kind == "raw":
            _, stream_kind, symbol, rec, id_value = op
            self.raw.write(stream_kind, symbol, rec, id_value)
            self.last_op_recv_ns = rec.get("recv_time_ns", self.last_op_recv_ns)
        elif kind == "book":
            self.norm.add_book_row(op[1])
        elif kind == "trade":
            self.norm.add_trade(op[1])
        elif kind == "trades":
            for r in op[1]:
                self.norm.add_trade(r)
        elif kind == "mark":
            self.norm.add_mark(op[1])
        elif kind == "gap":
            self.norm.add_gap(op[1])
        elif kind == "qe":
            self.norm.add_quality(op[1])
        elif kind == "flush":
            self.raw.maybe_flush(force=True)
            self.norm.maybe_flush(force=True)
        else:
            raise ValueError(f"未知写操作 {kind}")

    def status(self) -> dict[str, Any]:
        lag = None
        if self.last_op_recv_ns is not None:
            lag = round((time.time_ns() - self.last_op_recv_ns) / 1e9, 3)
        return {
            "queue_size": self.q.qsize(), "queue_max": self.q.maxsize, "overflow_total": self.overflow_count,
            "ops_done": self.ops_done, "error": self.error, "error_time_ns": self.error_time_ns,
            "raw_records_written": self.raw.records_written, "raw_bytes_written": self.raw.bytes_written,
            "raw_records_fsynced": self.raw.records_fsynced, "raw_active_files": self.raw.active_files(),
            "raw_closed_shards": len(self.raw.manifests),
            "normalized_pending": self.norm.pending(),
            "normalized_rows": {t.table: t.rows_written for t in self.norm.tables},
            "writer_lag_s": lag, "alive": self.is_alive(),
        }
