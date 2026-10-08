"""WebSocket 连接：单条连接的生命周期 + 轮换管理。

每条连接：connection_id、打开/关闭时间、消息计数、每个 stream 的最后消息时间。
收到消息第一件事记录 recv_time_ns / recv_monotonic_ns，然后才解析。
连接层不理解业务，只把 (RawMessage) 放进有界队列；队列满 → 记录溢出、不静默丢。
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import websockets
from websockets.asyncio.client import connect

from data.collector.clock import mono_ns, now_ns
from data.collector.config import WsConfig
from data.collector.rest import backoff_delay

log = logging.getLogger("collector.ws")


@dataclass
class RawMessage:
    connection_id: str
    recv_seq: int
    recv_time_ns: int
    recv_monotonic_ns: int
    text: str
    stream: str | None          # 组合流包装里的 stream 名
    data: dict[str, Any] | None  # 解析后的 data；解析失败为 None
    parse_error: str | None = None


@dataclass
class ConnectionInfo:
    connection_id: str
    url: str
    role: str                     # public / market
    opened_time_ns: int | None = None
    closed_time_ns: int | None = None
    close_reason: str = ""
    messages: int = 0
    bytes: int = 0
    last_msg_mono_ns: int | None = None
    last_msg_by_stream: dict[str, int] = field(default_factory=dict)   # stream -> recv_monotonic_ns
    msg_by_stream: dict[str, int] = field(default_factory=dict)
    parse_errors: int = 0
    queue_drops: int = 0
    generation: int = 0


EventSink = Callable[[str, dict[str, Any]], None]


class WsConnection:
    def __init__(self, url: str, role: str, cfg: WsConfig, queue: asyncio.Queue, proxy: str | None,
                 event_sink: EventSink, generation: int, expected_streams: list[str]):
        self.url = url
        self.role = role
        self.cfg = cfg
        self.queue = queue
        self.proxy = proxy
        self.event_sink = event_sink
        self.info = ConnectionInfo(f"{role}-{uuid.uuid4().hex[:10]}", url, role, generation=generation)
        self.expected_streams = expected_streams
        self._seq = 0
        self._stop = asyncio.Event()
        self._ws = None
        self.task: asyncio.Task | None = None
        self.ready = asyncio.Event()       # 收到第一条消息
        self.done = asyncio.Event()

    def stop(self, reason: str = "requested") -> None:
        self.info.close_reason = self.info.close_reason or reason
        self._stop.set()

    async def run(self) -> None:
        cid = self.info.connection_id
        try:
            async with connect(self.url, proxy=self.proxy, open_timeout=self.cfg.open_timeout_s,
                               close_timeout=self.cfg.close_timeout_s, max_size=self.cfg.max_message_bytes,
                               ping_interval=None,          # 服务器主动 ping；库自动回 pong
                               max_queue=4096, compression=None) as ws:
                self._ws = ws
                self.info.opened_time_ns = now_ns()
                self.event_sink("ws_open", {"connection_id": cid, "url": self.url, "role": self.role,
                                            "time_ns": self.info.opened_time_ns})
                log.info("[%s] 已连接 %s", cid, self.url)
                pong_task = asyncio.create_task(self._unsolicited_pong(ws)) if self.cfg.unsolicited_pong_s else None
                inject_task = None
                if self.cfg.inject_close_after_s and self.generation_allows_inject():
                    inject_task = asyncio.create_task(self._inject_close(ws))
                try:
                    await self._recv_loop(ws)
                finally:
                    if pong_task:
                        pong_task.cancel()
                    if inject_task:
                        inject_task.cancel()
        except asyncio.CancelledError:
            self.info.close_reason = self.info.close_reason or "cancelled"
            raise
        except Exception as exc:
            self.info.close_reason = self.info.close_reason or f"{type(exc).__name__}: {exc}"
            log.warning("[%s] 连接结束: %s", cid, self.info.close_reason)
        finally:
            self.info.closed_time_ns = now_ns()
            self.event_sink("ws_close", {"connection_id": cid, "role": self.role, "reason": self.info.close_reason,
                                         "time_ns": self.info.closed_time_ns, "messages": self.info.messages})
            self.done.set()

    def generation_allows_inject(self) -> bool:
        return self.info.generation == 1          # 只注入一次，验证重连路径

    async def _inject_close(self, ws) -> None:
        await asyncio.sleep(self.cfg.inject_close_after_s or 0)
        self.info.close_reason = "fault_injection_close"
        self.event_sink("fault_injection", {"connection_id": self.info.connection_id, "action": "close_ws"})
        await ws.close(code=4000, reason="fault injection")

    async def _unsolicited_pong(self, ws) -> None:
        while True:
            await asyncio.sleep(self.cfg.unsolicited_pong_s)
            try:
                await ws.pong(b"")
            except Exception:
                return

    async def _recv_loop(self, ws) -> None:
        idle = self.cfg.idle_timeout_s
        while not self._stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=idle)
            except asyncio.TimeoutError:
                self.info.close_reason = f"idle_timeout_{idle}s"
                self.event_sink("ws_idle_timeout", {"connection_id": self.info.connection_id, "idle_s": idle})
                return
            except websockets.ConnectionClosed as exc:
                self.info.close_reason = f"closed_by_peer code={exc.code} reason={exc.reason!r}"
                return
            recv_ns = now_ns()
            mono = mono_ns()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", "replace")
            self._seq += 1
            self.info.messages += 1
            self.info.bytes += len(raw)
            self.info.last_msg_mono_ns = mono
            stream = data = err = None
            try:
                msg = json.loads(raw)
                if isinstance(msg, dict) and "stream" in msg and "data" in msg:
                    stream, data = msg["stream"], msg["data"]
                elif isinstance(msg, dict) and "e" in msg:
                    data = msg
                    stream = None
                else:
                    err = "unexpected_shape"
                if not isinstance(data, dict):
                    err = err or "data_not_object"
                    data = None
            except json.JSONDecodeError as exc:
                err = f"json_error: {exc}"
            if err:
                self.info.parse_errors += 1
            key = stream or (data.get("e") if data else "?")
            self.info.last_msg_by_stream[key] = mono
            self.info.msg_by_stream[key] = self.info.msg_by_stream.get(key, 0) + 1
            rm = RawMessage(self.info.connection_id, self._seq, recv_ns, mono, raw, stream, data, err)
            try:
                self.queue.put_nowait(rm)
            except asyncio.QueueFull:
                self.info.queue_drops += 1
                self.event_sink("queue_overflow", {"connection_id": self.info.connection_id, "recv_seq": self._seq,
                                                   "stream": stream, "recv_time_ns": recv_ns})
            if not self.ready.is_set():
                self.ready.set()

    def silent_streams(self, now_mono_ns: int, thresholds: dict[str, float]) -> list[tuple[str, float]]:
        """返回超过静默阈值的 stream 及静默秒数。用于判断"连着但没数据"。"""
        out = []
        base = self.info.last_msg_mono_ns
        for s in self.expected_streams:
            kind = "depth" if "@depth" in s else ("aggTrade" if "aggTrade" in s else ("markPrice" if "markPrice" in s else s))
            thr = thresholds.get(kind)
            if thr is None:
                continue
            last = self.info.last_msg_by_stream.get(s)
            if last is None:
                if self.info.opened_time_ns is None:
                    continue
                silent = (now_mono_ns - (base or now_mono_ns)) / 1e9
                # 从未收到该流：用连接打开后的时长
                opened_mono = now_mono_ns - (now_ns() - self.info.opened_time_ns)
                silent = (now_mono_ns - opened_mono) / 1e9
            else:
                silent = (now_mono_ns - last) / 1e9
            if silent > thr:
                out.append((s, silent))
        return out


class ConnectionManager:
    """维护一个角色（public/market）的连接：重连退避、24h 前轮换、新旧重叠。"""

    def __init__(self, role: str, url: str, cfg: WsConfig, queue: asyncio.Queue, proxy: str | None,
                 event_sink: EventSink, expected_streams: list[str]):
        self.role = role
        self.url = url
        self.cfg = cfg
        self.queue = queue
        self.proxy = proxy
        self.event_sink = event_sink
        self.expected_streams = expected_streams
        self.current: WsConnection | None = None
        self.previous: WsConnection | None = None
        self.generation = 0
        self.reconnects = 0
        self.rotations = 0
        self.history: list[ConnectionInfo] = []
        self._stop = asyncio.Event()
        self.state = "connecting"
        self.next_rotation_mono: float | None = None
        self.last_error: str = ""

    def stop(self) -> None:
        self._stop.set()
        for c in (self.current, self.previous):
            if c:
                c.stop("shutdown")

    def _spawn(self) -> WsConnection:
        self.generation += 1
        c = WsConnection(self.url, self.role, self.cfg, self.queue, self.proxy, self.event_sink,
                         self.generation, self.expected_streams)
        c.task = asyncio.create_task(c.run(), name=f"ws-{c.info.connection_id}")
        return c

    async def run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            conn = self._spawn()
            self.current = conn
            self.state = "connecting"
            try:
                await asyncio.wait_for(conn.ready.wait(), timeout=self.cfg.open_timeout_s + 15)
            except asyncio.TimeoutError:
                if not conn.done.is_set():
                    conn.stop("no_first_message")
            if conn.ready.is_set():
                attempt = 0
                self.state = "connected"
                self.next_rotation_mono = time.monotonic() + self.cfg.rotate_after_s * random.uniform(0.9, 1.0)
                await self._watch(conn)
            if self._stop.is_set():
                break
            # 连接结束 → 退避重连
            await self._finish(conn)
            self.reconnects += 1
            delay = backoff_delay(self.cfg.backoff, attempt)
            attempt += 1
            self.state = "backoff"
            self.last_error = conn.info.close_reason
            self.event_sink("ws_reconnect_scheduled", {"role": self.role, "delay_s": delay, "attempt": attempt,
                                                       "reason": conn.info.close_reason})
            log.info("[%s] %.1fs 后重连 (第 %d 次)", self.role, delay, attempt)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
        for c in (self.current, self.previous):
            if c and c.task and c.info not in self.history:
                c.stop("shutdown")
                await self._finish(c)
        self.state = "stopped"

    async def _watch(self, conn: WsConnection) -> None:
        """连接活着时：检查静默流、到点轮换。"""
        while not conn.done.is_set() and not self._stop.is_set():
            try:
                await asyncio.wait_for(conn.done.wait(), timeout=1.0)
                return
            except asyncio.TimeoutError:
                pass
            silent = conn.silent_streams(mono_ns(), self.cfg.stream_silence_s)
            if silent:
                self.event_sink("ws_stream_silent", {"connection_id": conn.info.connection_id, "role": self.role,
                                                     "streams": [{"stream": s, "silent_s": round(v, 1)} for s, v in silent]})
                # depth 类流静默视为连接不健康 → 主动重连
                if any("@depth" in s or "markPrice" in s for s, _ in silent):
                    conn.stop("stream_silent:" + ",".join(s for s, _ in silent))
                    return
            if self.next_rotation_mono and time.monotonic() >= self.next_rotation_mono:
                await self._rotate(conn)
                return

    async def _rotate(self, old: WsConnection) -> None:
        """开新连接，等它收到第一条消息后再关旧连接。重叠期间两条连接的消息都进队列，
        由业务层按 u / a 去重（重复保存在 raw 层，不重复应用）。"""
        self.rotations += 1
        new = self._spawn()
        self.event_sink("ws_rotation_start", {"role": self.role, "old": old.info.connection_id,
                                              "new": new.info.connection_id})
        try:
            await asyncio.wait_for(new.ready.wait(), timeout=self.cfg.open_timeout_s + 15)
            ok = True
        except asyncio.TimeoutError:
            ok = False
        if not ok or new.done.is_set():
            new.stop("rotation_new_failed")
            self.event_sink("ws_rotation_failed", {"role": self.role, "new": new.info.connection_id,
                                                   "reason": new.info.close_reason})
            await self._finish(new)
            # 旧连接继续用，稍后再试
            self.next_rotation_mono = time.monotonic() + 60
            await self._watch(old)
            return
        self.previous = old
        self.current = new
        self.next_rotation_mono = time.monotonic() + self.cfg.rotate_after_s * random.uniform(0.9, 1.0)
        await asyncio.sleep(min(self.cfg.rotation_overlap_s, 5.0))
        old.stop("rotated_out")
        await self._finish(old)
        self.previous = None
        self.event_sink("ws_rotation_done", {"role": self.role, "old": old.info.connection_id,
                                             "new": new.info.connection_id,
                                             "note": "连续性由业务层按更新编号验证，未验证前不宣称无缝"})
        await self._watch(new)

    async def _finish(self, conn: WsConnection) -> None:
        if conn.task:
            try:
                await asyncio.wait_for(conn.task, timeout=self.cfg.close_timeout_s + 5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                conn.task.cancel()
            except Exception:
                pass
        self.history.append(conn.info)
        if len(self.history) > 200:
            self.history = self.history[-200:]

    def status(self) -> dict[str, Any]:
        c = self.current
        return {
            "role": self.role, "state": self.state, "reconnects": self.reconnects, "rotations": self.rotations,
            "last_error": self.last_error,
            "current": None if not c else {
                "connection_id": c.info.connection_id, "opened_time_ns": c.info.opened_time_ns,
                "messages": c.info.messages, "bytes": c.info.bytes, "parse_errors": c.info.parse_errors,
                "queue_drops": c.info.queue_drops, "msg_by_stream": c.info.msg_by_stream,
                "silent_s_by_stream": {s: round((mono_ns() - t) / 1e9, 1) for s, t in c.info.last_msg_by_stream.items()},
            },
            "next_rotation_in_s": None if not self.next_rotation_mono else round(self.next_rotation_mono - time.monotonic()),
        }
