"""连接层测试：用本地 websockets 服务器模拟交易所（轮换、静默、断开重连、队列溢出）。"""
import asyncio
import json

import pytest
from websockets.asyncio.server import serve

from data.collector.config import WsConfig, BackoffConfig
from data.collector.ws import ConnectionManager, WsConnection


class FakeExchange:
    def __init__(self, interval=0.02, silent_stream=None, close_after_msgs=None):
        self.interval = interval
        self.silent_stream = silent_stream
        self.close_after_msgs = close_after_msgs
        self.connections = 0
        self.seq = 0
        self.server = None

    async def handler(self, ws):
        self.connections += 1
        n = 0
        try:
            while True:
                for stream in ("btcusdt@depth@100ms", "btcusdt@aggTrade"):
                    if stream == self.silent_stream:
                        continue
                    self.seq += 1
                    await ws.send(json.dumps({"stream": stream, "data": {"e": "x", "u": self.seq}}))
                    n += 1
                    if self.close_after_msgs and n >= self.close_after_msgs:
                        await ws.close(code=1012, reason="restart")
                        return
                await asyncio.sleep(self.interval)
        except Exception:
            return

    async def __aenter__(self):
        self.server = await serve(self.handler, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *a):
        self.server.close()
        await self.server.wait_closed()


def cfg(**kw):
    base = dict(open_timeout_s=5, close_timeout_s=1, idle_timeout_s=2, rotate_after_s=1e9, rotation_overlap_s=0.2,
                unsolicited_pong_s=0, backoff=BackoffConfig(0.05, 0.2, 2, 0.1),
                stream_silence_s={"depth": 0.5, "aggTrade": 100})
    base.update(kw)
    return WsConfig(**base)


async def collect_events(mgr_run, duration):
    task = asyncio.create_task(mgr_run)
    await asyncio.sleep(duration)
    return task


# 10a. 新旧连接轮换
@pytest.mark.asyncio
async def test_rotation_overlaps_then_switches():
    events = []
    async with FakeExchange() as ex:
        q = asyncio.Queue(10000)
        m = ConnectionManager("public", f"ws://127.0.0.1:{ex.port}", cfg(rotate_after_s=0.6), q, None,
                              lambda k, d: events.append((k, d)), ["btcusdt@depth@100ms", "btcusdt@aggTrade"])
        t = asyncio.create_task(m.run())
        await asyncio.sleep(2.0)
        m.stop()
        await asyncio.wait_for(t, 5)
    kinds = [k for k, _ in events]
    assert m.rotations >= 1 and ex.connections >= 2
    assert "ws_rotation_start" in kinds and "ws_rotation_done" in kinds
    ids = {d["connection_id"] for k, d in events if k == "ws_open"}
    assert len(ids) >= 2
    # 队列里两条连接的消息都在（重叠期重复由业务层去重）
    cids = set()
    while not q.empty():
        cids.add(q.get_nowait().connection_id)
    assert len(cids) >= 2
    assert m.reconnects == 0          # 轮换不算重连


# 10b. 连接活着但订阅无数据 → 报告并重连
@pytest.mark.asyncio
async def test_silent_stream_detected_and_reconnects():
    events = []
    async with FakeExchange(silent_stream="btcusdt@depth@100ms") as ex:
        q = asyncio.Queue(10000)
        m = ConnectionManager("public", f"ws://127.0.0.1:{ex.port}", cfg(), q, None,
                              lambda k, d: events.append((k, d)), ["btcusdt@depth@100ms", "btcusdt@aggTrade"])
        t = asyncio.create_task(m.run())
        await asyncio.sleep(2.5)
        m.stop()
        await asyncio.wait_for(t, 5)
    silent = [d for k, d in events if k == "ws_stream_silent"]
    assert silent and silent[0]["streams"][0]["stream"] == "btcusdt@depth@100ms"
    assert m.reconnects >= 1
    assert any("stream_silent" in d.get("reason", "") for k, d in events if k == "ws_close")


# 服务端关闭 → 退避重连
@pytest.mark.asyncio
async def test_server_close_triggers_backoff_reconnect():
    events = []
    async with FakeExchange(close_after_msgs=4) as ex:
        q = asyncio.Queue(10000)
        m = ConnectionManager("public", f"ws://127.0.0.1:{ex.port}", cfg(), q, None,
                              lambda k, d: events.append((k, d)), ["btcusdt@depth@100ms"])
        t = asyncio.create_task(m.run())
        await asyncio.sleep(1.5)
        m.stop()
        await asyncio.wait_for(t, 5)
    assert m.reconnects >= 2 and ex.connections >= 3
    delays = [d["delay_s"] for k, d in events if k == "ws_reconnect_scheduled"]
    assert delays and all(0 < x <= 0.3 for x in delays)


# 接收队列满 → 记录溢出，不静默
@pytest.mark.asyncio
async def test_receive_queue_overflow_is_reported():
    events = []
    async with FakeExchange(interval=0.001) as ex:
        q = asyncio.Queue(5)
        m = ConnectionManager("public", f"ws://127.0.0.1:{ex.port}", cfg(), q, None,
                              lambda k, d: events.append((k, d)), ["btcusdt@depth@100ms"])
        t = asyncio.create_task(m.run())
        await asyncio.sleep(0.5)
        m.stop()
        await asyncio.wait_for(t, 5)
    assert any(k == "queue_overflow" for k, _ in events)
    assert m.history and sum(c.queue_drops for c in m.history) > 0


# 故障注入：建立后主动断开一次
@pytest.mark.asyncio
async def test_fault_injection_close_once():
    events = []
    async with FakeExchange() as ex:
        q = asyncio.Queue(10000)
        m = ConnectionManager("public", f"ws://127.0.0.1:{ex.port}", cfg(inject_close_after_s=0.3), q, None,
                              lambda k, d: events.append((k, d)), ["btcusdt@depth@100ms"])
        t = asyncio.create_task(m.run())
        await asyncio.sleep(1.5)
        m.stop()
        await asyncio.wait_for(t, 5)
    assert [k for k, _ in events].count("fault_injection") == 1
    assert m.reconnects == 1 and ex.connections == 2


def test_recv_time_recorded_before_parse():
    """解析失败的消息仍带接收时间与序号，并进入队列。"""
    import time
    from data.collector.ws import RawMessage
    q = asyncio.Queue(10)
    c = WsConnection("ws://x", "public", cfg(), q, None, lambda k, d: None, 1, [])
    # 直接驱动解析分支
    async def run():
        class FakeWs:
            def __init__(self): self.msgs = ["not json", json.dumps({"stream": "s", "data": {"e": "x"}}), json.dumps([1, 2])]
            async def recv(self):
                if not self.msgs: raise asyncio.TimeoutError
                return self.msgs.pop(0)
        await c._recv_loop(FakeWs())
    asyncio.run(run())
    got = [q.get_nowait() for _ in range(q.qsize())]
    assert len(got) == 3 and got[0].parse_error and got[0].recv_time_ns > 0 and got[0].recv_seq == 1
    assert got[1].data == {"e": "x"} and got[2].parse_error == "unexpected_shape"
    assert c.info.parse_errors == 2
