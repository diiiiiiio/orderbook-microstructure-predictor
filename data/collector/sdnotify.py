"""最小 sd_notify 实现（无依赖）。不在 systemd 下运行时所有调用都是空操作。

用法：Type=notify + WatchdogSec=N。采集器只有在写线程存活且未失败时才发 WATCHDOG=1，
失败/卡死 → systemd 在 N 秒后按 WatchdogSignal 重启服务。
"""
from __future__ import annotations

import os
import socket


class SdNotify:
    def __init__(self) -> None:
        addr = os.environ.get("NOTIFY_SOCKET")
        self._sock: socket.socket | None = None
        self._addr: str | bytes | None = None
        if addr:
            if addr.startswith("@"):
                self._addr = b"\0" + addr[1:].encode()
            else:
                self._addr = addr
            try:
                self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            except OSError:
                self._sock = None
        self.watchdog_usec = int(os.environ.get("WATCHDOG_USEC", "0") or 0)

    @property
    def enabled(self) -> bool:
        return self._sock is not None

    def _send(self, msg: str) -> None:
        if not self._sock or self._addr is None:
            return
        try:
            self._sock.sendto(msg.encode(), self._addr)
        except OSError:
            pass

    def ready(self) -> None:
        self._send("READY=1")

    def watchdog(self) -> None:
        self._send("WATCHDOG=1")

    def status(self, text: str) -> None:
        self._send("STATUS=" + text.replace("\n", " ")[:200])

    def stopping(self) -> None:
        self._send("STOPPING=1")
