"""时间：本机 UTC 纳秒、单调时钟纳秒、交易所时间偏差估计。

交易所字段保持毫秒原单位；本机接收时间用 time.time_ns()（UTC 纳秒，精度取决于系统）。
偏差估计只是 RTT/2 的粗略近似，不是单向延迟测量。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field


def now_ns() -> int:
    return time.time_ns()


def mono_ns() -> int:
    return time.monotonic_ns()


@dataclass
class ServerTimeSample:
    sent_time_ns: int
    recv_time_ns: int
    server_time_ms: int

    @property
    def rtt_ns(self) -> int:
        return self.recv_time_ns - self.sent_time_ns

    @property
    def offset_estimate_ms(self) -> float:
        """server - local 的粗略估计（假定往返对称，实际不一定）。"""
        mid_local_ms = (self.sent_time_ns + self.recv_time_ns) / 2 / 1e6
        return self.server_time_ms - mid_local_ms


@dataclass
class ClockMonitor:
    """记录服务器时间样本，并检测本机墙钟跳变（墙钟增量 与 单调钟增量 不一致）。"""
    jump_threshold_ms: float = 500.0
    samples: list[ServerTimeSample] = field(default_factory=list)
    max_samples: int = 200
    _last_wall_ns: int | None = None
    _last_mono_ns: int | None = None
    jumps: list[dict] = field(default_factory=list)

    def add_sample(self, s: ServerTimeSample) -> None:
        self.samples.append(s)
        if len(self.samples) > self.max_samples:
            del self.samples[: len(self.samples) - self.max_samples]

    def check_jump(self, wall_ns: int | None = None, mono: int | None = None) -> dict | None:
        wall_ns = now_ns() if wall_ns is None else wall_ns
        mono = mono_ns() if mono is None else mono
        result = None
        if self._last_wall_ns is not None and self._last_mono_ns is not None:
            d_wall = wall_ns - self._last_wall_ns
            d_mono = mono - self._last_mono_ns
            drift_ms = (d_wall - d_mono) / 1e6
            if abs(drift_ms) > self.jump_threshold_ms:
                result = {"wall_ns": wall_ns, "drift_ms": drift_ms,
                          "wall_delta_ms": d_wall / 1e6, "mono_delta_ms": d_mono / 1e6}
                self.jumps.append(result)
        self._last_wall_ns, self._last_mono_ns = wall_ns, mono
        return result

    def summary(self) -> dict:
        if not self.samples:
            return {"samples": 0}
        last = self.samples[-1]
        offsets = sorted(s.offset_estimate_ms for s in self.samples[-20:])
        rtts = sorted(s.rtt_ns / 1e6 for s in self.samples[-20:])
        return {
            "samples": len(self.samples),
            "last_sent_time_ns": last.sent_time_ns,
            "last_rtt_ms": round(last.rtt_ns / 1e6, 3),
            "last_offset_estimate_ms": round(last.offset_estimate_ms, 3),
            "offset_median_ms_recent": round(offsets[len(offsets) // 2], 3),
            "rtt_min_ms_recent": round(rtts[0], 3),
            "clock_jumps_detected": len(self.jumps),
            "note": "offset = server - local，按 RTT/2 粗估，不是精确单向延迟",
        }
