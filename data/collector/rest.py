"""REST 客户端：权重预算、退避、限流响应处理、原始响应保存回调。

只用公开行情端点，不带任何密钥。
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

import aiohttp

from data.collector.clock import ServerTimeSample, mono_ns, now_ns
from data.collector.config import BackoffConfig, RestConfig

log = logging.getLogger("collector.rest")

DEPTH_WEIGHT = {5: 2, 10: 2, 20: 2, 50: 2, 100: 5, 500: 10, 1000: 20}
AGG_TRADES_WEIGHT = 20
EXCHANGE_INFO_WEIGHT = 1
TIME_WEIGHT = 1


class RestError(Exception):
    def __init__(self, msg: str, status: int | None = None, code: int | None = None, retry_after: float | None = None):
        super().__init__(msg)
        self.status = status
        self.code = code
        self.retry_after = retry_after


class RateLimited(RestError):
    pass


class Forbidden(RestError):
    """403/451 等明确的访问受限，不重试。"""


@dataclass
class RestResponse:
    request_id: str
    endpoint: str
    params: dict[str, Any]
    status: int
    headers: dict[str, str]
    text: str
    sent_time_ns: int
    recv_time_ns: int
    recv_monotonic_ns: int
    weight: int

    def json(self) -> Any:
        return json.loads(self.text)


class WeightBudget:
    """滑动 1 分钟窗口的本地权重预算；同时尊重服务器返回的 X-MBX-USED-WEIGHT-1M。"""

    def __init__(self, per_minute: int):
        self.per_minute = per_minute
        self._events: list[tuple[float, int]] = []
        self.server_used_1m: int | None = None
        self.server_used_time: float | None = None

    def _prune(self, now: float) -> None:
        cutoff = now - 60.0
        while self._events and self._events[0][0] < cutoff:
            self._events.pop(0)

    def used(self) -> int:
        now = time.monotonic()
        self._prune(now)
        local = sum(w for _, w in self._events)
        if self.server_used_time is not None and now - self.server_used_time < 60:
            return max(local, self.server_used_1m or 0)
        return local

    async def acquire(self, weight: int) -> float:
        waited = 0.0
        while True:
            now = time.monotonic()
            self._prune(now)
            if self.used() + weight <= self.per_minute:
                self._events.append((now, weight))
                return waited
            sleep = 0.5 if not self._events else max(0.1, 60.0 - (now - self._events[0][0]) + 0.05)
            sleep = min(sleep, 5.0)
            waited += sleep
            await asyncio.sleep(sleep)

    def observe_server(self, headers: dict[str, str]) -> None:
        v = headers.get("X-MBX-USED-WEIGHT-1M") or headers.get("x-mbx-used-weight-1m")
        if v is not None:
            try:
                self.server_used_1m = int(v)
                self.server_used_time = time.monotonic()
            except ValueError:
                pass


def backoff_delay(cfg: BackoffConfig, attempt: int) -> float:
    base = min(cfg.max_s, cfg.initial_s * (cfg.factor ** attempt))
    return base * (1 + random.uniform(-cfg.jitter, cfg.jitter))


RawSink = Callable[[str, dict[str, Any]], None]


class RestClient:
    def __init__(self, base_url: str, cfg: RestConfig, proxy: str | None = None,
                 raw_sink: RawSink | None = None, session_id: str = ""):
        self.base_url = base_url.rstrip("/")
        self.cfg = cfg
        self.proxy = proxy
        self.raw_sink = raw_sink
        self.session_id = session_id
        self.budget = WeightBudget(cfg.weight_budget_per_min)
        self._session: aiohttp.ClientSession | None = None
        self.stats = {"requests": 0, "errors": 0, "rate_limited": 0, "retries": 0, "budget_wait_s": 0.0}
        self.banned_until_mono: float | None = None

    async def __aenter__(self) -> "RestClient":
        timeout = aiohttp.ClientTimeout(total=self.cfg.timeout_s)
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._session:
            await self._session.close()

    async def _get(self, endpoint: str, params: dict[str, Any], weight: int) -> RestResponse:
        assert self._session is not None
        if self.banned_until_mono and time.monotonic() < self.banned_until_mono:
            raise RateLimited(f"本地封禁冷却中 {self.banned_until_mono - time.monotonic():.0f}s", retry_after=self.banned_until_mono - time.monotonic())
        self.stats["budget_wait_s"] += await self.budget.acquire(weight)
        rid = uuid.uuid4().hex[:12]
        sent = now_ns()
        self.stats["requests"] += 1
        async with self._session.get(self.base_url + endpoint, params=params, proxy=self.proxy) as resp:
            text = await resp.text()
            recv = now_ns()
            mono = mono_ns()
            headers = {k: v for k, v in resp.headers.items()
                       if k.lower().startswith("x-mbx") or k.lower() in ("date", "retry-after", "content-type")}
            self.budget.observe_server(headers)
            rr = RestResponse(rid, endpoint, params, resp.status, headers, text, sent, recv, mono, weight)
        if self.raw_sink:
            self.raw_sink("rest", {
                "schema_version": 1, "source": "rest", "request_id": rid, "endpoint": endpoint, "params": params,
                "status": rr.status, "headers": headers, "sent_time_ns": sent, "recv_time_ns": recv,
                "recv_monotonic_ns": mono, "session_id": self.session_id, "raw": text,
            })
        if rr.status == 200:
            return rr
        self.stats["errors"] += 1
        code = None
        try:
            code = rr.json().get("code")
        except Exception:
            pass
        retry_after = None
        if "Retry-After" in resp.headers:
            try:
                retry_after = float(resp.headers["Retry-After"])
            except ValueError:
                pass
        if rr.status in (429, 418):
            self.stats["rate_limited"] += 1
            cool = retry_after if retry_after is not None else (120.0 if rr.status == 418 else 10.0)
            self.banned_until_mono = time.monotonic() + cool
            raise RateLimited(f"HTTP {rr.status} code={code}: {text[:200]}", rr.status, code, cool)
        if rr.status in (403, 451):
            raise Forbidden(f"HTTP {rr.status} 访问受限: {text[:200]}", rr.status, code)
        raise RestError(f"HTTP {rr.status} code={code}: {text[:200]}", rr.status, code, retry_after)

    async def get_with_retry(self, endpoint: str, params: dict[str, Any], weight: int) -> RestResponse:
        last: Exception | None = None
        for attempt in range(self.cfg.max_retries + 1):
            try:
                return await self._get(endpoint, params, weight)
            except Forbidden:
                raise
            except RateLimited as exc:
                last = exc
                delay = exc.retry_after or backoff_delay(self.cfg.backoff, attempt)
                log.warning("限流 %s，等待 %.1fs", exc, delay)
            except (RestError, aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last = exc
                if isinstance(exc, RestError) and exc.status is not None and 400 <= exc.status < 500 and exc.status not in (429, 418):
                    raise                              # 参数错误等，不重试
                delay = backoff_delay(self.cfg.backoff, attempt)
                log.warning("REST %s 失败: %s，%.1fs 后重试", endpoint, exc, delay)
            self.stats["retries"] += 1
            await asyncio.sleep(delay)
        raise RestError(f"{endpoint} 重试耗尽: {last}")

    # ---------- 端点 ----------
    async def server_time(self) -> tuple[ServerTimeSample, RestResponse]:
        rr = await self.get_with_retry("/fapi/v1/time", {}, TIME_WEIGHT)
        st = int(rr.json()["serverTime"])
        return ServerTimeSample(rr.sent_time_ns, rr.recv_time_ns, st), rr

    async def exchange_info(self) -> RestResponse:
        return await self.get_with_retry("/fapi/v1/exchangeInfo", {}, EXCHANGE_INFO_WEIGHT)

    async def depth(self, symbol: str, limit: int) -> RestResponse:
        return await self.get_with_retry("/fapi/v1/depth", {"symbol": symbol, "limit": limit},
                                         DEPTH_WEIGHT.get(limit, 20))

    async def agg_trades(self, symbol: str, from_id: int | None = None, start_time: int | None = None,
                         end_time: int | None = None, limit: int = 1000) -> RestResponse:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if from_id is not None:
            params["fromId"] = from_id
        else:
            if start_time is not None:
                params["startTime"] = start_time
            if end_time is not None:
                params["endTime"] = end_time
        return await self.get_with_retry("/fapi/v1/aggTrades", params, AGG_TRADES_WEIGHT)
