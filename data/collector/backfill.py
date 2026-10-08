"""aggTrade 补数：按 fromId 分页，受权重预算/退避约束，有进度、重试上限、窗口检查、终止条件。

官方约束（2026-09-25 实测）：历史查询窗口"最近 2 天"（错误码 -4166），
startTime/endTime 间隔 < 1 小时，limit 最大 1000，权重 20。
补数不阻塞实时接收：独立任务，逐个 gap 处理。
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

from data.collector.clock import now_ns
from data.collector.config import BackfillConfig
from data.collector.rest import Forbidden, RateLimited, RestClient, RestError
from data.collector.trades import TradeGap, TradeProcessor

log = logging.getLogger("collector.backfill")

RowSink = Callable[[list[dict[str, Any]]], None]


class TradeBackfiller:
    def __init__(self, rest: RestClient, cfg: BackfillConfig, processors: dict[str, TradeProcessor],
                 row_sink: RowSink):
        self.rest = rest
        self.cfg = cfg
        self.processors = processors
        self.row_sink = row_sink
        self.stats = {"gaps_seen": 0, "gaps_repaired": 0, "gaps_partial": 0, "gaps_unrepairable": 0,
                      "pages": 0, "trades_filled": 0, "errors": 0}
        self._stop = asyncio.Event()
        self.current: dict[str, Any] | None = None

    def stop(self) -> None:
        self._stop.set()

    def _window_ok(self, gap_T_ms: int | None) -> bool:
        if gap_T_ms is None:
            return True
        age_h = (now_ns() / 1e6 - gap_T_ms) / 3.6e6
        return age_h < self.cfg.window_hours - self.cfg.window_safety_margin_min / 60

    async def run(self) -> None:
        while not self._stop.is_set():
            worked = False
            for sym, proc in self.processors.items():
                for tg in list(proc.pending_gaps.values()):
                    if self._stop.is_set():
                        return
                    if tg.attempts >= self.cfg.max_attempts:
                        continue
                    worked = True
                    await self._repair(sym, proc, tg)
                    await asyncio.sleep(self.cfg.min_interval_s)
            if not worked:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass

    async def _repair(self, sym: str, proc: TradeProcessor, tg: TradeGap) -> None:
        tg.attempts += 1
        self.stats["gaps_seen"] += 1 if tg.attempts == 1 else 0
        gap = tg.gap
        if not self._window_ok(gap.start_exchange_time_ms):
            proc.q.close_gap(gap, "unrepairable", note=gap.note + "; beyond REST history window")
            proc.pending_gaps.pop(gap.gap_id, None)
            self.stats["gaps_unrepairable"] += 1
            return
        # 从缺口内最小未填的 a 开始
        cursor = tg.from_a
        pages = 0
        self.current = {"symbol": sym, "gap_id": gap.gap_id, "from_a": tg.from_a, "to_a": tg.to_a, "cursor": cursor}
        while cursor <= tg.to_a and pages < self.cfg.max_pages_per_gap and not self._stop.is_set():
            while cursor in tg.filled and cursor <= tg.to_a:
                cursor += 1
            if cursor > tg.to_a:
                break
            try:
                rr = await self.rest.agg_trades(sym, from_id=cursor, limit=self.cfg.page_limit)
            except Forbidden as exc:
                self.stats["errors"] += 1
                proc.q.event(sym, "aggTrade", "backfill_forbidden", "error", error=str(exc))
                tg.attempts = self.cfg.max_attempts
                return
            except (RateLimited, RestError) as exc:
                self.stats["errors"] += 1
                proc.q.event(sym, "aggTrade", "backfill_error", "warning", error=str(exc), attempt=tg.attempts)
                return                           # 下一轮再试
            pages += 1
            self.stats["pages"] += 1
            items = rr.json()
            if not isinstance(items, list):
                proc.q.event(sym, "aggTrade", "backfill_bad_response", "error", body=rr.text[:200])
                return
            if not items:
                break
            in_gap = [it for it in items if isinstance(it, dict) and tg.from_a <= it.get("a", -1) <= tg.to_a]
            rows = proc.on_backfill(in_gap, known_time_ns=rr.recv_time_ns, request_id=rr.request_id)
            if rows:
                self.row_sink(rows)
                self.stats["trades_filled"] += len(rows)
            max_a = max((it.get("a", -1) for it in items if isinstance(it, dict)), default=cursor)
            if max_a < cursor:
                break
            cursor = max_a + 1
            self.current["cursor"] = cursor
        filled = len(tg.filled)
        total = tg.to_a - tg.from_a + 1
        if gap.gap_id not in proc.pending_gaps:      # on_backfill 里已 repaired
            self.stats["gaps_repaired"] += 1
        elif filled == total:
            src = ", ".join(f"{k}={v}" for k, v in sorted(tg.sources.items()))
            proc.q.close_gap(gap, "repaired", note=gap.note + f"; filled ({src})")
            proc.pending_gaps.pop(gap.gap_id, None)
            self.stats["gaps_repaired"] += 1
        elif tg.attempts >= self.cfg.max_attempts or pages >= self.cfg.max_pages_per_gap:
            status = "partial" if filled else "unrepairable"
            proc.q.close_gap(gap, status, note=gap.note + f"; filled {filled}/{total} after {tg.attempts} attempts")
            proc.pending_gaps.pop(gap.gap_id, None)
            self.stats["gaps_partial" if filled else "gaps_unrepairable"] += 1
        self.current = None
