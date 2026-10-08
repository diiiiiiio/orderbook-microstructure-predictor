import asyncio

import pytest

from data.collector.backfill import TradeBackfiller
from data.collector.config import BackfillConfig
from data.collector.quality import QualityRegistry
from data.collector.rest import RestError, RestResponse, RateLimited
from data.collector.trades import TradeProcessor
from tests.test_trades import trade


class FakeRest:
    """模拟 /aggTrades：按 fromId 分页返回 universe 中的成交。"""

    def __init__(self, universe: dict[int, dict], page: int = 2, fail_first: int = 0):
        self.universe = universe
        self.page = page
        self.calls = []
        self.fail_first = fail_first

    async def agg_trades(self, symbol, from_id=None, start_time=None, end_time=None, limit=1000):
        self.calls.append(from_id)
        if self.fail_first > 0:
            self.fail_first -= 1
            raise RateLimited("429", 429, -1003, 0.01)
        ids = sorted(a for a in self.universe if a >= from_id)[: min(limit, self.page)]
        import json
        items = [self.universe[a] for a in ids]
        return RestResponse("rq", "/fapi/v1/aggTrades", {}, 200, {}, json.dumps(items), 1, 2, 2, 20)


def setup(universe, **cfg):
    gaps, events = [], []
    q = QualityRegistry("s", gaps.append, events.append)
    tp = TradeProcessor("BTCUSDT", q)
    rows = []
    bf = TradeBackfiller(FakeRest(universe, **{k: v for k, v in cfg.items() if k in ("page", "fail_first")}),
                         BackfillConfig(page_limit=1000, max_pages_per_gap=cfg.get("max_pages", 50),
                                        max_attempts=cfg.get("max_attempts", 3), min_interval_s=0),
                         {"BTCUSDT": tp}, rows.extend)
    return q, tp, bf, rows, gaps


def rest_item(a):
    return {"a": a, "p": "1", "q": "1", "f": a, "l": a, "T": 1_700_000_000_000, "m": True}


async def run_once(bf, tp):
    tg = next(iter(tp.pending_gaps.values()))
    await bf._repair("BTCUSDT", tp, tg)


# 9. 分页补数、重叠去重、不可修复
def test_paged_backfill_repairs_gap():
    universe = {a: rest_item(a) for a in range(1, 20)}
    q, tp, bf, rows, gaps = setup(universe, page=2)
    tp.on_ws(trade(1), 1, 1, 1, "c")
    tp.on_ws(trade(8), 2, 2, 2, "c")           # 2..7 缺
    asyncio.run(run_once(bf, tp))
    assert sorted(r["a"] for r in rows) == [2, 3, 4, 5, 6, 7]
    assert len(bf.rest.calls) >= 3                                   # 分页
    assert gaps[-1].repair_status == "repaired" and tp.pending_gaps == {}
    assert bf.stats["gaps_repaired"] == 1


def test_backfill_overlap_with_ws_is_deduped():
    universe = {a: rest_item(a) for a in range(1, 10)}
    q, tp, bf, rows, gaps = setup(universe, page=100)
    tp.on_ws(trade(1), 1, 1, 1, "c")
    tp.on_ws(trade(6), 2, 2, 2, "c")
    tp.on_ws(trade(3), 3, 3, 3, "c2")           # 迟到的 ws 消息先填了 3
    asyncio.run(run_once(bf, tp))
    assert sorted(r["a"] for r in rows) == [2, 4, 5]
    assert gaps[-1].repair_status == "repaired"


def test_unrepairable_when_exchange_lacks_trades():
    universe = {a: rest_item(a) for a in (1, 9)}   # 2..8 交易所也没有
    q, tp, bf, rows, gaps = setup(universe, page=100, max_attempts=2)
    tp.on_ws(trade(1), 1, 1, 1, "c")
    tp.on_ws(trade(9), 2, 2, 2, "c")
    asyncio.run(run_once(bf, tp))
    assert gaps[-1].repair_status == "open"       # 第一次尝试后仍 open
    asyncio.run(run_once(bf, tp))
    assert gaps[-1].repair_status == "unrepairable" and tp.pending_gaps == {}
    assert rows == []


def test_partial_when_only_some_found():
    universe = {a: rest_item(a) for a in (1, 3, 9)}
    q, tp, bf, rows, gaps = setup(universe, page=100, max_attempts=1)
    tp.on_ws(trade(1), 1, 1, 1, "c")
    tp.on_ws(trade(9), 2, 2, 2, "c")
    asyncio.run(run_once(bf, tp))
    assert gaps[-1].repair_status == "partial" and [r["a"] for r in rows] == [3]


def test_rate_limit_does_not_block_and_retries_later():
    universe = {a: rest_item(a) for a in range(1, 6)}
    q, tp, bf, rows, gaps = setup(universe, page=100, fail_first=1)
    tp.on_ws(trade(1), 1, 1, 1, "c")
    tp.on_ws(trade(5), 2, 2, 2, "c")
    asyncio.run(run_once(bf, tp))
    assert rows == [] and bf.stats["errors"] == 1 and gaps[-1].repair_status == "open"
    asyncio.run(run_once(bf, tp))
    assert sorted(r["a"] for r in rows) == [2, 3, 4] and gaps[-1].repair_status == "repaired"


def test_beyond_window_marked_unrepairable():
    universe = {}
    q, tp, bf, rows, gaps = setup(universe)
    old_T = 1_000_000_000_000                       # 2001 年
    tp.on_ws(trade(1, T=old_T), 1, 1, 1, "c")
    tp.on_ws(trade(5, T=old_T + 1), 2, 2, 2, "c")
    asyncio.run(run_once(bf, tp))
    assert gaps[-1].repair_status == "unrepairable" and "window" in gaps[-1].note
    assert bf.rest.calls == []


def test_resume_from_checkpoint_detects_downtime_gap():
    gaps, events = [], []
    q = QualityRegistry("s", gaps.append, events.append)
    tp = TradeProcessor("BTCUSDT", q)
    tp.resume_from_checkpoint(last_a=100, last_T_ms=1_700_000_000_000, last_recv_ns=5)
    tp.on_ws(trade(150), 10, 10, 1, "c")
    assert len(gaps) == 1 and gaps[0].prev_known_id == 100 and gaps[0].next_known_id == 150
