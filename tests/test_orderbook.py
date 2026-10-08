from decimal import Decimal

import pytest

from data.collector.orderbook import OrderBook
from data.collector.records import BookOutputKind, BookState
from tests.helpers import SPEC, ev, snap


def make(**kw) -> OrderBook:
    return OrderBook("BTCUSDT", SPEC, export_levels=kw.pop("levels", 20), **kw)


def rows(outs):
    return [o.row for o in outs if o.kind == BookOutputKind.ROW]


# ---------- 1. 快照与缓存增量衔接（含边界） ----------
def test_link_first_event_straddles_last_update_id():
    ob = make()
    ob.on_event(ev(90, 95, 80))        # u < lastUpdateId → 丢弃
    ob.on_event(ev(96, 102, 95, bids=[["100.0", "5"]]))   # U<=100<=u → 第一条
    ob.on_event(ev(103, 110, 102, asks=[["100.1", "7"]]))
    outs = ob.on_snapshot(snap(100))
    assert ob.state == BookState.LIVE
    assert ob.epoch == 1
    r = rows(outs)
    assert [x.u for x in r] == [102, 110]
    assert r[0].bid_px[0] == "100.00" and r[0].bid_qty[0] == "5.000"
    assert r[1].ask_px[0] == "100.10" and r[1].ask_qty[0] == "7.000"
    assert "first_after_snapshot" in r[0].flags


@pytest.mark.parametrize("U,u", [(100, 100), (100, 105), (95, 100)])
def test_link_boundary_equalities(U, u):
    """U == lastUpdateId 或 u == lastUpdateId 都是合法的第一条。"""
    ob = make()
    ob.on_event(ev(U, u, U - 1))
    ob.on_snapshot(snap(100))
    assert ob.state == BookState.LIVE
    assert ob.last_u == u


def test_link_waits_when_buffer_all_older_than_snapshot():
    ob = make()
    ob.on_event(ev(80, 90, 70))
    outs = ob.on_snapshot(snap(100))
    assert outs == [] and ob.state == BookState.SYNCING
    outs = ob.on_event(ev(95, 105, 90))
    assert ob.state == BookState.LIVE and len(rows(outs)) == 1


def test_link_fails_when_snapshot_older_than_buffer():
    ob = make()
    ob.on_event(ev(120, 130, 110))
    outs = ob.on_snapshot(snap(100))
    assert outs[0].kind == BookOutputKind.NEED_SNAPSHOT
    assert ob.state == BookState.SYNCING and ob.snapshot_wanted
    assert ob.stats.snapshot_stale == 1


def test_snapshot_ignored_when_not_syncing():
    ob = make()
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100))
    outs = ob.on_snapshot(snap(200))
    assert outs[0].kind == BookOutputKind.DROPPED and ob.state == BookState.LIVE


# ---------- 2. 数量覆盖而非累加 ----------
def test_quantity_is_absolute_not_incremental():
    ob = make()
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100, bids=[["100.0", "1"]], asks=[["100.1", "1"]]))
    ob.on_event(ev(101, 101, 100, bids=[["100.0", "3"]]))
    ob.on_event(ev(102, 102, 101, bids=[["100.0", "2"]]))
    assert ob.bids[SPEC.price_to_ticks("100.0")] == 2000


# ---------- 3. 归零删除、删除未知价位 ----------
def test_zero_removes_and_unknown_removal_is_normal():
    ob = make()
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100, bids=[["100.0", "1"], ["99.9", "1"]], asks=[["100.1", "1"]]))
    outs = ob.on_event(ev(101, 101, 100, bids=[["99.9", "0"], ["50.0", "0"]]))
    assert ob.state == BookState.LIVE
    assert SPEC.price_to_ticks("99.9") not in ob.bids
    assert len(rows(outs)) == 1
    assert rows(outs)[0].bid_px == ["100.00"]


# ---------- 4. 重复/过旧不重复应用 ----------
def test_duplicate_and_old_events_not_reapplied():
    ob = make()
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100, bids=[["100.0", "1"]], asks=[["100.1", "1"]]))
    e = ev(101, 101, 100, bids=[["100.0", "5"]])
    ob.on_event(e)
    ob.on_event(ev(102, 102, 101, bids=[["100.0", "6"]]))
    outs = ob.on_event(e)                       # 重复（来自另一连接）
    assert outs[0].kind == BookOutputKind.DROPPED and outs[0].reason == "duplicate"
    outs = ob.on_event(ev(90, 95, 80))          # 过旧
    assert outs[0].kind == BookOutputKind.DROPPED and outs[0].reason == "old"
    assert ob.bids[SPEC.price_to_ticks("100.0")] == 6000
    assert ob.state == BookState.LIVE
    assert ob.stats.events_applied == 3


def test_same_u_different_content_is_conflict():
    ob = make()
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100))
    ob.on_event(ev(101, 101, 100, bids=[["100.0", "5"]]))
    outs = ob.on_event(ev(101, 101, 100, bids=[["100.0", "9"]], cid="c2"))
    assert outs[0].kind == BookOutputKind.CONFLICT
    assert ob.bids[SPEC.price_to_ticks("100.0")] == 5000   # 未被覆盖
    assert ob.stats.conflicts == 1


# ---------- 5. pu 断档 → INVALID → 重新同步 ----------
def test_pu_gap_invalidates_and_resync_creates_new_epoch():
    ob = make()
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100))
    outs = ob.on_event(ev(105, 110, 103))       # pu 应为 100
    assert outs[0].kind == BookOutputKind.STATE and outs[0].state == BookState.INVALID
    assert outs[0].reason == "pu_mismatch"
    assert ob.stats.seq_gaps == 1
    ob.start_sync("pu_mismatch")
    ob.on_event(ev(200, 200, 199))
    ob.on_snapshot(snap(200))
    assert ob.state == BookState.LIVE and ob.epoch == 2


def test_pu_gap_inside_buffer_during_sync_invalidates():
    ob = make()
    ob.on_event(ev(100, 100, 99))
    ob.on_event(ev(105, 105, 103))              # 缓存内部断档
    outs = ob.on_snapshot(snap(100))
    assert ob.state == BookState.INVALID
    assert any(o.reason == "pu_mismatch_during_sync" for o in outs)


# ---------- 6. 无效期间不产生有效盘口 ----------
def test_no_valid_rows_while_invalid_or_syncing():
    ob = make()
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100))
    ob.on_event(ev(105, 110, 103))
    assert ob.state == BookState.INVALID
    outs = ob.on_event(ev(111, 111, 110, bids=[["100.0", "1"]]))
    assert rows(outs) == [] and outs[0].reason == "book_invalid"
    ob.start_sync("test")
    outs = ob.on_event(ev(112, 112, 111))
    assert rows(outs) == []
    for r in rows(ob.on_snapshot(snap(112))):
        assert r.is_valid and r.book_epoch == 2


def test_crossed_book_invalidates():
    ob = make()
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100, bids=[["100.0", "1"]], asks=[["100.1", "1"]]))
    outs = ob.on_event(ev(101, 101, 100, bids=[["100.1", "1"]]))
    assert ob.state == BookState.INVALID and outs[0].reason == "crossed_book"
    assert rows(outs) == []


def test_bad_price_precision_invalidates_without_partial_apply():
    ob = make()
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100, bids=[["100.0", "1"]], asks=[["100.1", "1"]]))
    outs = ob.on_event(ev(101, 101, 100, bids=[["99.9", "5"], ["99.95", "1"]]))
    assert ob.state == BookState.INVALID and outs[0].reason == "event_parse_error"
    assert SPEC.price_to_ticks("99.9") not in ob.bids   # 整条消息都未应用


# ---------- 覆盖范围 ----------
def test_coverage_boundary_marks_invalid_when_top_levels_unconfirmed():
    bids = [[f"{100 - i * 0.1:.1f}", "1"] for i in range(5)]   # 5 档且 limit=5 → 有边界
    asks = [[f"{100.1 + i * 0.1:.1f}", "1"] for i in range(5)]
    ob = make(levels=3)
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100, bids=bids, asks=asks, limit=5))
    assert ob.state == BookState.LIVE and ob.cov_bid_floor == SPEC.price_to_ticks("99.6")
    # 删掉前 3 档买单 → 第 3 档变成 99.6，仍在覆盖内
    outs = ob.on_event(ev(101, 101, 100, bids=[["100.0", "0"], ["99.9", "0"]]))
    assert ob.state == BookState.LIVE and rows(outs)[0].bid_px == ["99.80", "99.70", "99.60"]
    # 再删两档 → 只剩 1 档在覆盖范围内，前 3 档完整性无法确认
    outs = ob.on_event(ev(102, 102, 101, bids=[["99.8", "0"], ["99.7", "0"]]))
    assert ob.state == BookState.INVALID and outs[0].reason == "top_levels_coverage_unconfirmed"


def test_full_coverage_when_snapshot_smaller_than_limit():
    ob = make(levels=20)
    ob.on_event(ev(100, 100, 99))
    ob.on_snapshot(snap(100, bids=[["100.0", "1"]], asks=[["100.1", "1"]], limit=1000))
    assert ob.cov_bid_floor is None
    outs = ob.on_event(ev(101, 101, 100, bids=[["100.0", "0"]]))
    assert ob.state == BookState.LIVE and rows(outs)[0].bid_px == []


# ---------- STALE ----------
def test_stale_then_resume_with_continuous_pu():
    ob = make(stale_after_ms=1000)
    ob.on_event(ev(100, 100, 99, recv=1_000_000_000))
    ob.on_snapshot(snap(100))
    out = ob.check_stale(now_ns=3_000_000_000)
    assert out is not None and ob.state == BookState.STALE
    outs = ob.on_event(ev(101, 101, 100, recv=3_100_000_000))
    assert ob.state == BookState.LIVE and "resumed_after_stale" in rows(outs)[0].flags


def test_buffer_overflow_requests_new_snapshot():
    ob = make(buffer_max_events=3)
    for i in range(3):
        ob.on_event(ev(100 + i, 100 + i, 99 + i))
    outs = ob.on_event(ev(103, 103, 102))
    assert outs[0].kind == BookOutputKind.NEED_SNAPSHOT and ob.stats.buffer_overflows == 1
    assert ob.buffer == []


# ---------- 8. 定点数往返 ----------
@pytest.mark.parametrize("p", ["83900.10", "0.10", "556.80", "4529764.00", "83900.1"])
def test_price_roundtrip_lossless(p):
    t = SPEC.price_to_ticks(p)
    assert SPEC.ticks_to_price(t) == Decimal(p)


@pytest.mark.parametrize("q", ["6.088", "0.001", "1000.000", "139.440"])
def test_qty_roundtrip_lossless(q):
    n = SPEC.qty_to_steps(q)
    assert SPEC.steps_to_qty(n) == Decimal(q)


def test_precision_violation_raises_not_rounds():
    from data.collector.numeric import PrecisionError
    with pytest.raises(PrecisionError):
        SPEC.price_to_ticks("83900.15")
    with pytest.raises(PrecisionError):
        SPEC.qty_to_steps("0.0005")
    with pytest.raises(PrecisionError):
        SPEC.price_to_ticks(83900.1)   # float 不接受


def test_spec_validation_rejects_non_usdt_perpetual():
    from data.collector.numeric import SymbolSpec, validate_usdt_perpetual, PrecisionError
    ok = {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT",
          "marginAsset": "USDT", "pricePrecision": 2, "quantityPrecision": 3,
          "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.10"}, {"filterType": "LOT_SIZE", "stepSize": "0.001"}]}
    validate_usdt_perpetual(SymbolSpec.from_exchange_info_symbol(ok))
    bad = dict(ok, contractType="CURRENT_QUARTER")
    with pytest.raises(PrecisionError):
        validate_usdt_perpetual(SymbolSpec.from_exchange_info_symbol(bad))
    coin = dict(ok, quoteAsset="USD", marginAsset="BTC")
    with pytest.raises(PrecisionError):
        validate_usdt_perpetual(SymbolSpec.from_exchange_info_symbol(coin))
