import time

import pytest

from data.collector.quality import QualityRegistry
from data.collector.trades import TradeProcessor, aggressor_side, parse_agg_trade


def mk_q():
    gaps, events = [], []
    return QualityRegistry("s", gaps.append, events.append), gaps, events


def trade(a, m=True, p="100.0", q="1", T=None, E=None, **kw):
    T = int(time.time() * 1000) if T is None else T
    d = {"e": "aggTrade", "E": E if E is not None else T + 5, "s": "BTCUSDT", "a": a, "p": p, "q": q, "nq": q,
         "f": a * 10, "l": a * 10 + 1, "T": T, "m": m, "st": 1}
    d.update(kw)
    return d


# 7. 主动方向
def test_m_true_means_buyer_is_maker_so_aggressor_is_seller():
    assert aggressor_side(True) == "sell"
    assert parse_agg_trade(trade(1, m=True))["aggressor_side"] == "sell"


def test_m_false_means_buyer_is_taker_so_aggressor_is_buyer():
    assert aggressor_side(False) == "buy"
    assert parse_agg_trade(trade(1, m=False))["aggressor_side"] == "buy"


def test_parse_keeps_all_fields_and_strings():
    row = parse_agg_trade(trade(5, p="83900.10", q="0.095", nq="0.090", newfield=7))
    assert row["p"] == "83900.10" and row["q"] == "0.095" and row["nq"] == "0.090"
    assert row["st"] == 1 and '"newfield": 7' in row["extra_json"]


def test_parse_rejects_bad_types():
    with pytest.raises(ValueError):
        parse_agg_trade(trade(1, p=100.0))
    with pytest.raises(ValueError):
        parse_agg_trade({k: v for k, v in trade(1).items() if k != "m"})


def test_dedup_and_gap_detection():
    q, gaps, events = mk_q()
    tp = TradeProcessor("BTCUSDT", q)
    assert tp.on_ws(trade(10), 1, 1, 1, "c1") is not None
    assert tp.on_ws(trade(10), 2, 2, 2, "c2") is None                # 重复
    assert tp.on_ws(trade(11), 3, 3, 3, "c1") is not None
    assert tp.on_ws(trade(15), 4, 4, 4, "c1") is not None            # 12..14 缺
    assert len(gaps) == 1 and gaps[0].kind == "aggtrade_id" and gaps[0].repair_status == "open"
    assert gaps[0].prev_known_id == 11 and gaps[0].next_known_id == 15
    assert "[12,14]" in gaps[0].note
    c = q.c("BTCUSDT", "aggTrade")
    assert c.accepted == 3 and c.duplicates == 1 and c.extra["suspected_missing_trades"] == 3


def test_late_ws_messages_fill_gap():
    q, gaps, _ = mk_q()
    tp = TradeProcessor("BTCUSDT", q)
    tp.on_ws(trade(1), 1, 1, 1, "c1")
    tp.on_ws(trade(4), 2, 2, 2, "c1")
    for a in (2, 3):
        assert tp.on_ws(trade(a), 3, 3, 3, "c2") is not None
    assert gaps[-1].repair_status == "repaired" and "ws=2" in gaps[-1].note and tp.pending_gaps == {}


def test_backfill_rows_have_no_fake_recv_time_and_dedup_overlap():
    q, gaps, _ = mk_q()
    tp = TradeProcessor("BTCUSDT", q)
    tp.on_ws(trade(1), 1, 1, 1, "c1")
    tp.on_ws(trade(5), 2, 2, 2, "c1")
    rest_items = [{"a": a, "p": "1", "q": "1", "f": a, "l": a, "T": 1_700_000_000_000, "m": False} for a in (1, 2, 3, 4, 5)]
    rows = tp.on_backfill(rest_items, known_time_ns=99, request_id="r1")
    assert [r["a"] for r in rows] == [2, 3, 4]                       # 1、5 已有，不重复
    assert all(r["recv_time_ns"] is None and r["E_ms"] is None and r["source"] == "rest_backfill"
               and r["known_time_ns"] == 99 for r in rows)
    assert gaps[-1].repair_status == "repaired" and "rest_backfill=3" in gaps[-1].note
    # 重复执行不再增加
    assert tp.on_backfill(rest_items, 100, "r2") == []


def test_downtime_gap_closed_and_linked_on_first_trade_after_restart():
    q, gaps, _ = mk_q()
    tp = TradeProcessor("BTCUSDT", q)
    tp.resume_from_checkpoint(last_a=100, last_T_ms=None, last_recv_ns=None)
    tp.downtime_gap = q.open_gap("BTCUSDT", "aggTrade", "downtime", "certain", "process not running",
                                 None, None, 100, "local")
    tp.on_ws(trade(105), 10, 10, 1, "c")
    by_kind = {g.kind: g for g in gaps}
    assert by_kind["downtime"].repair_status == "not_applicable" and "[101,104]" in by_kind["downtime"].note
    assert by_kind["aggtrade_id"].repair_status == "open" and by_kind["aggtrade_id"].prev_known_id == 100
    assert tp.downtime_gap is None


def test_downtime_gap_no_missing_ids():
    q, gaps, _ = mk_q()
    tp = TradeProcessor("BTCUSDT", q)
    tp.resume_from_checkpoint(last_a=100, last_T_ms=None, last_recv_ns=None)
    tp.downtime_gap = q.open_gap("BTCUSDT", "aggTrade", "downtime", "certain", "x", None, None, 100, "local")
    tp.on_ws(trade(101), 10, 10, 1, "c")
    assert gaps[-1].kind == "downtime" and gaps[-1].repair_status == "not_applicable" and "no aggTrade id missing" in gaps[-1].note
    assert not any(g.kind == "aggtrade_id" for g in gaps)


def test_downtime_gap_without_watermark_is_unknown_not_clean():
    q, gaps, _ = mk_q()
    tp = TradeProcessor("BTCUSDT", q)          # 没有 resume_from_checkpoint
    tp.downtime_gap = q.open_gap("BTCUSDT", "aggTrade", "downtime", "certain", "x", None, None, None, "local")
    tp.on_ws(trade(500), 10, 10, 1, "c")
    tp.on_ws(trade(501), 11, 11, 2, "c")
    assert gaps[-1].kind == "downtime" and gaps[-1].repair_status == "unrepairable" and "unknown" in gaps[-1].note
