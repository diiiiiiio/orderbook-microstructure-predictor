from data.collector.markprice import MarkPriceProcessor, parse_mark_price
from data.collector.quality import QualityRegistry


def mp(E, **kw):
    d = {"e": "markPriceUpdate", "E": E, "s": "BTCUSDT", "p": "83898.04885507", "ap": "83898.04885507",
         "P": "83963.57417104", "i": "83938.99760870", "r": "0.00005055", "T": 1790380800000, "st": 1}
    d.update(kw)
    return d


def test_parse_keeps_strings_and_fields():
    r = parse_mark_price(mp(1000, zz="x"))
    assert r["p"] == "83898.04885507" and r["r"] == "0.00005055" and r["T_next_funding_ms"] == 1790380800000
    assert r["st"] == 1 and '"zz"' in r["extra_json"]


def test_dedup_out_of_order_and_silence_gap():
    gaps, events = [], []
    q = QualityRegistry("s", gaps.append, events.append)
    p = MarkPriceProcessor("BTCUSDT", q, 1000, 5.0)
    assert p.on_ws(mp(1000), 1, 1, 1, "c") is not None
    assert p.on_ws(mp(1000), 2, 2, 2, "c") is None
    assert p.on_ws(mp(500), 3, 3, 3, "c") is None
    assert p.on_ws(mp(2000), 4, 4, 4, "c") is not None
    assert gaps == []
    assert p.on_ws(mp(9000), 5, 5, 5, "c") is not None
    assert len(gaps) == 1 and gaps[0].certainty == "suspected" and gaps[0].repair_status == "unrepairable"
    c = q.c("BTCUSDT", "markPrice")
    assert c.accepted == 3 and c.duplicates == 1 and c.old_or_out_of_order == 1
