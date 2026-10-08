import json

import pytest
from aiohttp.test_utils import TestClient, TestServer

from data.collector.config import CollectorConfig
from data.collector.dashboard import Dashboard
from data.collector.records import BookRow, GapRecord
from data.collector.storage.normalized import NormalizedWriter


def seed(tmp_path):
    nw = NormalizedWriter(tmp_path, "s1", levels=20, schema_version=1, batch_rows=100, flush_interval_s=1e9, compression="zstd")
    t0 = 1_767_225_600_000_000_000
    for i in range(5):
        nw.add_book_row(BookRow("BTCUSDT", 1, 10 + i, 9 + i, 8 + i, 1767225600000 + i, 1767225600000 + i, t0 + i * 10**8, 5, i, "c1",
                                ["100.00", "99.90"], ["1.000", "2.000"], ["100.10"], ["3.000"], 2, 1, True, "LIVE", []))
    nw.add_trade({"symbol": "BTCUSDT", "a": 7, "p": "100.00", "q": "1", "nq": "1", "f": 1, "l": 1, "T_ms": 1767225600000, "E_ms": None,
                  "m": True, "aggressor_side": "sell", "st": 1, "source": "rest_backfill", "recv_time_ns": None, "known_time_ns": t0,
                  "recv_seq": None, "connection_id": "r1", "extra_json": None})
    g = GapRecord("g1", "BTCUSDT", "aggTrade", "aggtrade_id", "certain", "jump", t0, t0, t0 + 1, None, None, "local", 6, 8, "open", "s1")
    nw.add_gap(g)
    g.repair_status = "repaired"; g.update_time_ns = t0 + 5
    nw.add_gap(g)
    nw.close()
    (tmp_path / "state").mkdir()
    (tmp_path / "state/status.json").write_text(json.dumps({"time_ns": t0, "health": {"level": "ok", "reasons": []}, "session_id": "s1",
                                                            "pipeline": {}, "symbols": {}, "connections": {}}))
    (tmp_path / "reports").mkdir()
    (tmp_path / "reports/20260101.md").write_text("# report")


@pytest.mark.asyncio
async def test_dashboard_endpoints(tmp_path):
    seed(tmp_path)
    cfg = CollectorConfig(symbols=["BTCUSDT"], data_dir=str(tmp_path))
    async with TestClient(TestServer(Dashboard(cfg).app())) as c:
        r = await (await c.get("/")).text()
        assert "<title>" in r
        st = await (await c.get("/api/status")).json()
        assert st["_meta"]["exists"] and st["_meta"]["collector_alive"] is False   # 老 status → 离线
        inv = await (await c.get("/api/inventory")).json()
        d = inv["tables"]["book_top20"]["BTCUSDT"]["days"]["20260101"]
        assert d["rows"] == 5 and d["files"] == 1 and d["t_min"] < d["t_max"]
        assert inv["days"] == ["20260101"]
        book = await (await c.get("/api/book?symbol=BTCUSDT")).json()
        assert book["row"]["u"] == 14 and book["book"]["bids"][0] == ["100.00", "1.000"] and len(book["book"]["asks"]) == 1
        tr = await (await c.get("/api/trades?symbol=BTCUSDT")).json()
        assert tr["trades"][0]["a"] == 7 and tr["trades"][0]["recv_time_ns"] is None
        se = await (await c.get("/api/series?symbol=BTCUSDT&day=20260101")).json()
        assert len(se["points"]) == 5 and abs(se["points"][0]["mid"] - 100.05) < 1e-9
        gaps = await (await c.get("/api/gaps")).json()
        assert gaps["total"] == 1 and gaps["gaps"][0]["repair_status"] == "repaired"   # 取最新状态
        h = await (await c.get("/api/hourly?symbol=BTCUSDT&day=20260101")).json()
        assert h["hourly"]["book_top20"][0] == 5 and h["hourly"]["agg_trades"][0] == 1
        rep = await (await c.get("/api/reports")).json()
        assert rep["reports"][0]["day"] == "20260101"
        assert (await (await c.get("/api/report/20260101")).text()) == "# report"
        assert (await c.get("/api/report/../x")).status in (400, 404)

@pytest.mark.asyncio
async def test_backtest_endpoints(tmp_path):
    cfg = CollectorConfig(symbols=["BTCUSDT"], data_dir=str(tmp_path))
    async with TestClient(TestServer(Dashboard(cfg).app())) as c:
        empty = await (await c.get("/api/bt/list")).json()
        assert empty["reports"] == [] and "backtest.run" in empty["note"]     # 没报告时给出命令提示
        d = tmp_path / "reports" / "backtest"
        d.mkdir(parents=True)
        (d / "BTCUSDT_2026-01-01_2026-01-02.json").write_text(json.dumps({"symbol": "BTCUSDT"}))
        lst = await (await c.get("/api/bt/list")).json()
        assert [r["name"] for r in lst["reports"]] == ["BTCUSDT_2026-01-01_2026-01-02"]
        rep = await (await c.get("/api/bt/report/BTCUSDT_2026-01-01_2026-01-02")).json()
        assert rep["symbol"] == "BTCUSDT"
        assert (await c.get("/api/bt/report/nope")).status == 404
        assert (await c.get("/api/bt/report/..%2F..%2Fetc%2Fpasswd")).status == 400
        html = await (await c.get("/backtest")).text()
        assert "回测结果" in html and "夏普" not in html.split("<script>")[0]
