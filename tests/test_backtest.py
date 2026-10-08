"""backtest.engine + strategy.model：逐档吃单、成本扣减、跳过条件、时间切分、指标。"""
from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from backtest import engine as E
from strategy import model as M
from strategy.features import LEVELS


def _book(rows: list[tuple[int, float, float]]) -> pa.Table:
    """rows = [(T_ms, bid_px_0, ask_px_0)]；其余档位按 tick 递推，每档数量 1.0。"""
    d: dict[str, list] = {"T_ms": [r[0] for r in rows]}
    for i in range(LEVELS):
        d[f"bid_px_{i}"] = [f"{r[1] - i * 0.1:.2f}" for r in rows]
        d[f"ask_px_{i}"] = [f"{r[2] + i * 0.1:.2f}" for r in rows]
        d[f"bid_qty_{i}"] = ["1.000"] * len(rows)
        d[f"ask_qty_{i}"] = ["1.000"] * len(rows)
    return pa.table(d)


def test_walk_book_weighted_average_and_partial_fill():
    px, qty = np.array([100.0, 101.0, 102.0]), np.array([1.0, 2.0, 3.0])
    p, f = E.walk_book(px, qty, 2.5)
    assert f == pytest.approx(2.5) and p == pytest.approx((100 + 1.5 * 101) / 2.5)
    p, f = E.walk_book(np.array([100.0, 101.0]), np.array([1.0, 1.0]), 5.0)
    assert f == pytest.approx(2.0) and p == pytest.approx(100.5)      # 深度不足只吃到 2
    assert np.isnan(E.walk_book(np.array([100.0]), np.array([0.0]), 1.0)[0])


def test_long_trade_pays_spread_and_fees():
    # 价格不动：买入吃 ask 100.10、卖出吃 bid 100.00，毛收益 = -价差，净收益再扣 2×taker
    book = _book([(t, 100.00, 100.10) for t in range(0, 20_000, 100)])
    tr = E.taker_backtest(book, np.array([1_000]), np.array([5.0]), enter_bps=0.5,
                          size=0.5, delay_ms=100, horizon_ms=3_000, taker_bps=5.0)
    assert tr.num_rows == 1
    r = tr.to_pylist()[0]
    assert r["side"] == 1 and r["entry_px"] == pytest.approx(100.10) and r["exit_px"] == pytest.approx(100.00)
    assert r["gross_bps"] == pytest.approx((100.00 - 100.10) / 100.10 * 1e4)
    assert r["net_bps"] == pytest.approx(r["gross_bps"] - 10.0)
    assert r["entry_T_ms"] == 1_100 and r["exit_T_ms"] == 4_100     # 延迟 100ms + 持有 3s


def test_short_trade_profits_when_price_falls():
    rows = [(t, 100.00, 100.10) for t in range(0, 1_200, 100)]
    rows += [(t, 99.00, 99.10) for t in range(1_200, 20_000, 100)]
    tr = E.taker_backtest(_book(rows), np.array([1_000]), np.array([-5.0]), enter_bps=0.5,
                          size=0.5, delay_ms=100, horizon_ms=3_000, taker_bps=0.0)
    r = tr.to_pylist()[0]
    assert r["side"] == -1 and r["gross_bps"] > 0                    # 卖 100.00 买回 99.10
    assert r["gross_bps"] == pytest.approx(-1 * (99.10 - 100.00) / 100.00 * 1e4)


def test_below_threshold_produces_no_trade_and_one_position_at_a_time():
    book = _book([(t, 100.00, 100.10) for t in range(0, 30_000, 100)])
    tr = E.taker_backtest(book, np.array([1_000]), np.array([0.1]), enter_bps=0.5, size=0.5)
    assert tr.num_rows == 0 and E.metrics(tr)["n_signals"] == 0
    # 三个 1s 间隔的信号、持有 3s：只有第 1 个和最后 1 个能成交
    sigs = np.array([1_000, 2_000, 3_000, 4_100, 5_000])
    tr = E.taker_backtest(book, sigs, np.full(5, 5.0), enter_bps=0.5, size=0.5,
                          delay_ms=100, horizon_ms=3_000)
    m = E.metrics(tr)
    assert m["n_signals"] == 5 and m["skipped"]["in_position"] == 3 and tr.num_rows == 2


def test_stale_and_thin_book_are_skipped_not_faked():
    gap = _book([(0, 100.00, 100.10), (1_000, 100.00, 100.10)])      # 1s 后再无盘口
    m = E.metrics(E.taker_backtest(gap, np.array([1_000]), np.array([5.0]),
                                   enter_bps=0.5, size=0.5, max_stale_ms=1_000))
    assert m["n_trades"] == 0 and m["skipped"]["stale_book"] == 1
    book = _book([(t, 100.00, 100.10) for t in range(0, 20_000, 100)])
    m = E.metrics(E.taker_backtest(book, np.array([1_000]), np.array([5.0]),
                                   enter_bps=0.5, size=999.0))       # 20 档共 20.0，吃不下
    assert m["n_trades"] == 0 and m["skipped"]["thin_book"] == 1


def test_metrics_sharpe_drawdown_and_empty():
    empty = E.taker_backtest(_book([(0, 100.0, 100.1)]), np.array([]), np.array([]))
    m = E.metrics(empty)
    assert m["n_trades"] == 0 and "sharpe_annual" not in m
    book = _book([(t, 100.00, 100.10) for t in range(0, 200_000, 100)])
    sigs = np.arange(1_000, 100_000, 4_000)
    tr = E.taker_backtest(book, sigs, np.full(len(sigs), 5.0), enter_bps=0.5, size=0.5, taker_bps=5.0)
    m = E.metrics(tr)
    assert m["n_trades"] == len(sigs) and m["n_long"] == len(sigs) and m["n_short"] == 0
    assert m["net_bps_mean"] < 0 and m["hit_rate"] == 0.0            # 平盘 + 成本 → 必亏
    assert m["max_drawdown_bps"] == pytest.approx(-m["net_bps_total"])
    assert m["sharpe_annual"] == 0.0                                 # 每单完全一样，方差为 0
    assert m["hold_ms_mean"] == pytest.approx(3_000) and m["trades_per_day"] > 0


def test_sharpe_negative_when_returns_vary():
    rows, px = [], 100.00
    for k, t in enumerate(range(0, 200_000, 100)):
        px = 100.00 + (1.50 if (t // 4_000) % 2 else -0.03)          # 有的单毛收益为正
        rows.append((t, px, px + 0.10))
    sigs = np.arange(1_000, 100_000, 4_000)
    tr = E.taker_backtest(_book(rows), sigs, np.full(len(sigs), 5.0), enter_bps=0.5,
                          size=0.5, taker_bps=5.0)
    m = E.metrics(tr)
    assert m["net_bps_std"] > 0 and m["sharpe_annual"] < 0            # 成本吃掉全部边际
    assert m["sharpe_per_trade"] < 0 and abs(m["sharpe_per_trade"]) < abs(m["sharpe_annual"])
    assert 0.0 < m["gross_hit_rate"] < 1.0


def test_time_split_embargoes_label_leakage():
    T = np.arange(0, 100_000, 1_000)
    sp = M.time_split(T, frac=(0.6, 0.2), horizon_ms=3_000)
    assert len(sp.train) == 58 and len(sp.valid) == 18 and len(sp.test) == 20
    assert sp.train.max() < sp.valid.min() < sp.test.min()
    assert T[sp.train].max() + 3_000 <= T[sp.valid.min()]            # 训练标签不进验证集时段


def test_ridge_recovers_coefficients_and_scores():
    rng = np.random.default_rng(7)
    X = rng.normal(size=(800, 3))
    y = X @ [1.0, -2.0, 0.0] + rng.normal(scale=0.1, size=800)
    fit = M.fit_ridge(X, y, alpha=1e-8, cols=["a", "b", "c"])
    c = fit.coefs()
    assert list(c)[:2] == ["b", "a"] and c["b"] < -1.9 and abs(c["c"]) < 0.05
    sc = M.score(M.predict(fit, X), y)
    assert sc["corr"] > 0.99 and sc["rmse"] < sc["rmse_baseline_zero"]
    assert sc["rmse_improvement"] > 0.9 and sc["monotonic_pairs"] == 1.0
    assert len(sc["deciles"]) == 10 and sum(d["n"] for d in sc["deciles"]) == 800
