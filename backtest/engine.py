"""Taker（主动吃单）回测：把预测变成秒级信号，按盘口逐档吃单模拟成交，算每单收益与风险指标。

对齐《实操指南》第四步，先不模拟挂单排队：
- 信号：pred >= enter_bps 做多，<= -enter_bps 做空，其余不动；
- 延迟：信号时刻 t 之后 delay_ms 才下单，用 t+delay_ms 的盘口成交；
- 成交：买入吃卖盘、卖出吃买盘，逐档吃到指定数量并取加权均价（体现价差与数量滑点）；
- 平仓：持有 horizon_ms 后吃对手盘，不用中间价假装成交；
- 同一时间最多一笔持仓，持仓期间的信号忽略；
- 成本：开平各扣 taker_bps 手续费；价差与滑点已由逐档吃单体现，不再重复扣。

用法：
    from backtest import engine as E
    trades = E.taker_backtest(book, sig_T, pred)      # pyarrow.Table，每行一单
    m = E.metrics(trades)                             # 夏普、胜率、最大回撤等
盘口若在成交时刻已过期（超过 max_stale_ms 没更新，通常是断线缺口），该单跳过并计入 skipped。
"""
from __future__ import annotations

import numpy as np
import pyarrow as pa

from data import loader as L
from strategy.features import LEVELS, asof_index

TAKER_BPS = 5.0        # 币安 USDⓈ-M 永续 VIP0 taker 0.05%；maker 0.02%
YEAR_MS = 365 * 24 * 3_600_000


def book_arrays(book: pa.Table, levels: int = LEVELS) -> dict[str, np.ndarray]:
    """把盘口表转成回测要用的数值数组：T_ms 以及 (n, levels) 的四个价量矩阵。"""
    book = L.to_float(book)
    f = lambda c: book[c].to_numpy(zero_copy_only=False)
    return {
        "T_ms": f("T_ms").astype(np.int64),
        "bid_px": np.stack([f(f"bid_px_{i}") for i in range(levels)], axis=1),
        "bid_qty": np.stack([f(f"bid_qty_{i}") for i in range(levels)], axis=1),
        "ask_px": np.stack([f(f"ask_px_{i}") for i in range(levels)], axis=1),
        "ask_qty": np.stack([f(f"ask_qty_{i}") for i in range(levels)], axis=1),
    }


def walk_book(px: np.ndarray, qty: np.ndarray, size: float) -> tuple[float, float]:
    """逐档吃单：返回 (加权均价, 实际成交数量)。深度不够时只成交可吃到的部分。"""
    left, cost, filled = size, 0.0, 0.0
    for p, q in zip(px, qty):
        if left <= 0:
            break
        if not (q > 0) or not (p > 0):
            continue
        take = min(left, q)
        cost += take * p
        filled += take
        left -= take
    return (cost / filled if filled > 0 else float("nan")), filled


def taker_backtest(book: pa.Table, sig_T: np.ndarray, pred: np.ndarray, *,
                   enter_bps: float = 0.5, size: float = 0.01, delay_ms: int = 100,
                   horizon_ms: int = 3_000, taker_bps: float = TAKER_BPS,
                   max_stale_ms: int = 1_000, levels: int = LEVELS) -> pa.Table:
    """按信号逐单模拟，返回每行一单的表。

    列：side(1 多 / -1 空)、signal_T_ms、entry_T_ms、exit_T_ms、pred_bps、
        entry_px、exit_px、qty、gross_bps、fee_bps、net_bps、cum_bps、hold_ms。
    元信息放在 schema metadata：n_signals / n_skipped / 参数。
    """
    b = book_arrays(book, levels)
    T = b["T_ms"]
    order = np.argsort(sig_T, kind="stable")
    sig_T, pred = np.asarray(sig_T)[order].astype(np.int64), np.asarray(pred)[order].astype(np.float64)
    want = np.abs(pred) >= enter_bps

    rows: list[tuple] = []
    skipped = {"stale_book": 0, "thin_book": 0, "no_data": 0, "in_position": 0}
    free_at = np.int64(-1)
    for i in np.flatnonzero(want):
        t = sig_T[i]
        if t < free_at:
            skipped["in_position"] += 1
            continue
        side = 1 if pred[i] > 0 else -1
        t_in, t_out = t + delay_ms, t + delay_ms + horizon_ms
        k_in, k_out = asof_index(T, np.array([t_in, t_out]))
        if k_in < 0 or k_out < 0:
            skipped["no_data"] += 1
            continue
        if (t_in - T[k_in]) > max_stale_ms or (t_out - T[k_out]) > max_stale_ms:
            skipped["stale_book"] += 1      # 成交时刻盘口过期：断线缺口，不能假装成交
            continue
        if side == 1:
            px_in, f_in = walk_book(b["ask_px"][k_in], b["ask_qty"][k_in], size)
            px_out, f_out = walk_book(b["bid_px"][k_out], b["bid_qty"][k_out], size)
        else:
            px_in, f_in = walk_book(b["bid_px"][k_in], b["bid_qty"][k_in], size)
            px_out, f_out = walk_book(b["ask_px"][k_out], b["ask_qty"][k_out], size)
        if f_in < size or f_out < size:
            skipped["thin_book"] += 1       # 20 档吃不下这个数量，先不成交而不是假装吃到
            continue
        gross = side * (px_out - px_in) / px_in * 1e4
        fee = 2 * taker_bps
        rows.append((side, int(t), int(t_in), int(t_out), pred[i],
                     px_in, px_out, size, gross, fee, gross - fee, t_out - t_in))
        free_at = t_out                     # 同一时间最多一笔持仓

    cols = ["side", "signal_T_ms", "entry_T_ms", "exit_T_ms", "pred_bps",
            "entry_px", "exit_px", "qty", "gross_bps", "fee_bps", "net_bps", "hold_ms"]
    if not rows:
        data = {c: np.array([], dtype=np.float64) for c in cols}
    else:
        arr = list(zip(*rows))
        data = {c: np.array(v, dtype=np.float64) for c, v in zip(cols, arr)}
    for c in ("side", "signal_T_ms", "entry_T_ms", "exit_T_ms", "hold_ms"):
        data[c] = data[c].astype(np.int64)
    data["cum_bps"] = np.cumsum(data["net_bps"])
    meta = {"n_signals": str(int(want.sum())), "enter_bps": str(enter_bps), "size": str(size),
            "delay_ms": str(delay_ms), "horizon_ms": str(horizon_ms), "taker_bps": str(taker_bps),
            **{f"skipped_{k}": str(v) for k, v in skipped.items()}}
    return pa.table(data).replace_schema_metadata(meta)


def metrics(trades: pa.Table) -> dict:
    """每单收益统计 + 夏普 + 最大回撤。收益单位 bps，夏普按实际交易频率年化。"""
    n = trades.num_rows
    out: dict = {"n_trades": n}
    out.update({k: v for k, v in (trades.schema.metadata or {}).items()} if False else {})
    md = {k.decode(): v.decode() for k, v in (trades.schema.metadata or {}).items()}
    out["n_signals"] = int(md.get("n_signals", 0))
    out["skipped"] = {k[8:]: int(v) for k, v in md.items() if k.startswith("skipped_")}
    if n == 0:
        return out
    net = trades["net_bps"].to_numpy()
    gross = trades["gross_bps"].to_numpy()
    side = trades["side"].to_numpy()
    t0, t1 = trades["signal_T_ms"][0].as_py(), trades["exit_T_ms"][n - 1].as_py()
    span_ms = max(t1 - t0, 1)
    cum = np.concatenate([[0.0], np.cumsum(net)])   # 从净值 0 起算，首单亏损也算进回撤
    out.update({
        "n_long": int((side == 1).sum()), "n_short": int((side == -1).sum()),
        "gross_bps_mean": float(gross.mean()), "net_bps_mean": float(net.mean()),
        "net_bps_total": float(net.sum()), "net_bps_std": float(net.std(ddof=1)) if n > 1 else 0.0,
        "hit_rate": float((net > 0).mean()), "gross_hit_rate": float((gross > 0).mean()),
        "max_drawdown_bps": float((np.maximum.accumulate(cum) - cum).max()),
        "hold_ms_mean": float(trades["hold_ms"].to_numpy().mean()),
        "span_hours": span_ms / 3.6e6, "trades_per_day": n / (span_ms / 8.64e7),
    })
    if n > 1 and net.std(ddof=1) > 0:
        # 每单夏普是原始量级；年化只是乘 sqrt(年内单数)，高频下会放大到很大的绝对值
        out["sharpe_per_trade"] = float(net.mean() / net.std(ddof=1))
        out["sharpe_annual"] = float(out["sharpe_per_trade"] * np.sqrt(n / (span_ms / YEAR_MS)))
    else:
        out["sharpe_per_trade"] = 0.0
        out["sharpe_annual"] = 0.0
    return out
