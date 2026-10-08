"""第一版特征：把盘口 + 成交对齐到固定时间栅格，再算未来收益标签。

用法：
    from strategy import features as F
    ds = F.make_dataset("BTCUSDT", "2026-09-25", "2026-09-26")   # pyarrow.Table
    X = np.column_stack([ds[c].to_numpy() for c in F.FEATURE_COLS]); y = ds["y_bps"].to_numpy()

对齐《实操指南》第二步：每秒一个样本，输入只用过去 5 秒，标签是未来 3 秒 mid 收益。六类特征：
    OBI   盘口不平衡      imb1 / imb20 / micro_bps      当前买卖挂单谁更多
    OFI   订单流不平衡    ofi                           过去 5 秒买一卖一价量变化的净流入（Cont 定义）
    Spread 买卖价差       spread_bps
    Depth 盘口深度        depth20_bid / depth20_ask     20 档挂单总量（采集只有 20 档）
    TFI   主动成交不平衡  buy_vol / sell_vol / n_trades / flow_imb
    RV    短期波动率      rv_bps                        过去 5 秒 mid 逐笔收益平方和的平方根
OFI 与 TFI 分别计算：前者来自报价变化，后者来自 aggTrade。

思路（刻意保持简单）：
1. 每条盘口更新算瞬时量（mid、spread、不平衡）和增量（OFI 增量、mid 收益）；
2. 每 step_ms（默认 1s）取一个栅格点，瞬时量取"栅格点之前最近一条盘口"（as-of，不看未来）；
3. 增量类（OFI、收益平方、成交）按 (t - window_ms, t] 窗口求和；
4. 标签 = 未来 horizon_ms 后 mid 的变化（bps），附带三分类 y_cls 备用；
5. 只在连续数据段内算，跨缺口的样本丢掉，增量在段首清零。
所有价格用 float64；输出 pyarrow.Table，列名即特征名。
注意：对齐用的是交易所时间 T_ms，本机收到要再晚约 150ms；按接收时间对齐留待下一步。
"""
from __future__ import annotations

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from data import loader as L

LEVELS = 20                     # 采集的全部档位；BTC 前 5 档只覆盖 0.5 USD，92% 的量都在第 1 档


# ---------- 1. 逐条盘口量 ----------

def book_features(book: pa.Table, levels: int = LEVELS) -> dict[str, np.ndarray]:
    """输入 loader.load('book_top20', ...) 的表，输出每条更新的瞬时量和增量数组。"""
    book = L.to_float(book)
    f = lambda c: book[c].to_numpy(zero_copy_only=False)
    bp = np.stack([f(f"bid_px_{i}") for i in range(levels)], axis=1)
    ap = np.stack([f(f"ask_px_{i}") for i in range(levels)], axis=1)
    bq = np.stack([f(f"bid_qty_{i}") for i in range(levels)], axis=1)
    aq = np.stack([f(f"ask_qty_{i}") for i in range(levels)], axis=1)
    b0, a0, bq0, aq0 = bp[:, 0], ap[:, 0], bq[:, 0], aq[:, 0]

    mid = (b0 + a0) / 2
    spread_bps = (a0 - b0) / mid * 1e4
    # 一档不平衡：买盘厚为正，卖盘厚为负，范围 [-1, 1]
    imb1 = (bq0 - aq0) / (bq0 + aq0)
    bq_sum, aq_sum = bq.sum(1), aq.sum(1)
    imb20 = (bq_sum - aq_sum) / (bq_sum + aq_sum)
    # 微观价格：按对手方数量加权，偏向数量少的一侧；相对 mid 的偏移（bps）
    micro = (b0 * aq0 + a0 * bq0) / (bq0 + aq0)
    micro_bps = (micro - mid) / mid * 1e4
    return {
        "T_ms": f("T_ms").astype(np.int64),
        "mid": mid, "spread_bps": spread_bps,
        "imb1": imb1, "imb20": imb20, "micro_bps": micro_bps,
        "depth20_bid": bq_sum, "depth20_ask": aq_sum,
        "ofi_inc": ofi_increment(b0, bq0, a0, aq0),
        "r2": log_return_sq_bps(mid),
    }


def ofi_increment(b: np.ndarray, bq: np.ndarray, a: np.ndarray, aq: np.ndarray) -> np.ndarray:
    """Cont, Kukanov & Stoikov (2014) 的一档订单流不平衡增量 e_n，第一条记 0。

    买价抬高或不变时新买量计入、买价降低或不变时旧买量扣除；卖侧符号相反。
    直觉：买方挂单变厚 / 卖方挂单变薄为正。"""
    e = np.zeros_like(b)
    pb, pbq, pa_, paq = b[:-1], bq[:-1], a[:-1], aq[:-1]
    cb, cbq, ca, caq = b[1:], bq[1:], a[1:], aq[1:]
    e[1:] = (np.where(cb >= pb, cbq, 0.0) - np.where(cb <= pb, pbq, 0.0)
             - np.where(ca <= pa_, caq, 0.0) + np.where(ca >= pa_, paq, 0.0))
    return e


def log_return_sq_bps(mid: np.ndarray) -> np.ndarray:
    """相邻两条盘口 mid 的对数收益（bps）的平方，第一条记 0；窗口求和开根号即已实现波动率。"""
    r2 = np.zeros_like(mid)
    r2[1:] = (np.diff(np.log(mid)) * 1e4) ** 2
    return r2


# ---------- 2/3. 对齐到栅格 ----------

def asof_index(times: np.ndarray, grid: np.ndarray) -> np.ndarray:
    """每个栅格点对应 times 里 <= 它的最后一个下标（times 需升序）。"""
    return np.searchsorted(times, grid, side="right") - 1


def window_sum(times: np.ndarray, values: np.ndarray, grid: np.ndarray, window_ms: int) -> np.ndarray:
    """每个栅格点 t 求 (t - window_ms, t] 内 values 之和；前缀和 + searchsorted，O(log n)。"""
    cum = np.concatenate([[0.0], np.cumsum(values, dtype=np.float64)])
    hi = np.searchsorted(times, grid, side="right")
    lo = np.searchsorted(times, grid - window_ms, side="right")
    return cum[hi] - cum[lo]


def trade_features(trades: pa.Table, grid: np.ndarray, window_ms: int) -> dict[str, np.ndarray]:
    """每个栅格点 t 统计 (t-window_ms, t] 内的成交：买量、卖量、笔数、买卖量差占比。"""
    trades = L.to_float(trades, ["p", "q"])
    T = trades["T_ms"].to_numpy()
    q = trades["q"].to_numpy(zero_copy_only=False)
    is_buy = pc.equal(trades["aggressor_side"], "buy").to_numpy(zero_copy_only=False)
    buy = window_sum(T, np.where(is_buy, q, 0.0), grid, window_ms)
    sell = window_sum(T, np.where(is_buy, 0.0, q), grid, window_ms)
    n = window_sum(T, np.ones_like(q), grid, window_ms)
    total = buy + sell
    flow = np.divide(buy - sell, total, out=np.zeros_like(total), where=total > 0)
    return {"buy_vol": buy, "sell_vol": sell, "n_trades": n, "flow_imb": flow}


# ---------- 4/5. 组装数据集 ----------

def make_dataset(symbol: str, start_day: str, end_day: str | None = None, *,
                 step_ms: int = 1_000, horizon_ms: int = 3_000, window_ms: int = 5_000,
                 max_gap_ms: int = 5_000, flat_bps: float = 0.5,
                 align_ms: int | None = None,
                 data_dir=L.DEFAULT_DATA_DIR) -> pa.Table:
    """返回每行一个栅格点的样本：特征列 + 标签列 (y_bps, y_cls) + 辅助列 (T_ms, mid, segment)。

    y_bps: 未来 horizon_ms 后 mid 相对现在的变化（bps）。
    y_cls: 1 = 涨超 flat_bps，-1 = 跌超，0 = 其余（备用，第一版按文档用回归）。
    align_ms: 给定时栅格点对齐到它的整数倍（如 60_000 = 整分钟），便于和 K 线拼接。
    """
    cols = ["T_ms"] + [f"{s}_{k}_{i}" for s in ("bid", "ask") for k in ("px", "qty") for i in range(LEVELS)]
    book = L.load("book_top20", symbol, start_day, end_day, data_dir=data_dir, columns=cols)
    trades = L.load("agg_trades", symbol, start_day, end_day, data_dir=data_dir,
                    columns=["T_ms", "p", "q", "aggressor_side"])
    if book.num_rows == 0:
        return pa.table({})
    bf = book_features(book)
    T = bf["T_ms"]
    segs = L.segments(book, "T_ms", max_gap_ms)
    # 增量不能跨缺口：每段第一条与上一段最后一条的差值清零
    for a, _ in segs:
        bf["ofi_inc"][a] = 0.0
        bf["r2"][a] = 0.0
    state_cols = ["mid", "spread_bps", "imb1", "imb20", "micro_bps", "depth20_bid", "depth20_ask"]

    parts: list[dict[str, np.ndarray]] = []
    for seg_no, (a, b) in enumerate(segs):
        t0, t1 = T[a], T[b - 1]
        # 栅格从段开始后 window_ms（让窗口有数据）到段结束前 horizon_ms（标签不越界）
        g0 = t0 + window_ms
        if align_ms:
            g0 = -(-g0 // align_ms) * align_ms      # 向上取整到 align_ms 的倍数
        grid = np.arange(g0, t1 - horizon_ms, step_ms, dtype=np.int64)
        if len(grid) == 0:
            continue
        now = asof_index(T, grid)                    # 当前盘口
        fut = asof_index(T, grid + horizon_ms)       # horizon 后的盘口
        row = {k: bf[k][now] for k in state_cols}
        row["ofi"] = window_sum(T, bf["ofi_inc"], grid, window_ms)
        row["rv_bps"] = np.sqrt(window_sum(T, bf["r2"], grid, window_ms))
        row.update(trade_features(trades, grid, window_ms))
        y = (bf["mid"][fut] - bf["mid"][now]) / bf["mid"][now] * 1e4
        row["y_bps"] = y
        row["y_cls"] = np.where(y > flat_bps, 1, np.where(y < -flat_bps, -1, 0)).astype(np.int8)
        row["T_ms"] = grid
        row["segment"] = np.full(len(grid), seg_no, dtype=np.int32)
        parts.append(row)
    if not parts:
        return pa.table({})
    return pa.table({k: np.concatenate([p[k] for p in parts]) for k in parts[0]})


FEATURE_COLS = [
    "imb1", "imb20", "micro_bps",                       # OBI
    "ofi",                                              # OFI
    "spread_bps",                                       # Spread
    "depth20_bid", "depth20_ask",                       # Depth
    "buy_vol", "sell_vol", "n_trades", "flow_imb",      # Trade Flow Imbalance
    "rv_bps",                                           # Realized Volatility
]
