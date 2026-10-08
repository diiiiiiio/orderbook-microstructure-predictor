"""把分钟 K 线特征和盘口微观结构特征拼到同一张表，测分钟级预测。

用法：
    from strategy import combined as C
    ds = C.make_dataset("BTCUSDT", "2026-09-25", "2026-09-26", horizon_min=5)
    # 列：T_ms、y_bps，K 线特征（klines.FEATURE_COLS），微观特征（features.FEATURE_COLS 加后缀）

K 线不用币安文件而是从 agg_trades 自建，保证和微观特征同源、同口径；
采集重启造成的缺分钟从币安日度文件补价格和量（价格三项与自建 99% 完全一致）。
n_trades 用 aggTrade 条数，比币安撮合数少约 60%，但在本表内口径一致，只影响 trade_z 的尺度。

对齐规则（无未来泄漏）：
- 样本时刻 t 是整分钟。K 线用 open_time = t - 60s 那根，它覆盖 [t-60s, t)，在 t 时刻已收完。
- 微观特征在 t 时刻 as-of 取盘口、(t-window, t] 窗口取增量，和 features.make_dataset 一致。
- 标签 y = mid(t + horizon) 相对 mid(t)，来自盘口，三组特征共用同一个标签。
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from data import loader as L
from strategy import features as F
from strategy import klines as K

BAR_MS = 60_000
KLINE_RAW = Path(__file__).resolve().parents[1] / "data" / "store" / "klines" / "raw"


# ---------- 1. 自建 K 线 ----------

def bars_from_trades(symbol: str, start_day: str, end_day: str | None = None, *,
                     data_dir=L.DEFAULT_DATA_DIR) -> dict[str, np.ndarray]:
    tr = L.load("agg_trades", symbol, start_day, end_day, data_dir=data_dir,
                columns=["T_ms", "p", "q", "aggressor_side"])
    tr = L.to_float(tr, ["p", "q"])
    T = tr["T_ms"].to_numpy()
    p = tr["p"].to_numpy(zero_copy_only=False)
    q = tr["q"].to_numpy(zero_copy_only=False)
    buy = pc.equal(tr["aggressor_side"], "buy").to_numpy(zero_copy_only=False)
    o = np.argsort(T, kind="stable")
    T, p, q, buy = T[o], p[o], q[o], buy[o]
    m = (T // BAR_MS) * BAR_MS
    uniq, first = np.unique(m, return_index=True)
    last = np.r_[first[1:], len(m)] - 1
    return {
        "T_ms": uniq.astype(np.float64), "open": p[first], "close": p[last],
        "high": np.maximum.reduceat(p, first), "low": np.minimum.reduceat(p, first),
        "volume": np.add.reduceat(q, first), "quote_volume": np.add.reduceat(p * q, first),
        "n_trades": np.diff(np.r_[first, len(m)]).astype(np.float64),
        "taker_buy_volume": np.add.reduceat(np.where(buy, q, 0.0), first),
    }


def _binance_daily(symbol: str, raw_dir: Path) -> dict[str, np.ndarray] | None:
    files = sorted(raw_dir.glob(f"{symbol}-1m-????-??-??.zip"))
    if not files:
        return None
    rows = []
    for zp in files:
        with zipfile.ZipFile(zp) as z:
            for line in z.open(z.namelist()[0]):
                if line[:1].isdigit():
                    rows.append(line.decode().split(","))
    a = np.array(rows, dtype=np.float64)
    a = a[np.argsort(a[:, 0], kind="stable")]
    return {"T_ms": a[:, 0], "open": a[:, 1], "high": a[:, 2], "low": a[:, 3], "close": a[:, 4],
            "volume": a[:, 5], "quote_volume": a[:, 7], "n_trades": a[:, 8],
            "taker_buy_volume": a[:, 9]}


def fill_gaps(bars: dict[str, np.ndarray], ref: dict[str, np.ndarray] | None) -> tuple[dict, int]:
    """自建 K 线缺的分钟从币安文件补；n_trades 按重叠段中位比例缩放到 aggTrade 口径。"""
    if ref is None:
        return bars, 0
    T = bars["T_ms"]
    full = np.arange(T[0], T[-1] + BAR_MS, BAR_MS)
    missing = np.setdiff1d(full, T)
    missing = missing[np.isin(missing, ref["T_ms"])]
    if len(missing) == 0:
        return bars, 0
    common = np.intersect1d(T, ref["T_ms"])
    ratio = np.median(bars["n_trades"][np.searchsorted(T, common)]
                      / np.maximum(ref["n_trades"][np.searchsorted(ref["T_ms"], common)], 1.0))
    ir = np.searchsorted(ref["T_ms"], missing)
    out = {}
    for k in bars:
        add = ref[k][ir] * (ratio if k == "n_trades" else 1.0)
        merged = np.concatenate([bars[k], add])
        out[k] = merged
    order = np.argsort(out["T_ms"], kind="stable")
    return {k: v[order] for k, v in out.items()}, len(missing)


# ---------- 2. 拼接 ----------

MICRO_COLS = list(F.FEATURE_COLS)


def make_dataset(symbol: str, start_day: str, end_day: str | None = None, *,
                 horizon_min: int = 5, micro_windows_s: tuple[int, ...] = (5, 60),
                 data_dir=L.DEFAULT_DATA_DIR, kline_raw: Path = KLINE_RAW) -> pa.Table:
    bars, n_fill = fill_gaps(bars_from_trades(symbol, start_day, end_day, data_dir=data_dir),
                             _binance_daily(symbol, kline_raw))
    kf = K.compute(bars)
    k_avail = bars["T_ms"].astype(np.int64) + BAR_MS          # 这根 K 线在 t = open + 60s 可用

    h_ms = horizon_min * BAR_MS
    micro_parts = []
    for w in micro_windows_s:
        ds = F.make_dataset(symbol, start_day, end_day, step_ms=BAR_MS, horizon_ms=h_ms,
                            window_ms=w * 1000, align_ms=BAR_MS, data_dir=data_dir)
        if ds.num_rows == 0:
            raise ValueError(f"{symbol} 微观特征为空")
        micro_parts.append((w, ds))

    # 以第一组微观特征的栅格为基准，其它组和 K 线按 T_ms 精确对齐
    base_w, base = micro_parts[0]
    T = base["T_ms"].to_numpy()
    cols: dict[str, np.ndarray] = {"T_ms": T, "y_bps": base["y_bps"].to_numpy()}
    for c in MICRO_COLS:
        cols[f"{c}_w{base_w}"] = base[c].to_numpy()
    for w, ds in micro_parts[1:]:
        Tw = ds["T_ms"].to_numpy()
        pos = np.searchsorted(Tw, T)
        ok = (pos < len(Tw)) & (Tw[np.minimum(pos, len(Tw) - 1)] == T)
        for c in MICRO_COLS:
            v = np.full(len(T), np.nan)
            v[ok] = ds[c].to_numpy()[pos[ok]]
            cols[f"{c}_w{w}"] = v
    pos = np.searchsorted(k_avail, T)
    ok = (pos < len(k_avail)) & (k_avail[np.minimum(pos, len(k_avail) - 1)] == T)
    for c in K.FEATURE_COLS:
        v = np.full(len(T), np.nan)
        v[ok] = kf[c][pos[ok]]
        cols[c] = v

    good = np.ones(len(T), dtype=bool)
    for v in cols.values():
        if v.dtype.kind == "f":
            good &= np.isfinite(v)
    tbl = pa.table({c: pa.array(v[good]) for c, v in cols.items()})
    return tbl.replace_schema_metadata({"filled_bars": str(n_fill), "dropped": str(int((~good).sum()))})


def feature_groups(micro_windows_s: tuple[int, ...] = (5, 60)) -> dict[str, list[str]]:
    micro = [f"{c}_w{w}" for w in micro_windows_s for c in MICRO_COLS]
    return {"kline": list(K.FEATURE_COLS), "micro": micro, "combined": list(K.FEATURE_COLS) + micro}
