"""分钟级 K 线特征：从币安月度 1m K 线算特征和未来收益标签。

用法：
    from strategy import klines as K
    ds = K.make_dataset("BTCUSDT", horizon_min=5)     # pyarrow.Table
    X = np.column_stack([ds[c].to_numpy() for c in K.FEATURE_COLS]); y = ds["y_bps"].to_numpy()

和 features.py 的区别：那边是 100ms 盘口快照上的微观结构特征（OBI/OFI/价差），
3 秒到 1 分钟尺度；这边是 1 分钟 K 线上的量价特征，5 分钟到数小时尺度。
K 线没有盘口，所以拿不到 OBI、价差、队列，只能用收益、成交量、taker 买卖比。

特征都是"过去 N 根 K 线"的函数，标签是未来 horizon_min 根之后 close 的收益（bps）。
K 线用 close 而不是 mid，close 是最后一笔成交价，本身带买卖价差的一半噪声。

无未来泄漏的关键：第 i 个样本的特征只用第 i 根及之前的 K 线（含第 i 根的 close），
标签用第 i+horizon 根的 close。所有 rolling 都是 shift 过的，不含当根未来信息。
月度文件按 open_time 排序拼接，跨月缺口按时间戳连续性切段。
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import numpy as np
import pyarrow as pa

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "store" / "klines" / "raw"
BAR_MS = 60_000


# ---------- 1. 加载 ----------

def load_klines(symbol: str, *, raw_dir: Path | None = None) -> dict[str, np.ndarray]:
    """读取该标的所有月度 zip，按 open_time 升序拼接，返回列名到数组的字典。"""
    raw_dir = raw_dir or RAW_DIR
    files = sorted(raw_dir.glob(f"{symbol}-1m-*.zip"))
    if not files:
        raise FileNotFoundError(f"没有 {symbol} 的 K 线文件，找找 {raw_dir}")

    rows: list[list[float]] = []
    for zp in files:
        with zipfile.ZipFile(zp) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as fh:
                for raw in fh:
                    line = raw.decode().strip()
                    if not line or line[0] not in "0123456789":
                        continue                      # 跳过 header
                    p = line.split(",")
                    rows.append([float(p[0]), float(p[1]), float(p[2]), float(p[3]),
                                 float(p[4]), float(p[5]), float(p[7]), float(p[8]),
                                 float(p[9])])
    if not rows:
        raise ValueError(f"{symbol} 解析出 0 行")

    a = np.asarray(rows, dtype=np.float64)
    a = a[np.argsort(a[:, 0], kind="stable")]
    _, keep = np.unique(a[:, 0], return_index=True)    # 跨月重复的 K 线去重
    a = a[np.sort(keep)]
    return {"T_ms": a[:, 0], "open": a[:, 1], "high": a[:, 2], "low": a[:, 3],
            "close": a[:, 4], "volume": a[:, 5], "quote_volume": a[:, 6],
            "n_trades": a[:, 7], "taker_buy_volume": a[:, 8]}


# ---------- 2. 工具 ----------

def _roll_sum(x: np.ndarray, n: int) -> np.ndarray:
    """过去 n 个元素的和（含当前），不足 n 个的位置为 nan。"""
    c = np.concatenate([[0.0], np.nancumsum(x)])
    out = np.full(len(x), np.nan)
    out[n - 1:] = c[n:] - c[:len(x) - n + 1]
    return out


def _roll_mean(x: np.ndarray, n: int) -> np.ndarray:
    return _roll_sum(x, n) / n


def _roll_std(x: np.ndarray, n: int) -> np.ndarray:
    m = _roll_mean(x, n)
    m2 = _roll_mean(x * x, n)
    return np.sqrt(np.maximum(m2 - m * m, 0.0))


def _segments(T_ms: np.ndarray, *, max_gap_ms: int = BAR_MS * 3) -> list[tuple[int, int]]:
    """按时间戳连续性切段，返回 [start, stop) 索引对。"""
    if len(T_ms) == 0:
        return []
    brk = np.nonzero(np.diff(T_ms) > max_gap_ms)[0] + 1
    edges = np.concatenate([[0], brk, [len(T_ms)]])
    return [(int(edges[i]), int(edges[i + 1])) for i in range(len(edges) - 1)]


# ---------- 3. 特征 ----------

FEATURE_COLS = [
    "ret_1", "ret_5", "ret_15", "ret_60",          # 动量：过去 N 分钟收益
    "rv_15", "rv_60",                              # 已实现波动率
    "taker_imb_5", "taker_imb_30",                 # 主动买卖不平衡
    "vol_z_15", "trade_z_15",                      # 成交量 / 笔数的标准化异常
    "range_15",                                    # 高低幅度
    "vwap_dev",                                    # close 相对 VWAP 的偏离
    "rsi_14",                                      # 超买超卖
    "ac_15",                                       # 收益自相关：均值回复还是趋势
]


def compute(k: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """在整条 K 线序列上算特征。每个值只用当根及之前的信息。"""
    close, vol, qv = k["close"], k["volume"], k["quote_volume"]
    n_tr, tbv, high, low = k["n_trades"], k["taker_buy_volume"], k["high"], k["low"]

    lr = np.zeros(len(close))
    lr[1:] = np.log(close[1:] / close[:-1]) * 1e4   # 逐根对数收益，bps

    f: dict[str, np.ndarray] = {}
    for n in (1, 5, 15, 60):
        f[f"ret_{n}"] = _roll_sum(lr, n)
    for n in (15, 60):
        f[f"rv_{n}"] = np.sqrt(_roll_sum(lr * lr, n))

    # taker 主动买占比减 0.5，正值表示买方主动更多
    for n in (5, 30):
        bv, tv = _roll_sum(tbv, n), _roll_sum(vol, n)
        f[f"taker_imb_{n}"] = np.where(tv > 0, bv / np.where(tv > 0, tv, 1.0) - 0.5, 0.0)

    # 成交量和笔数相对过去 60 根的 z 分数
    for src, name in ((vol, "vol"), (n_tr, "trade")):
        m, s = _roll_mean(src, 60), _roll_std(src, 60)
        f[f"{name}_z_15"] = np.where(s > 0, (_roll_mean(src, 15) - m) / np.where(s > 0, s, 1.0), 0.0)

    f["range_15"] = (_roll_sum(high - low, 15) / 15) / close * 1e4

    vwap = _roll_sum(qv, 15) / np.maximum(_roll_sum(vol, 15), 1e-12)
    f["vwap_dev"] = (close / vwap - 1.0) * 1e4

    up = _roll_mean(np.maximum(lr, 0.0), 14)
    dn = _roll_mean(np.maximum(-lr, 0.0), 14)
    f["rsi_14"] = 100.0 - 100.0 / (1.0 + up / np.where(dn > 0, dn, 1e-12)) - 50.0

    prev = np.concatenate([[0.0], lr[:-1]])
    cov, var = _roll_mean(lr * prev, 15), _roll_mean(lr * lr, 15)
    f["ac_15"] = np.where(var > 0, cov / np.where(var > 0, var, 1.0), 0.0)
    return f


# ---------- 4. 组装数据集 ----------

WARMUP = 60             # 最长回看窗口，段首这么多根丢掉


def make_dataset(symbol: str, *, horizon_min: int = 5, step_min: int = 1,
                 raw_dir: Path | None = None) -> pa.Table:
    """算特征和未来 horizon_min 分钟的 close 收益标签。

    step_min > 1 时按步长抽样，用于得到标签窗口不重叠的独立样本
    （step_min >= horizon_min 即完全不重叠）。
    """
    k = load_klines(symbol, raw_dir=raw_dir)
    f = compute(k)
    T_ms, close = k["T_ms"], k["close"]

    idx_parts = []
    for start, stop in _segments(T_ms):
        lo, hi = start + WARMUP, stop - horizon_min
        if hi <= lo:
            continue
        idx_parts.append(np.arange(lo, hi, step_min))
    if not idx_parts:
        raise ValueError(f"{symbol} 没有足够长的连续段")
    idx = np.concatenate(idx_parts)

    y = (close[idx + horizon_min] / close[idx] - 1.0) * 1e4
    cols = {"T_ms": T_ms[idx].astype(np.int64), "close": close[idx],
            **{c: f[c][idx] for c in FEATURE_COLS}, "y_bps": y}

    good = np.ones(len(idx), dtype=bool)               # 丢掉任何含 nan/inf 的样本
    for v in cols.values():
        if v.dtype.kind == "f":
            good &= np.isfinite(v)
    return pa.table({c: pa.array(v[good]) for c, v in cols.items()})
