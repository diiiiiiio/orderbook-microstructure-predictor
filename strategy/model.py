"""第一版模型：岭回归（纯 numpy，不引入 sklearn）。按时间切分，标准化只用训练集拟合。

用法：
    from strategy import model as M
    sp = M.time_split(ds, horizon_ms=3000, step_ms=1000)     # 训练/验证/测试下标
    fit = M.fit_ridge(X[sp.train], y[sp.train], alpha=1.0)
    pred = M.predict(fit, X[sp.test])
文档第三步要求：按时间切分不打乱；切分边界删掉标签跨进下一集合的样本；标准化参数只来自训练集。
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pyarrow as pa


@dataclass
class Split:
    train: np.ndarray
    valid: np.ndarray
    test: np.ndarray

    def sizes(self) -> dict[str, int]:
        return {"train": len(self.train), "valid": len(self.valid), "test": len(self.test)}


@dataclass
class RidgeFit:
    w: np.ndarray            # 标准化空间的系数
    b: float
    mu: np.ndarray
    sd: np.ndarray
    cols: list[str]
    degenerate: list[str] | None = None   # 训练段内方差为 0、已被置零的特征

    def coefs(self) -> dict[str, float]:
        """按绝对值排序的标准化系数，用于看哪个特征起作用。"""
        return dict(sorted(zip(self.cols, self.w.tolist()), key=lambda kv: -abs(kv[1])))


def time_split(T_ms: np.ndarray, *, frac=(0.6, 0.2), horizon_ms: int = 3_000) -> Split:
    """按时间顺序切 train/valid/test，并在每个边界后剔除标签跨界的样本（embargo）。"""
    T = np.asarray(T_ms)
    n = len(T)
    i1, i2 = int(n * frac[0]), int(n * (frac[0] + frac[1]))
    idx = np.arange(n)
    # 标签用到 T+horizon，训练集末尾 horizon 内的样本会看到验证集时段，删掉
    tr = idx[:i1][T[:i1] + horizon_ms <= T[i1]] if i1 > 0 else idx[:0]
    va = idx[i1:i2][T[i1:i2] + horizon_ms <= T[i2]] if i2 > i1 else idx[:0]
    return Split(train=tr, valid=va, test=idx[i2:])


def fit_ridge(X: np.ndarray, y: np.ndarray, alpha: float = 1.0,
              cols: list[str] | None = None) -> RidgeFit:
    """标准化 + 带截距的岭回归（截距不惩罚）。"""
    X = np.asarray(X, dtype=np.float64)
    names = list(cols or [f"x{i}" for i in range(X.shape[1])])
    mu, sd = X.mean(0), X.std(0)
    # 训练段内方差为 0 的特征（例如 BTC 上几乎恒为 1 tick 的 spread）不可标准化：
    # 若沿用 sd=1，测试段一个轻微偏离就会被放大成天文数字的 z 值，进而让预测爆掉。
    # 浮点下常数列的 std 往往是 1e-18 而非精确 0，故用相对于均值量级的容差判定
    scale = np.maximum(np.abs(mu), 1.0)
    bad = sd <= 1e-12 * scale
    sd = np.where(bad, 1.0, sd)
    Z = (X - mu) / sd
    Z[:, bad] = 0.0
    A = np.column_stack([Z, np.ones(len(Z))])
    P = np.eye(A.shape[1]) * alpha
    P[-1, -1] = 0.0                                  # 不惩罚截距
    coef = np.linalg.solve(A.T @ A + P, A.T @ np.asarray(y, dtype=np.float64))
    w = coef[:-1]
    w[bad] = 0.0                                     # 该特征在本折不参与预测
    return RidgeFit(w=w, b=float(coef[-1]), mu=mu, sd=sd, cols=names,
                    degenerate=[n for n, f in zip(names, bad) if f])


def predict(fit: RidgeFit, X: np.ndarray, clip_z: float | None = 10.0) -> np.ndarray:
    """clip_z 把标准化后的特征截到 ±clip_z，避免测试段的离群值外推出荒谬的预测。"""
    Z = (np.asarray(X, dtype=np.float64) - fit.mu) / fit.sd
    if clip_z is not None:
        Z = np.clip(Z, -clip_z, clip_z)
    return Z @ fit.w + fit.b


def matrix(ds: pa.Table, cols: list[str]) -> np.ndarray:
    return np.column_stack([ds[c].to_numpy() for c in cols]).astype(np.float64)


def score(pred: np.ndarray, y: np.ndarray, n_bins: int = 10) -> dict:
    """文档第三步的三个问题：误差是否下降、预测越高实际收益是否越高、分组单调性。"""
    pred, y = np.asarray(pred), np.asarray(y)
    rmse = float(np.sqrt(((y - pred) ** 2).mean()))
    base = float(np.sqrt((y ** 2).mean()))            # 永远预测 0 的基线
    out = {"n": len(y), "rmse": rmse, "rmse_baseline_zero": base,
           "rmse_improvement": (base - rmse) / base if base > 0 else 0.0,
           "corr": float(np.corrcoef(pred, y)[0, 1]) if len(y) > 2 else 0.0}
    q = np.quantile(pred, np.linspace(0, 1, n_bins + 1))
    q[-1] += 1e-9
    b = np.clip(np.searchsorted(q, pred, side="right") - 1, 0, n_bins - 1)
    bins = [{"bin": int(k), "n": int((b == k).sum()),
             "pred_mean": float(pred[b == k].mean()) if (b == k).any() else 0.0,
             "y_mean": float(y[b == k].mean()) if (b == k).any() else 0.0}
            for k in range(n_bins)]
    out["deciles"] = bins
    ym = [d["y_mean"] for d in bins]
    out["monotonic_pairs"] = float(np.mean(np.diff(ym) > 0)) if len(ym) > 1 else 0.0
    return out


@dataclass
class Fold:
    k: int
    train: np.ndarray
    test: np.ndarray
    t_train_end: int          # 训练段末尾时间戳
    t_test_beg: int           # 测试段起始时间戳
    t_test_end: int


def walk_forward_folds(T_ms: np.ndarray, *, n_folds: int = 6, horizon_ms: int = 3_000,
                       purge_ms: int | None = None, expanding: bool = True,
                       min_train: int = 500) -> list[Fold]:
    """滚动前推切分：把时间轴等分成 n_folds+1 段，第 k 折用前面所有段训练、第 k+1 段测试。

    训练段末尾与测试段之间留 purge gap（默认等于 horizon_ms），确保训练样本的标签
    不会伸进测试段。expanding=False 时训练段只取紧邻测试段的前一段（滚动窗口）。
    """
    T = np.asarray(T_ms)
    n = len(T)
    purge = horizon_ms if purge_ms is None else purge_ms
    edges = [int(n * i / (n_folds + 1)) for i in range(n_folds + 2)]
    idx = np.arange(n)
    folds: list[Fold] = []
    for k in range(n_folds):
        te_beg, te_end = edges[k + 1], edges[k + 2]
        tr_beg = 0 if expanding else edges[k]
        if te_end <= te_beg or te_beg <= tr_beg:
            continue
        cut = T[te_beg] - purge                       # 标签必须完全落在 purge 之前
        tr = idx[tr_beg:te_beg][T[tr_beg:te_beg] + horizon_ms <= cut]
        if len(tr) < min_train:
            continue
        folds.append(Fold(k=len(folds), train=tr, test=idx[te_beg:te_end],
                          t_train_end=int(T[tr[-1]]), t_test_beg=int(T[te_beg]),
                          t_test_end=int(T[te_end - 1])))
    return folds


def r2_oos(pred: np.ndarray, y: np.ndarray) -> float:
    """Kolm et al. (2023) 口径：基准是该段自身的样本外均值，不是零。"""
    pred, y = np.asarray(pred, dtype=np.float64), np.asarray(y, dtype=np.float64)
    denom = float(((y - y.mean()) ** 2).sum())
    if denom <= 0:
        return 0.0
    return 1.0 - float(((y - pred) ** 2).sum()) / denom


def walk_forward(X: np.ndarray, y: np.ndarray, T_ms: np.ndarray, *, alpha: float = 1.0,
                 cols: list[str] | None = None, **fold_kw) -> dict:
    """在每折上重拟合并评估，返回逐折与汇总指标。标准化与系数都只来自该折训练段。"""
    X, y = np.asarray(X, dtype=np.float64), np.asarray(y, dtype=np.float64)
    folds = walk_forward_folds(T_ms, **fold_kw)
    rows = []
    for f in folds:
        fit = fit_ridge(X[f.train], y[f.train], alpha=alpha, cols=cols)
        p = predict(fit, X[f.test])
        yt = y[f.test]
        rows.append({"fold": f.k, "n_train": len(f.train), "n_test": len(f.test),
                     "t_test_beg": _ms_iso_safe(f.t_test_beg),
                     "t_test_end": _ms_iso_safe(f.t_test_end),
                     "r2_oos": r2_oos(p, yt),
                     "corr": float(np.corrcoef(p, yt)[0, 1]) if len(yt) > 2 else 0.0,
                     "y_std": float(yt.std()),
                     "top_coef": next(iter(fit.coefs()), "")})
    if not rows:
        return {"folds": [], "summary": {"n_folds": 0}}
    r2 = np.array([r["r2_oos"] for r in rows])
    cr = np.array([r["corr"] for r in rows])
    return {"folds": rows,
            "summary": {"n_folds": len(rows),
                        "r2_oos_mean": float(r2.mean()), "r2_oos_median": float(np.median(r2)),
                        "r2_oos_min": float(r2.min()), "r2_oos_max": float(r2.max()),
                        "r2_oos_pos_frac": float((r2 > 0).mean()),
                        "corr_mean": float(cr.mean()), "corr_min": float(cr.min()),
                        "corr_pos_frac": float((cr > 0).mean())}}


def _ms_iso_safe(ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%m-%d %H:%M")
