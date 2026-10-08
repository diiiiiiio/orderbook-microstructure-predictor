"""期限扫描 + 稳定性检验（嵌套选参，测试段只用一次）。

与之前几轮的区别：alpha 和信号阈值都在每折训练段内部的后 20% 验证段上选，
测试段不参与任何选择。之前把六折预测拼起来再取分位数，用到了测试期信息。

用法：
    .venv/bin/python -m strategy.horizon_scan BTCUSDT
"""
from __future__ import annotations

import sys
from dataclasses import dataclass

import numpy as np

from strategy import features as F, model as M

STEP_MS = 100
WINDOWS = (1_000, 5_000, 30_000)
HORIZONS_S = (3, 5, 10, 20, 30, 60)
ALPHAS = (1.0, 10.0, 100.0)
QUANTILES = (0.99, 0.999, 0.9999)
MAKER_BPS = 4.0
DAYS = ("2026-09-25", "2026-09-26")


def build_features(symbol: str, start_day: str = DAYS[0], end_day: str | None = DAYS[1],
                   *, data_dir=None) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, list[str]]:
    """返回 (X, T_ms, mid, segment, cols)。只建一次，标签后面按栅格位移算。"""
    parts = []
    kw = {} if data_dir is None else {"data_dir": data_dir}
    for w in WINDOWS:
        ds = F.make_dataset(symbol, start_day, end_day, step_ms=STEP_MS,
                            horizon_ms=STEP_MS, window_ms=w, **kw)
        parts.append((w, ds))
    base = parts[0][1]
    T = base["T_ms"].to_numpy()
    mid = base["mid"].to_numpy()
    seg = base["segment"].to_numpy()
    cols: list[str] = []
    Xs: list[np.ndarray] = []
    for w, ds in parts:
        Tw = ds["T_ms"].to_numpy()
        pos = np.searchsorted(Tw, T)
        ok = (pos < len(Tw)) & (Tw[np.minimum(pos, len(Tw) - 1)] == T)
        for c in F.FEATURE_COLS:
            v = np.full(len(T), np.nan)
            v[ok] = ds[c].to_numpy()[pos[ok]]
            Xs.append(v)
            cols.append(f"{c}_w{w // 1000}")
    X = np.column_stack(Xs)
    good = np.all(np.isfinite(X), axis=1) & np.isfinite(mid)
    return X[good], T[good], mid[good], seg[good], cols


def labels_at(T: np.ndarray, mid: np.ndarray, seg: np.ndarray, horizon_ms: int) -> np.ndarray:
    """按栅格位移取 t+h 的 mid；跨段或越界返回 nan。栅格是均匀 STEP_MS。"""
    k = horizon_ms // STEP_MS
    n = len(T)
    y = np.full(n, np.nan)
    j = np.arange(n) + k
    ok = j < n
    jj = j[ok]
    i = np.arange(n)[ok]
    same_seg = seg[jj] == seg[i]
    exact = T[jj] - T[i] == horizon_ms          # 段内有缺口时时间差会不等
    m = same_seg & exact
    y[i[m]] = (mid[jj[m]] / mid[i[m]] - 1.0) * 1e4
    return y


def dedupe(ts: np.ndarray, horizon_ms: int) -> np.ndarray:
    """同一时刻只持一笔：上一笔平仓后才允许下一笔。"""
    keep: list[int] = []
    last = -1
    for i, t in enumerate(ts):
        if t >= last:
            keep.append(i)
            last = t + horizon_ms
    return np.array(keep, dtype=int)


def trade_stats(pred: np.ndarray, y: np.ndarray, T: np.ndarray, thr: float,
                horizon_ms: int) -> tuple[int, float, float]:
    """按阈值选样本、去重叠，返回 (笔数, 每笔 bps, 命中率)。"""
    sel = np.abs(pred) >= thr
    if sel.sum() == 0:
        return 0, float("nan"), float("nan")
    o = np.argsort(T[sel])
    ts, ps, ys = T[sel][o], pred[sel][o], y[sel][o]
    k = dedupe(ts, horizon_ms)
    if len(k) == 0:
        return 0, float("nan"), float("nan")
    r = np.sign(ps[k]) * ys[k]
    return len(r), float(r.mean()), float(np.mean(r > 0))


@dataclass
class FoldResult:
    k: int
    alpha: float
    q: float
    thr: float
    n: int
    mean_bps: float
    hit: float
    ic: float
    rets: np.ndarray


def run_horizon(X, T, mid, seg, cols, horizon_ms: int, n_folds: int = 6) -> list[FoldResult]:
    y = labels_at(T, mid, seg, horizon_ms)
    ok = np.isfinite(y)
    Xh, Th, yh = X[ok], T[ok], y[ok]
    out: list[FoldResult] = []
    for f in M.walk_forward_folds(Th, n_folds=n_folds, horizon_ms=horizon_ms):
        tr = f.train
        # 训练段内部再切：前 80% 拟合，后 20% 选参，中间留 purge
        cut = int(len(tr) * 0.8)
        inner_fit, inner_val = tr[:cut], tr[cut:]
        inner_fit = inner_fit[Th[inner_fit] + horizon_ms <= Th[inner_val[0]]]
        if len(inner_fit) < 500 or len(inner_val) < 200:
            continue
        best = None
        for a in ALPHAS:
            fit = M.fit_ridge(Xh[inner_fit], yh[inner_fit], alpha=a, cols=cols)
            pv = M.predict(fit, Xh[inner_val])
            for q in QUANTILES:
                thr = float(np.quantile(np.abs(pv), q))
                n, mean_bps, _ = trade_stats(pv, yh[inner_val], Th[inner_val], thr, horizon_ms)
                if n < 5 or not np.isfinite(mean_bps):
                    continue
                score = mean_bps
                if best is None or score > best[0]:
                    best = (score, a, q)
        if best is None:
            continue
        _, alpha, q = best
        fit = M.fit_ridge(Xh[tr], yh[tr], alpha=alpha, cols=cols)
        ptr = M.predict(fit, Xh[tr])
        thr = float(np.quantile(np.abs(ptr), q))     # 阈值也只用训练段的分布
        pt = M.predict(fit, Xh[f.test])
        yt, Tt = yh[f.test], Th[f.test]
        n, mean_bps, hit = trade_stats(pt, yt, Tt, thr, horizon_ms)
        sel = np.abs(pt) >= thr
        rets = np.array([])
        if sel.sum() > 0:
            o = np.argsort(Tt[sel])
            ts, ps, ys = Tt[sel][o], pt[sel][o], yt[sel][o]
            k = dedupe(ts, horizon_ms)
            rets = np.sign(ps[k]) * ys[k]
        ic = float(np.corrcoef(pt, yt)[0, 1]) if len(pt) > 2 else float("nan")
        out.append(FoldResult(f.k, alpha, q, thr, n, mean_bps, hit, ic, rets))
    return out


def main(symbol: str) -> None:
    print(f"===== {symbol} 期限扫描（嵌套选参，测试段只用一次）=====", flush=True)
    X, T, mid, seg, cols = build_features(symbol)
    span_h = (T[-1] - T[0]) / 3.6e6
    print(f"栅格 {STEP_MS}ms  样本 {len(T):,}  特征 {X.shape[1]}  跨度 {span_h:.1f}h  段数 {len(np.unique(seg))}\n", flush=True)
    print(f"{'期限':>5}{'样本':>10}{'IC均值':>9}{'IC折数':>8}{'笔数':>7}{'每笔bps':>10}{'命中':>8}{'净挂单':>9}{'t值':>7}", flush=True)
    summary = {}
    for hs in HORIZONS_S:
        h = hs * 1000
        res = run_horizon(X, T, mid, seg, cols, h)
        if not res:
            print(f"{hs:>4}s  折数不足", flush=True)
            continue
        allr = np.concatenate([r.rets for r in res]) if any(len(r.rets) for r in res) else np.array([])
        ics = np.array([r.ic for r in res])
        n_tot = int(sum(r.n for r in res))
        if len(allr) > 2 and allr.std(ddof=1) > 0:
            mean_bps = allr.mean()
            tval = mean_bps / (allr.std(ddof=1) / np.sqrt(len(allr)))
        else:
            mean_bps, tval = float("nan"), float("nan")
        ny = labels_at(T, mid, seg, h)
        print(f"{hs:>4}s{int(np.isfinite(ny).sum()):>10,}{ics.mean():>+9.4f}"
              f"{f'{int((ics>0).sum())}/{len(ics)}':>8}{n_tot:>7}{mean_bps:>+10.3f}"
              f"{np.mean(allr>0) if len(allr) else float('nan'):>8.1%}"
              f"{mean_bps-MAKER_BPS:>+9.2f}{tval:>7.2f}", flush=True)
        summary[hs] = (res, allr)
    print("\n===== 逐折明细（看稳定性）=====", flush=True)
    for hs, (res, allr) in summary.items():
        picks = ",".join(f"a{int(r.alpha)}q{r.q}" for r in res)
        print(f"\n-- {hs}s  内层选到: {picks}", flush=True)
        print(f"   {'折':>3}{'IC':>9}{'笔数':>7}{'每笔bps':>10}{'命中':>8}", flush=True)
        for r in res:
            print(f"   {r.k:>3}{r.ic:>+9.4f}{r.n:>7}{r.mean_bps:>+10.3f}"
                  f"{r.hit if np.isfinite(r.hit) else float('nan'):>8.1%}", flush=True)
        if len(allr) > 5:
            rng = np.random.default_rng(0)
            bs = np.array([rng.choice(allr, len(allr), replace=True).mean() for _ in range(2000)])
            lo, hi = np.percentile(bs, [2.5, 97.5])
            print(f"   自助法 95% 区间 [{lo:+.2f}, {hi:+.2f}] bps  成本线 {MAKER_BPS}", flush=True)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT")
