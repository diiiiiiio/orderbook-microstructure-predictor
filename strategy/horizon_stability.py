"""对选出的期限做稳定性检验：换折数、滚动 vs 扩张窗口、按小时拆分、块自助法。

    .venv/bin/python -m strategy.horizon_stability BTCUSDT 10
"""
from __future__ import annotations

import sys

import numpy as np

from strategy import model as M
from strategy.horizon_scan import (MAKER_BPS, build_features, dedupe, labels_at,
                                   run_horizon, trade_stats)


def block_bootstrap(r: np.ndarray, block: int = 5, n: int = 2000, seed: int = 0):
    """把交易序列按 block 笔切块重抽，保留局部相关，比 iid 自助法更保守。"""
    if len(r) <= block:
        return float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    nb = int(np.ceil(len(r) / block))
    starts = np.arange(0, len(r) - block + 1)
    means = []
    for _ in range(n):
        idx = np.concatenate([np.arange(s, s + block) for s in rng.choice(starts, nb)])[:len(r)]
        means.append(r[idx].mean())
    return tuple(np.percentile(means, [2.5, 97.5]))


def main(symbol: str, hs: int) -> None:
    h = hs * 1000
    X, T, mid, seg, cols = build_features(symbol)
    print(f"===== {symbol} {hs}s 稳定性 =====  样本 {len(T):,}  跨度 {(T[-1]-T[0])/3.6e6:.1f}h\n")

    print("【1】换折数 / 换窗口类型：每笔 bps 是否稳定")
    print(f"   {'设置':<16}{'折':>4}{'笔数':>7}{'每笔bps':>10}{'命中':>8}{'IC正折':>8}{'t值':>7}")
    for n_folds in (4, 6, 8, 10):
        res = run_horizon(X, T, mid, seg, cols, h, n_folds=n_folds)
        if not res:
            continue
        r = np.concatenate([x.rets for x in res if len(x.rets)])
        ics = np.array([x.ic for x in res])
        t = r.mean() / (r.std(ddof=1) / np.sqrt(len(r))) if len(r) > 2 else float("nan")
        print(f"   {'扩张 '+str(n_folds)+'折':<16}{len(res):>4}{len(r):>7}{r.mean():>+10.3f}"
              f"{np.mean(r>0):>8.1%}{f'{int((ics>0).sum())}/{len(ics)}':>8}{t:>7.2f}")

    # 滚动窗口需要绕过 run_horizon 里的 expanding 默认值：复用其内部逻辑
    import strategy.horizon_scan as HS
    orig = M.walk_forward_folds
    def rolling(*a, **k):
        k["expanding"] = False
        k.setdefault("min_train", 300)
        return orig(*a, **k)
    HS.M.walk_forward_folds = rolling
    try:
        for n_folds in (6, 8):
            res = run_horizon(X, T, mid, seg, cols, h, n_folds=n_folds)
            if not res:
                print(f"   {'滚动 '+str(n_folds)+'折':<16}  折数不足")
                continue
            r = np.concatenate([x.rets for x in res if len(x.rets)])
            ics = np.array([x.ic for x in res])
            t = r.mean() / (r.std(ddof=1) / np.sqrt(len(r))) if len(r) > 2 else float("nan")
            print(f"   {'滚动 '+str(n_folds)+'折':<16}{len(res):>4}{len(r):>7}{r.mean():>+10.3f}"
                  f"{np.mean(r>0):>8.1%}{f'{int((ics>0).sum())}/{len(ics)}':>8}{t:>7.2f}")
    finally:
        HS.M.walk_forward_folds = orig

    print("\n【2】按 UTC 小时拆分（六折扩张，用各折测试段预测）")
    y = labels_at(T, mid, seg, h)
    ok = np.isfinite(y)
    Xh, Th, yh = X[ok], T[ok], y[ok]
    folds = M.walk_forward_folds(Th, n_folds=6, horizon_ms=h)
    P, Y, TT = [], [], []
    for f in folds:
        fit = M.fit_ridge(Xh[f.train], yh[f.train], alpha=1.0, cols=cols)
        ptr = M.predict(fit, Xh[f.train])
        thr = float(np.quantile(np.abs(ptr), 0.99))
        pt = M.predict(fit, Xh[f.test])
        sel = np.abs(pt) >= thr
        P.append(pt[sel]); Y.append(yh[f.test][sel]); TT.append(Th[f.test][sel])
    p = np.concatenate(P); yy = np.concatenate(Y); tt = np.concatenate(TT)
    o = np.argsort(tt); p, yy, tt = p[o], yy[o], tt[o]
    k = dedupe(tt, h)
    p, yy, tt = p[k], yy[k], tt[k]
    r = np.sign(p) * yy
    hour = ((tt // 3_600_000) % 24).astype(int)
    print(f"   {'UTC小时':>8}{'笔数':>7}{'每笔bps':>10}{'命中':>8}")
    for hr in np.unique(hour):
        m = hour == hr
        if m.sum() < 3:
            continue
        print(f"   {hr:>8}{m.sum():>7}{r[m].mean():>+10.3f}{np.mean(r[m]>0):>8.1%}")
    pos_hours = sum(1 for hr in np.unique(hour) if (hour == hr).sum() >= 3 and r[hour == hr].mean() > 0)
    tot_hours = sum(1 for hr in np.unique(hour) if (hour == hr).sum() >= 3)
    print(f"   为正的小时 {pos_hours}/{tot_hours}")

    print(f"\n【3】自助法区间（全部 {len(r)} 笔，固定 a=1 q=0.99）")
    rng = np.random.default_rng(0)
    iid = np.percentile([rng.choice(r, len(r)).mean() for _ in range(2000)], [2.5, 97.5])
    blk = block_bootstrap(r, block=5)
    print(f"   均值 {r.mean():+.3f}  iid 95% [{iid[0]:+.2f}, {iid[1]:+.2f}]  块(5笔) 95% [{blk[0]:+.2f}, {blk[1]:+.2f}]  成本线 {MAKER_BPS}")

    print("\n【4】前半 / 后半各自独立")
    half = len(r) // 2
    for name, rr in (("前半", r[:half]), ("后半", r[half:])):
        t = rr.mean() / (rr.std(ddof=1) / np.sqrt(len(rr))) if len(rr) > 2 else float("nan")
        print(f"   {name}  笔数 {len(rr):>4}  每笔 {rr.mean():+.3f}  命中 {np.mean(rr>0):.1%}  t={t:.2f}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT", int(sys.argv[2]) if len(sys.argv) > 2 else 10)
