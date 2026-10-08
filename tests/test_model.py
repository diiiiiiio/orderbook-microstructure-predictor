"""strategy.model 的滚动前推验证：purge gap、退化特征保护、R2_OS 口径。"""
from __future__ import annotations

import numpy as np

from strategy import model as M


# ---- 滚动前推验证 ----

def test_walk_forward_folds_no_overlap_and_purged():
    """每折训练段的标签不得伸进测试段，且测试段之间不重叠。"""
    T = np.arange(0, 100_000, 100, dtype=np.int64)       # 1000 点，间隔 100ms
    folds = M.walk_forward_folds(T, n_folds=4, horizon_ms=3_000, min_train=10)
    assert len(folds) == 4
    for f in folds:
        assert f.train.max() < f.test.min()              # 训练严格早于测试
        assert T[f.train].max() + 3_000 <= T[f.test].min() - 3_000 + 3_000
        assert T[f.train].max() + 3_000 <= T[f.test[0]]  # purge gap 生效
    for a, b in zip(folds, folds[1:]):
        assert a.test.max() < b.test.min()               # 测试段不重叠


def test_walk_forward_rolling_vs_expanding():
    T = np.arange(0, 100_000, 100, dtype=np.int64)
    exp = M.walk_forward_folds(T, n_folds=4, horizon_ms=3_000, min_train=10, expanding=True)
    rol = M.walk_forward_folds(T, n_folds=4, horizon_ms=3_000, min_train=10, expanding=False)
    assert exp[-1].train.min() == 0                      # 扩张窗口从头开始
    assert rol[-1].train.min() > 0                       # 滚动窗口丢掉早期数据
    assert len(exp[-1].train) > len(rol[-1].train)


def test_r2_oos_baseline_is_sample_mean():
    """预测恒等于样本均值时 R2_OS 应为 0；完美预测为 1。"""
    y = np.array([1.0, -2.0, 3.0, 0.5])
    assert abs(M.r2_oos(np.full(4, y.mean()), y)) < 1e-12
    assert abs(M.r2_oos(y, y) - 1.0) < 1e-12
    assert M.r2_oos(np.zeros(4), y) < 0                   # 常数 0 差于均值基线


def test_zero_variance_feature_does_not_explode():
    """训练段方差为 0 的特征必须被置零，否则测试段的轻微偏离会放大成荒谬预测。"""
    rng = np.random.default_rng(0)
    n = 400
    good = rng.normal(size=n)
    const = np.full(n, 0.0119)                            # 模拟恒为 1 tick 的 spread
    X = np.column_stack([good, const])
    y = good * 2.0 + rng.normal(scale=0.1, size=n)
    fit = M.fit_ridge(X, y, alpha=1.0, cols=["good", "const"])
    assert fit.degenerate == ["const"]
    assert fit.w[1] == 0.0
    Xt = X.copy()
    Xt[0, 1] = 0.5                                        # 测试段出现一个宽价差
    pred = M.predict(fit, Xt)
    assert abs(pred[0]) < 100.0                           # 未爆炸
    assert M.r2_oos(pred, y) > 0.9


def test_predict_clips_outliers():
    rng = np.random.default_rng(1)
    X = rng.normal(size=(300, 2))
    y = X[:, 0] + rng.normal(scale=0.1, size=300)
    fit = M.fit_ridge(X, y, alpha=1.0, cols=["a", "b"])
    Xt = np.array([[1e6, 0.0]])
    assert abs(M.predict(fit, Xt, clip_z=10.0)[0]) < abs(M.predict(fit, Xt, clip_z=None)[0])


def test_walk_forward_summary_shape():
    rng = np.random.default_rng(2)
    n = 3000
    T = np.arange(n, dtype=np.int64) * 1000
    X = rng.normal(size=(n, 3))
    y = X[:, 0] * 0.5 + rng.normal(scale=1.0, size=n)
    r = M.walk_forward(X, y, T, alpha=1.0, cols=["a", "b", "c"], n_folds=5,
                       horizon_ms=3_000, min_train=100)
    assert r["summary"]["n_folds"] == 5
    assert len(r["folds"]) == 5
    assert r["summary"]["r2_oos_mean"] > 0                # 信号真实存在，应为正
    assert 0.0 <= r["summary"]["r2_oos_pos_frac"] <= 1.0
    for f in r["folds"]:
        assert f["n_train"] > 0 and f["n_test"] > 0
