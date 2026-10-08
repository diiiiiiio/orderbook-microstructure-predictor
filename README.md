# orderbook-microstructure-predictor

币安 USDⓈ-M 永续（BTCUSDT / ETHUSDT）**盘口微观结构短期收益预测**研究。

**价格纯预测** 

**预测能力真实存在，但当前费率下不可交易。**

| 指标（20 秒期限，约 20 小时 / 69.7 万样本） | BTCUSDT | ETHUSDT |
|---|---|---|
| 预测相关系数 IC 均值 | +0.205 | +0.212 |
| IC 为正的折数（六折前推） | 6/6 | 6/6 |
| 毛收益/笔（未扣费） | +0.54 bps | +0.78 bps |
| taker 净收益/笔（扣 10 bps 往返费） | −9.46 bps | −9.22 bps |
| 净胜率 | 0.0 | 0.0 |

**根因**：每笔毛收益 ≈ 0.5–0.8 bps，而吃单往返手续费 10 bps（0.05%×2），成本是 edge 的 12–20 倍。


## 流水线

```
行情采集 ──► 特征 ──► 模型（岭回归） ──► 逐单回测
100ms 盘口     OBI / OFI / 价差      时间切分 +        逐档吃单
+ aggTrade     深度 / 成交流 / 波动    embargo           成本 / 滑点
```

- **采集器**（`data/collector/`）：100ms 盘口增量 + aggTrade + markPrice，重建订单簿、校验更新号、断档重同步、崩溃恢复，输出 raw JSONL + normalized Parquet + 质量日报。只读公开行情，不下单、不用密钥。
- **特征**（`strategy/features.py`）：六类微观结构特征（OBI、OFI、价差、深度、主动成交不平衡、已实现波动率），固定时间栅格对齐，无未来泄漏。
- **模型**（`strategy/model.py`）：岭回归对照模型，按时间切分、切分边界删标签跨界样本、标准化只用训练集拟合。
- **回测**（`backtest/`）：Taker 逐档吃单，卖盘买 / 买盘卖，不拿中间价假装成交；同一时刻单持仓；分报吃单 / 挂单两套费率。

## 目录

```
data/        collector/ 采集器；loader.py 读取入口；store/ 数据（不入库）
strategy/    features.py 特征；model.py 岭回归；klines.py 分钟 K 线；
             horizon_scan.py 期限扫描；horizon_stability.py 稳定性检验
backtest/    engine.py 逐档吃单回测；run.py 一键出报告
tests/       pytest 测试（83 个，离线可跑）
deploy/      systemd 单元、安装脚本、collector.yaml 模板、运维手册
docs/        详细设计、示例报告、采集器文档
```

## 快速开始

```bash
# 环境（Python ≥ 3.11）
uv venv --python 3.12 .venv
.venv/bin/pip install -r requirements.txt        # Windows 用 .venv\Scripts\pip

# 测试（离线，约 10s）
.venv/bin/python -m pytest tests -q

# 采集（先复制配置并按需填 proxy；本机直连币安超时，需本地代理）
cp deploy/config/collector.example.yaml deploy/config/collector.yaml
.venv/bin/python -m data.collector -c deploy/config/collector.yaml run

# 期限扫描（评估预测能力：IC、逐折明细）
.venv/bin/python -m strategy.horizon_scan BTCUSDT

# 稳定性检验（换折数、滚动/扩张窗口、按小时拆分、块自助法）
.venv/bin/python -m strategy.horizon_stability BTCUSDT 20

# 一键回测（特征 → 岭回归 → 逐单回测 → JSON 报告）
.venv/bin/python -m backtest.run BTCUSDT 2026-09-25 2026-09-26
```

> 采集器文档（数据语义、盘口同步规则、崩溃恢复、已知边界、运维）见 `docs/collector.md`。

## 参考论文

| 论文 | 来源 |
|---|---|
| Explainable Patterns in Cryptocurrency Microstructure | [arXiv 2602.00776](https://arxiv.org/abs/2602.00776) |
| Deep order flow imbalance: Extracting alpha at multiple horizons from the limit order book（Kercheval & Turiel） | [Wiley · DOI 10.1111/mafi.12413](https://doi.org/10.1111/mafi.12413) |
| The Market Maker's Dilemma: Navigating the Fill Probability vs. Post-Fill Returns Trade-Off（Albers & Cucuringu） | [arXiv 2502.18625](https://arxiv.org/abs/2502.18625) |

## 已知边界

- 交易所侧漏推、聚合口径、RPI（Retail Price Improvement）不可见等上游因素，本项目无法检测或弥补。
- 回测只做了 Taker 吃单；挂单排队与不成交未模拟，挂单净收益是乐观上界，不可当成可实现收益。
- 结论基于 2026-09-25 ~ 28 约 4 天生产数据，是「起步实验」，不是长期 edge 成立的证明。
