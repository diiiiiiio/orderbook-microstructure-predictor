# Binance USDⓈ-M Futures 公开行情采集器（BTCUSDT / ETHUSDT）

用于高频预测研究的**真实盘口与成交数据长期积累**。只做采集、保存、校验、恢复、回放；
不做特征、模型、回测、前端、交易。

核心原则：**宁可明确记录缺失，也不把错误、过期或补造的数据当成正常数据。**
本项目不承诺"绝对不丢数据"，而是用代码、质量报告和测试说明系统的边界（见「已知边界」）。

本 README 只讲采集器（`data/collector/`）。项目整体目录：

```
data/        collector/ 采集器；loader.py 研究层读取入口；store/ 数据（gitignore）
strategy/    features.py 特征与标签；之后放模型、信号
backtest/    回测（待建）
deploy/      systemd 单元、安装脚本、config/collector.yaml 配置、OPERATIONS.md 运维手册
scripts/     采集器 smoke/soak/故障注入脚本；dsh*.sh 为无关的 DeepSeek Harness 辅助
tests/       全部自动化测试（pytest）
docs/        示例报告、codeflow 设计图、参考文档
paper/       参考论文
```

---

## 1. 官方文档核对

核对日期：**2026-09-25 / 2026-09-26（UTC）**。developers.binance.com 对脚本请求返回 AWS WAF 挑战页（HTTP 202 空内容），
以下内容来自 Wayback Machine 保存的官方页面正文，并用生产 API 实测验证（见 §1.3）。

### 1.1 使用的接口

| 用途 | 地址 | 说明 |
|---|---|---|
| 深度增量（高频公共流） | `wss://fstream.binance.com/public/stream?streams=btcusdt@depth@100ms/ethusdt@depth@100ms` | `<symbol>@depth@100ms`，官方"Diff. Book Depth Streams"，URL PATH `/public` |
| 聚合成交 + 标记价格 | `wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s/ethusdt@aggTrade/ethusdt@markPrice@1s` | `@aggTrade`、`@markPrice@1s` 归 `/market` |
| 盘口快照 | `GET https://fapi.binance.com/fapi/v1/depth?symbol=&limit=1000` | 权重：limit 1000 → 20 |
| 合约规格 | `GET /fapi/v1/exchangeInfo` | 权重 1；读 `filters` 的 `tickSize` / `stepSize` |
| 服务器时间 | `GET /fapi/v1/time` | 权重 1 |
| 成交补数 | `GET /fapi/v1/aggTrades?symbol=&fromId=&limit=1000` | 权重 20 |

来源页面（官方）：
- Websocket Market Streams / Connect：`developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams`
- How to manage a local order book correctly：`.../websocket-market-streams/How-to-manage-a-local-order-book-correctly`
- Diff. Book Depth Streams、Aggregate Trade Streams、Mark Price Stream：同目录下对应页面
- Important WebSocket Change Notice — Base URL Split & Migration：`.../websocket-market-streams/Important-WebSocket-Change-Notice`
- REST：Order Book、Compressed Aggregate Trades List、Exchange Information、Check Server Time：`.../market-data/rest-api/...`

### 1.2 与任务书描述的差异（按最新官方规范实现）

1. **URL 路由拆分**：官方已把 WebSocket 拆成 `/public`（高频公共：depth、bookTicker）、`/market`（aggTrade、markPrice、kline 等）、`/private`。
   旧 `wss://fstream.binance.com/stream` 于 **2026-04-23** 停用；实测旧地址与 `/public` 上订阅 `@aggTrade` 均收不到数据（超时）。
   任务书给出的两条组合流地址与此一致，本项目照此实现。
2. **aggTrades 历史窗口**：任务书写"最近 48 小时，编码时再核对"。实测请求 3 天前数据返回
   `{"code":-4166,"msg":"Search window is restricted to recent 2 days only."}`；Wayback 页面文字为"not older than one year"（已过期）。
   **实现按 2 天窗口**（配置 `backfill.window_hours: 48`，留 30 分钟安全边际）。
3. **新增字段**：aggTrade 实际返回 `nq`（不含 RPI 成交的数量）与 `st`（1=UM, 2=CM）；markPrice 返回 `ap`、`P`、`st`；
   depthUpdate 返回 `ps`。全部保留：已知字段规范化成列，未知字段进 `extra_json`，原文完整保存在 raw 层。
4. **markPrice@1s**：任务书追加要求，已实现（`/market` 路由）。
5. 连接规则（官方）：单连接 24 小时有效；服务器每 3 分钟 ping，10 分钟内无 pong 断开；每连接最多 200 个流；
   客户端每秒最多 10 条入站消息。本项目 23 小时主动轮换（`ws.rotate_after_s`），依赖 websockets 库自动回 pong，另每 5 分钟发一次主动 pong。

### 1.3 生产 API 实测（2026-09-25 19:01 UTC，经本机代理）

- `/fapi/v1/time` → `{"serverTime":1790362884262}`；`/fapi/v1/depth?limit=5` 响应头含 `x-mbx-used-weight-1m`。
- `exchangeInfo`：`futuresType=U_MARGINED`；BTCUSDT `PERPETUAL/TRADING/quoteAsset=USDT/marginAsset=USDT`，`tickSize=0.10`，`stepSize=0.001`，
  而 `pricePrecision=2`、`quantityPrecision=3` —— **精度位数 ≠ 步长**（0.10 的 tick 用 2 位小数表示），所以只用 filters。
- `/public` 上 `btcusdt@depth@100ms`、`/market` 上 `aggTrade`/`markPrice@1s` 均实时收到数据；`/stream` 与 `/public` 上的 `aggTrade` 超时无数据。

---

## 2. 数据说明（必须读）

- **`@depth@100ms` 是交易所每 100ms 推送的聚合盘口变化**（某价位的新绝对数量），不是逐笔新增/撤单/撮合记录。
  仅凭价位数量变化**不能精确拆分**新增、撤单和成交。
- **`aggTrade` 是聚合成交**：100ms 内同价、同主动方向的成交合并为一条，不是未聚合的逐笔撮合。保险基金与 ADL 成交不包含。
- **常规盘口不包含所有不可见流动性**。官方明确：RPI（Retail Price Improvement）订单在 depth 快照/增量中不可见；
  在 aggTrade 中被聚合进 `q` 且无标记，`nq` 为不含 RPI 参与成交的数量。两者口径不同，本项目原样分别保留，不混用。
- **主动方向**：`m=true` 买方是 maker → 主动方是卖方 → `aggressor_side="sell"`；`m=false` → `"buy"`。有单元测试。
- **标记价格** `p` 不是可成交价格；`r` 是实时推送的预估资金费率，**不是已实际扣收的资金费**；`T` 是下一次资金费时间。
- **1000 档快照 ≠ 全市场所有挂单**。本地盘口维护"已知深度"，导出前 20 档；当快照覆盖边界无法保证前 20 档完整时标记 INVALID 并重新同步。
- **序号连续 + 文件校验通过 ≠ 上游市场信息绝对完整**。SHA-256 只证明文件未被改动。
- **断档后恢复的是当前盘口（新 `book_epoch`），不等于补齐历史缺失**。禁止插值、复制上一帧、成交反推。
- **时间**：交易所字段 `E`/`T` 为毫秒（列名带 `_ms`）；本机 `recv_time_ns` 为 `time.time_ns()`（UTC，精度取决于系统），
  `recv_monotonic_ns` 为单调时钟。`recv_time - E` 含时钟偏差与代理转发，**不是精确单向网络延迟**。
- **REST 补回的成交**没有 `E`、没有实时接收时间：对应列为 null，`known_time_ns` 为 REST 响应接收时间，`source=rest_backfill`。

---

## 3. 目录与存储布局

```
data/collector/            采集器代码
  config.py  clock.py  numeric.py  records.py
  orderbook.py             盘口状态机（USDⓈ-M 衔接规则，纯逻辑）
  depth.py  trades.py  markprice.py  backfill.py  quality.py
  rest.py  ws.py           REST（权重预算/退避/限流）、WebSocket（重连/轮换/静默检测）
  storage/raw.py           JSONL 分片、manifest、崩溃恢复
  storage/normalized.py    Parquet 研究层
  storage/checkpoint.py
  writer.py  collector.py  monitor.py  replay.py  verify.py  __main__.py
  dashboard.py  static/index.html   只读监控面板（aiohttp + 单页 HTML）
data/loader.py             研究层读取入口：按 symbol/日期读 normalized、去重、排序、切连续段
deploy/config/collector.example.yaml   配置模板（collector.yaml 为本机配置，已 gitignore）
deploy/                    systemd 单元(user/system)、定时器、install/uninstall、collectorctl、healthcheck、OPERATIONS.md 运维手册
scripts/collector_smoke.sh | collector_soak.sh | collector_verify_day.sh | collector_fault_inject.sh
tests/                     83 个自动化测试（含 test_loader.py）

<data_dir>/                默认 data/store（建议改到独立磁盘）
  raw/<kind>/<SYMBOL>/<YYYYMMDD>/<session>_<seq>.jsonl[.gz] (+ .manifest.json, .tail.corrupt)
      kind ∈ depth | aggTrade | markPrice | rest | events | unparsed | other
  normalized/<table>/<SYMBOL>/<YYYYMMDD>/<session>_<seq>.parquet
      table ∈ book_top20 | agg_trades | mark_price | gaps | quality_events
  state/status.json  checkpoint.json  config_<session>.json  specs/exchangeInfo_<hash>.json
  reports/<YYYYMMDD>.json  <YYYYMMDD>.md
```

### 3.1 Raw 层（不可变，含重复与异常）

每行一个 JSON 对象。WebSocket 记录字段：
`schema_version, exchange, market, symbol, stream, source(ws), session_id, connection_id, recv_seq, recv_time_ns, recv_monotonic_ns, id(u/a/E), raw(原文，含组合流包装), parse_error?`。
REST 记录：`request_id, endpoint, params, status, headers(x-mbx-*, date, retry-after), sent_time_ns, recv_time_ns, raw(响应原文)`。
`events` 分片记录连接开关、轮换、同步状态、gap、质量事件、会话启停与运行配置。

活动分片不压缩；关闭（64 MiB 或 15 分钟或 UTC 日切）后校验行数、gzip、写 manifest
（路径、记录数、首末接收时间、首末 ID、schema 版本、大小、SHA-256）。

### 3.2 Normalized 层

- `book_top20`：每条**有效**深度更新处理完成后的前 20 档。列：`symbol, book_epoch, u, U, pu, E_ms, T_ms, recv_time_ns, recv_monotonic_ns, available_time_ns, recv_seq, connection_id, session_id, n_bid_levels_known, n_ask_levels_known, is_valid, state, flags, bid_px_0..19, bid_qty_0..19, ask_px_0..19, ask_qty_0..19`。
  价格/数量为**字符串**（按 tickSize/stepSize 位数格式化，无损）。不足 20 档的位置为 null。**不做重采样**，不补造未收到的 100ms 行。
- `agg_trades`：`symbol, a, p, q, nq, f, l, T_ms, E_ms, m, aggressor_side, st, source(ws|rest_backfill), recv_time_ns, known_time_ns, recv_seq, connection_id, session_id, extra_json`。以 (symbol, a) 唯一；补数与实时重叠时保留最早真实获知的那条，不覆写。
- `mark_price`：`symbol, E_ms, p, ap, i, P, r, T_next_funding_ms, st, recv_time_ns, ...`。
- `gaps`：`gap_id, symbol, stream, kind, certainty(certain|suspected), reason, detected_time_ns, start_time_ns, end_time_ns, start_exchange_time_ms, end_exchange_time_ms, time_basis, prev_known_id, next_known_id, repair_status(open|repaired|partial|unrepairable|not_applicable), session_id, connection_id, book_epoch_before, book_epoch_after, note, update_time_ns`。
  同一 `gap_id` 状态变化时追加新行，研究时取 `update_time_ns` 最大者。
  kind：`depth_sequence`（pu 断档，确定）、`depth_consistency`（交叉/解析/覆盖失败，确定）、`depth_stale`（疑似停滞）、`depth_buffer_overflow`、`aggtrade_id`（a 跳号，确定）、`markprice_silence`（疑似）、`downtime`（进程未运行）、`queue_overflow`。
- `quality_events`：`time_ns, symbol, stream, event_type, severity, session_id, connection_id, detail(JSON)`。

### 3.3 盘口同步规则（USDⓈ-M，不是 Spot）

1. 先连 WS 缓存增量；**收到首条增量后**才请求 `depth?limit=1000`（避免快照早于缓存）。
2. 丢弃 `u < lastUpdateId`；第一条应用的事件须 `U <= lastUpdateId <= u`；若缓存首条 `U > lastUpdateId`，重新取快照。
3. 之后每条 `pu` 必须等于上一条已应用的 `u`，否则 INVALID → 新 `book_epoch` 重新同步。
4. 数量为绝对值；0 删除；删除不存在价位正常。一条消息买卖两侧整体解析、整体应用，解析失败整条不应用。
5. 状态：`SYNCING / LIVE / STALE(>5s 无事件) / INVALID`。只有 LIVE 的更新才写入 `book_top20`。
6. 重复（同 u）不重复应用；同 u 不同内容 → `same_u_different_content` 报警；过旧丢弃并计数。
7. 快照有 1000 档边界时跟踪覆盖范围；前 20 档任一档越过边界即 INVALID。

---

## 4. 安装与运行

```bash
# Python 3.12（本机由 uv 安装：~/.local/bin/uv venv --python 3.12 .venv）
.venv/bin/pip install -r requirements.txt          # 或 uv pip install ...
cp deploy/config/collector.example.yaml deploy/config/collector.yaml   # 按需改 data_dir / proxy

# 单元与集成测试（不联网，约 10s）
.venv/bin/python -m pytest tests -q

# 前台运行（Ctrl-C / SIGTERM 优雅停止：排空队列、关闭分片、提交 checkpoint）
.venv/bin/python -m data.collector -c deploy/config/collector.yaml run

# 有限时长真实行情 smoke（运行 + 校验 + 回放比较）
scripts/collector_smoke.sh 90

# 长时间运行验收（建议 ≥ 25h 以覆盖一次连接轮换）
scripts/collector_soak.sh            # 或 scripts/collector_soak.sh 90000

# 故障注入（真实行情：强制断连一次 / 模拟磁盘写错误）
scripts/collector_fault_inject.sh

# 按日期校验文件 + 回放
scripts/collector_verify_day.sh 20260926
.venv/bin/python -m data.collector -c deploy/config/collector.yaml verify 20260926
.venv/bin/python -m data.collector -c deploy/config/collector.yaml replay BTCUSDT 20260926 [--out x.parquet]
.venv/bin/python -m data.collector -c deploy/config/collector.yaml status
```

回放比较口径：用 raw 层的深度增量与 REST 快照原文，按本机接收时间顺序重放**同一份**状态机代码，
在 `(book_epoch, u)` 上逐档精确比较价格与数量；报告 mismatched / only_in_replay / only_in_online。
同一天多个 session（重启）时按 session 分别回放比较（键 = session_id, book_epoch, u），并汇总 totals。同一份数据重复回放结果确定（工具自检 `deterministic`）。

### 4.0 监控面板（只读）

```bash
.venv/bin/python -m data.collector -c deploy/config/collector.yaml dashboard --port 8787
# 浏览器打开 http://127.0.0.1:8787 ；远程机器：ssh -L 8787:127.0.0.1:8787 user@host
```
默认只监听 127.0.0.1，不写任何数据。页面每 5s 刷新状态、每 30s 刷新存量：
健康级别与原因、采集器是否在线（按 status.json 新鲜度判断）、管线计数（收到/已写/已 fsync/队列/溢出/写线程）、
连接状态与各流静默秒数、每个交易对的盘口状态/epoch/各流计数/延迟分位/未修复缺口、
中间价与价差曲线（epoch 切换处断开不连线）、每小时行数、最新前 20 档盘口与最近成交（标出补数来源）、
normalized 各表 × 交易对 × 日期的文件数/行数/大小/时间范围、gaps（每个 gap_id 最新状态，可按状态筛选）、质量事件、日报正文。
存量统计只读 Parquet 页脚与行组统计，不扫描数据；API 见 `data/collector/dashboard.py`。

### 4.1 长期运行：systemd（全部运维见 `deploy/OPERATIONS.md`）

```bash
deploy/install.sh                       # 安装 4 个用户级单元并启动，并启用 linger（本机实测均不需要 root）
loginctl show-user $(id -un) -p Linger  # 应为 yes：无人登录也随机器启动
deploy/bin/collectorctl status|logs|health|stop|start|units
```
单元：`binance-collector.service`（`Type=notify`，`WatchdogSec=90`，写线程失败即停止喂狗，systemd 优雅重启；`Restart=always`）、
`binance-dashboard.service`（面板）、`binance-healthcheck.timer`（每 5 分钟一行健康摘要进 journal）、
`binance-daily-verify.timer`（每天 UTC 00:40 校验昨天分片并回放比较）。
采集器依赖本机用户级 clash 代理，因此单元也放在用户级并 `After=clash.service`；可直连的服务器用 `deploy/systemd/system/` 的系统级版本。
重启、断电、断网后的行为，健康级别含义，磁盘、升级、排障，都在 `deploy/OPERATIONS.md`。

### 4.2 时钟检查（不修改系统时钟）

```bash
timedatectl status | grep -iE 'synchronized|ntp'
chronyc tracking            # 若用 chrony
ntpq -p                     # 若用 ntpd
```
`status.json.clock` 提供 `/fapi/v1/time` 的 RTT、`server - local` 偏差估计（RTT/2 假设）以及墙钟跳变检测次数。

### 4.3 运行提醒

- 休眠、关机、网络中断、代理故障都会造成缺口；重启后会在 `gaps` 记录 `downtime`（从上次 checkpoint 提交时刻到本次启动，实际停止时间可能更晚）。
- 本机直连 `fapi/fstream.binance.com` 超时，实测需经本地代理（配置 `proxy`）。代理增加延迟（实测 recv−E 中位数约 145ms，含约 160ms 时钟偏差估计）。
- 磁盘：`disk_warn_free_gb`（degraded）/ `disk_fail_free_gb`（停止采集并标记 failed）。60s 双币约 4.6 MB raw（压缩前）+ 少量 parquet，
  折合约 **6–7 GB/天（raw 未压缩口径）**，gzip 后约 1/5；请按此规划磁盘。

---

## 5. 持久化语义与崩溃恢复

- 接收（WS 线程/事件循环）→ 有界 `asyncio.Queue`（200k）→ dispatcher → 有界写队列（200k）→ **写线程**（raw JSONL + Parquet）。
  压缩与 Parquet 写入在写线程，不阻塞接收。
- 三个位置分别统计：**已收到**（`messages_dispatched`）、**已写入**（`raw_records_written`，进入 OS 缓存）、**已持久化**（`raw_records_fsynced`）。
  默认 `raw_flush_interval_s=1`、`raw_fsync_interval_s=5`：**异常断电最多丢最近 5 秒已写未 fsync 的数据**；入队成功不等于已保存。
- 队列满 → 记录 `queue_overflow` gap（确定）与计数，健康降级；写线程异常（如 ENOSPC）→ `health.level=failed`，后续记录计入 `overflow_total`，不再报告"正常"。
- checkpoint 每 10s 提交，只包含已 fsync 的位置与最后成交 `a`；正常停止时标记 `clean_shutdown=true`。
- 重启：扫描无 manifest 的 `.jsonl`，读出完整行，损坏尾部原样另存 `.tail.corrupt`，主文件截断到最后完整行并写 manifest（`recovered=true`）；
  不覆盖旧文件；以上次 checkpoint 为水位为每个流登记 `downtime` gap；aggTrade 缺口进入补数队列。
- Parquet 每批（5000 行或 30s）独立小文件、先写 tmp 再 rename，崩溃只丢当前批。

---

## 6. 质量报告

- `state/status.json` 每 5s 刷新；`reports/<day>.json|.md` 每 5 分钟及日切/停止时生成。
- 内容：健康级别与原因、每条连接状态/重连/轮换/各流消息数与静默秒数、队列积压与溢出、写入/fsync 计数、落盘滞后、
  每个交易对每条流的收到/接受/重复/乱序/解析错误、首末接收时间、gap 统计与未修复数、最长缺口、
  `recv−E` 的 P50/P95/P99（注明不是单向延迟）、盘口状态/epoch/重同步/断档/交叉/覆盖失败、时钟偏差、REST 权重使用、补数进度、磁盘。
- **不**用"收到行数 / 预计每秒 10 行"计算完整率；某 100ms 没有消息不判定丢包（100ms 流只在有变化时推送）。
  区分：确定断档（序号）、疑似停滞（STALE / markPrice 间隔异常）、正常无事件。

示例见 `docs/example_report_20260925.md`（60s smoke）与 `docs/example_report_rotation_run.md`（轮换运行）。

---

## 7. 测试与验收状态

| 项 | 覆盖 | 状态 |
|---|---|---|
| 1 快照与缓存衔接（含 U==last、u==last、缓存全旧、快照早于缓存） | `tests/test_orderbook.py` | 通过 |
| 2 数量覆盖而非累加 | 同上 | 通过 |
| 3 归零删除 / 删除未知价位 | 同上 | 通过 |
| 4 重复/过旧不重复应用；同 u 内容冲突 | 同上 | 通过 |
| 5 pu 断档 → INVALID → 新 epoch | 同上 | 通过 |
| 6 无效期间不产生有效盘口；交叉/解析失败整条不应用 | 同上 | 通过 |
| 7 主动买卖方向 | `tests/test_trades.py` | 通过 |
| 8 Decimal/定点往返无损；非法精度报错不取整；float 拒收 | `tests/test_orderbook.py` | 通过 |
| 9 成交分页补数、重叠去重、partial/unrepairable、限流重试、窗口外 | `tests/test_backfill.py` | 通过 |
| 10 新旧连接轮换、订阅无数据、服务端关闭退避重连、接收队列溢出 | `tests/test_ws.py`（本地 ws 服务器） | 通过 |
| 11 写入失败、队列积压、异常重启恢复（分片尾部 + downtime + 水位） | `tests/test_writer_and_recovery.py`、`tests/test_storage.py` | 通过 |
| 12 原始日志离线回放与在线一致、重复回放确定（按 session） | `tests/test_replay.py` + 真实数据 `replay` 命令 | 通过 |
| 配置校验、verify 工具 | `tests/test_config_and_verify.py` | 通过 |

**已执行的联网验收**（本机经代理，生产环境）——见 §8 记录。
**尚未验证**（需长期运行）：24h 连接轮换在真实环境的连续性、多日磁盘增长、跨 UTC 日切分片、REST 限流（429/418）真实触发路径、
长时间内存占用。短时测试通过不等于长期稳定性已验证。

---

## 8. 联网验收记录

所有联网测试均在本机（Linux，经本地 HTTP 代理 127.0.0.1:7890，直连超时）对**生产环境**执行，未使用测试网、未使用密钥。

**A. 60s smoke（2026-09-25 19:28–19:29 UTC，`data/store`）**
- 两个交易对盘口均在收到首条增量后取快照、一次衔接成功进入 LIVE（epoch 1）；首次快照因早于缓存被正确拒绝并重取（`need_snapshot: snapshot_older_than_buffer`）。
- depth 收到 580/579 条，接受 576/575（其余为快照前的过旧事件），无 pu 断档、无交叉、无冲突；aggTrade 258/284 条无跳号；markPrice 60/59 条。
- raw 1839 行全部 fsync，10 个分片关闭并生成 manifest；`verify` 全部通过。
- `replay`：BTCUSDT 576/576、ETHUSDT 575/575 逐档一致，重复回放确定。
- recv−E：depth P50≈147ms、P99≈167ms；`/fapi/v1/time` RTT 291–613ms，offset(server−local) 估计 +160ms（经代理，仅供参考）。
- 报告样例：`docs/example_report_20260925.md`、`docs/example_status.json`。

**B. 强制连接轮换 + 分片滚动（2026-09-26 03:35–03:37 UTC，110s，`rotate_after_s=40`，`raw_shard_max_seconds=30`）**
- public/market 各轮换 2 次，重连 0 次。重叠期重复消息：depth 99/100 条、aggTrade 15/41 条被识别为重复并丢弃，**未重复应用**。
- 两个盘口全程 epoch 1、`seq_gaps=0`：新旧连接切换处的 pu 链连续，连续性由更新编号验证而非假设。
- 32 个 raw 分片滚动、压缩、manifest 全部校验通过；book_top20 2127 行、agg_trades 1079 行，键唯一。
- `replay`：1063/1063、1064/1064 一致。报告样例：`docs/example_report_rotation_run.md`。

**C. 故障注入（2026-09-26 03:35–03:37 UTC，`scripts/collector_fault_inject.sh`）**
- 连接建立 20s 后强制关闭（code 4000）：两条连接 ~1s 退避后重连；盘口先 STALE（疑似）后因新连接首条 `pu` 不衔接进入 INVALID，重同步为 **epoch 2**；
  gaps 记录 1 条（`depth_stale` → 升级为 certain，`repair_status=not_applicable`，注明"已用新快照恢复当前盘口，区间内历史不可恢复"）。最终两盘口 LIVE。
- 写入 300 条后注入 ENOSPC：写线程进入失败状态，`health.level=failed, reasons=[writer_failed]`，后续 3567 个写操作被计入 `overflow_total`，
  最终日志 `raw 300 行(fsync 255)`：**明确报告已写与已持久化的差异**，未宣称"采集正常"。未关闭分片留待下次启动恢复。

**D. 重启 + 真实成交补数（2026-09-26 03:44–03:46 UTC）**
- 上一会话正常停止（`clean_shutdown=true`，checkpoint 记录已 fsync 的最后 `a`）；间隔约 55s 后重启。
- 重启登记 `downtime` gap；首条 ws 成交跳号，形成 `aggtrade_id` gap：BTCUSDT 缺 412 条、ETHUSDT 缺 345 条。
- 补数任务各用 1 页 `/fapi/v1/aggTrades?fromId=` 拉回全部缺失成交（`gaps_repaired=2`，权重使用 78/1200），两条 gap 关闭为 `repaired`，
  `downtime` gap 关闭为 `not_applicable` 并指向对应 `aggtrade_id` gap。
- `agg_trades` 表：ws 1574/1584 行 + rest_backfill 412/345 行，(symbol, a) 无重复；补数行 `E_ms=null`、`recv_time_ns=null`、`known_time_ns` 为 REST 响应接收时间。
- 更早两次会话因当时 checkpoint 尚无成交水位，其 downtime 缺口标为 `unrepairable`（"missing count unknown"），不宣称无缺失。

**E. 离线测试**：`pytest tests -q` → 77 passed（约 9s）。

**未执行 / 待长期运行**：真实 24h 轮换、跨 UTC 日切、多日磁盘增长、真实 429/418 限流、跨 2 天窗口的补数拒绝路径、长期内存。

---

## 9. 已知边界

- 交易所侧漏推、聚合口径、RPI 不可见等上游因素无法由本项目检测或弥补。
- markPrice 无历史补数接口，缺失即缺失。depth 缺口只能恢复当前盘口。
- aggTrades 补数受 2 天窗口限制；停机超过 2 天的成交缺口标记 `unrepairable`。
- `recv_time_ns` 依赖本机时钟；时钟被 NTP 阶跃调整时会记录 `clock_jump` 事件，但已记录的时间不回改。
- 写线程失败后进程本身不退出，但停止喂 systemd 看门狗，90s 后被 SIGTERM 优雅重启；非 systemd 前台运行时需人工重启，`status.json` 会一直显示 `failed`。
