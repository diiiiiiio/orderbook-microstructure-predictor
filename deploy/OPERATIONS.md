# 运维手册（deploy/）

采集器以 **systemd 用户级服务** 运行（与本机的用户级 clash 代理同级），由 systemd 负责开机自启、崩溃重启、看门狗；
由采集器自身负责 WebSocket 重连、盘口重同步、成交补数、缺口登记。本文件是全部运维事项的唯一入口。

## 0. 一次性安装

```bash
deploy/install.sh                       # 安装并启动 4 个单元，并尝试 loginctl enable-linger（不需要 root）
loginctl show-user $(id -un) -p Linger  # 应显示 Linger=yes：无人登录也启动用户级服务（clash + 采集器）
# 若显示 no（polkit 不允许），请管理员执行一次: sudo loginctl enable-linger <用户名>
```
可选（系统级，sudo）：`sudo cp deploy/journald/binance.conf /etc/systemd/journald.conf.d/ && sudo systemctl restart systemd-journald` 限制日志总量。

安装的单元（`~/.config/systemd/user/`）：

| 单元 | 作用 |
|---|---|
| `binance-collector.service` | 采集器。`Type=notify` + `WatchdogSec=90`，`Restart=always` |
| `binance-dashboard.service` | 只读面板 http://127.0.0.1:8787 |
| `binance-healthcheck.timer` | 每 5 分钟跑 `deploy/bin/healthcheck.sh`，一行摘要写入 journal |
| `binance-daily-verify.timer` | 每天 UTC 00:40 校验昨天分片 + 回放比较，结果 `<data_dir>/reports/verify_<day>.json` |

## 1. 日常命令

```bash
deploy/bin/collectorctl status      # systemd 状态 + 健康摘要一行
deploy/bin/collectorctl logs        # 跟踪日志（journalctl --user -u binance-collector -f）
deploy/bin/collectorctl health      # 退出码 0=ok 1=degraded 2=failed 3=未运行/状态过期
deploy/bin/collectorctl units       # 所有 binance-* 单元与定时器
deploy/bin/collectorctl stop|start|restart
deploy/bin/collectorctl verify [YYYYMMDD]   # 手动跑校验+回放（默认昨天）
journalctl --user -u binance-healthcheck --since -1d   # 过去一天的健康摘要
```
`stop` 是优雅停止：排空队列、关闭并压缩分片、提交 checkpoint，最长 180s。**不要用 kill -9**，会留下未关闭分片（下次启动能恢复，但会截断损坏尾部）。

## 2. 重启机器 / 断电后会发生什么

1. 机器起来 → systemd 因 linger 启动用户会话 → `clash.service` 与 `binance-collector.service` 启动（`After=clash.service`）。
2. 采集器扫描上次未关闭的 `.jsonl` 分片：读出完整行、损坏尾部另存 `.tail.corrupt`、补 manifest。
3. 以上次 checkpoint 为水位登记 `downtime` 缺口（depth/markPrice 标 `unrepairable`，aggTrade 标 `open`）。
4. 连接 WS，收到首条增量后取快照，新 `book_epoch` 进入 LIVE；成交按 `a` 跳号形成缺口 → REST 补数（窗口 2 天）。
5. 断电时最多丢失最近 `raw_fsync_interval_s`（默认 5s）内已写未 fsync 的数据；checkpoint 只记录已 fsync 位置。

**停机期间的盘口和标记价格无法补回**，会在 `gaps` 表如实记录；面板顶部会显示 open/unrepairable 计数。

## 3. 连接出问题时会发生什么

| 情况 | 行为 |
|---|---|
| WS 被服务端关闭 / 网络断 / 30s 无任何消息 | 随机抖动指数退避重连（1s→60s），事件写 `events` 分片与 `quality_events` |
| 单个流静默（depth>5s、markPrice>5s、aggTrade>120s） | 记 `ws_stream_silent`；depth/markPrice 静默直接重建连接 |
| 24h 连接上限 | 23h 时开新连接、重叠 ≤30s 后关旧连接；重叠期重复消息按 `u`/`a` 去重，不重复应用 |
| 重连后 `pu` 不衔接 | 盘口 INVALID → 新 epoch 重新同步；`gaps` 记 `depth_sequence` |
| REST 429/418 | 按 Retry-After 退避，本地权重预算 1200/min（官方 2400） |
| REST 403/451 | 不重试，`health.reasons` 含 `rest_forbidden`，快照拿不到 → 盘口停在 SYNCING（degraded） |
| 代理 clash 挂掉 | 采集器所有连接失败 → 持续退避重连；clash `Restart=on-failure` 起来后自动恢复 |
| 写线程失败（磁盘满 / IO 错误） | `health=failed`，停止喂狗 → 90s 后 systemd SIGTERM 重启；若磁盘仍满会反复重启，10 分钟 20 次后停止并需人工 `collectorctl reset` |
| 剩余磁盘 < `disk_fail_free_gb`(5GB) | 采集器主动优雅停止，systemd 5s 后重启 → 再次停止…直到 StartLimit；**先清磁盘再 reset** |

## 4. 健康级别怎么读

- `ok`：两盘口 LIVE、连接 connected、无写错误、磁盘充足。
- `degraded`：任一盘口非 LIVE（同步中/STALE/INVALID）、连接在重连、磁盘低于 20GB、REST 受限、有队列溢出记录。短暂 degraded 正常（重连、重同步几秒）；持续 degraded 看 `reasons` 与日志。
- `failed`：写线程失败 / 死亡，或磁盘低于 5GB。数据正在丢失，立即处理。
- healthcheck 的 `STALE`：status.json 超过 30s 未更新，说明进程没在跑或卡死。

## 5. 磁盘

- 估算：raw 未压缩约 6–7 GB/天（两个交易对），关闭分片后 gzip 约 1/5；Parquet 研究层约 0.5–1 GB/天。
- 检查：`df -h <data_dir>`，面板"数据目录"tile，`status.json.disk`。
- 清理旧数据：直接删除 `raw/*/*/<YYYYMMDD>/` 与 `normalized/*/*/<YYYYMMDD>/` 整日目录即可（先归档）；不要删当天的活动分片。
- 迁移 `data_dir`：`collectorctl stop` → 改 `deploy/config/collector.yaml` 的 `data_dir` → `mv` 旧目录（含 `state/`，否则丢 checkpoint 会多记一段"水位未知"缺口）→ `collectorctl start`。

## 6. 升级代码 / 改配置

```bash
deploy/bin/collectorctl stop
git pull / 修改代码 / 修改 deploy/config/collector.yaml
.venv/bin/python -m pytest tests -q          # 必须全过
deploy/bin/collectorctl start && deploy/bin/collectorctl status
```
改了 `deploy/systemd/user/*` 后重新运行 `deploy/install.sh`（会覆盖单元文件并 daemon-reload）。
停机期间会形成缺口，选择成交少的时段。

## 7. 排障速查

```bash
systemctl --user status binance-collector -l          # 看 Status: 行（采集器自报：级别、盘口状态、raw/fsync 计数）
journalctl --user -u binance-collector -n 200 --no-pager
journalctl --user -u binance-collector --since "1 hour ago" | grep -E "ERROR|WARNING|重连|resync"
cat <data_dir>/state/status.json | python3 -m json.tool | less
ls <data_dir>/raw/depth/BTCUSDT/$(date -u +%Y%m%d)/ | tail    # 活动分片 .jsonl 应在持续增长
systemctl --user reset-failed binance-collector       # 触发 StartLimit 后
```
时钟：`timedatectl status`（应 synchronized: yes）；采集器不会改时钟，只在 `quality_events` 记 `clock_jump`。

## 8. 已知边界（运维视角）

- 依赖用户级 clash：clash 起不来则采集不到任何数据，健康显示 degraded + 连接 backoff。可考虑把 clash 也改成系统级服务。
- Linger 未启用时，重启后必须有人登录才会开始采集。安装脚本会检查并提示。
- systemd 重启只能恢复"当前"，不能补回停机期间的盘口；这由设计决定，面板与 gaps 表如实呈现。
