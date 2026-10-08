#!/usr/bin/env bash
# 长时间运行验收：前台运行（Ctrl-C / SIGTERM 优雅停止）。建议配合 systemd 或 nohup。
#   scripts/collector_soak.sh                # 一直跑
#   scripts/collector_soak.sh 86400          # 跑 24h（跨越一次连接轮换）
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CFG="${CFG:-deploy/config/collector.yaml}"
PY="${PY:-.venv/bin/python}"
if [ -n "${1:-}" ]; then
  exec "$PY" -m data.collector -c "$CFG" run --seconds "$1"
else
  exec "$PY" -m data.collector -c "$CFG" run
fi
