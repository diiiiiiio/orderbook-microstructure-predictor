#!/usr/bin/env bash
# 按日期校验文件与回放：scripts/collector_verify_day.sh 20260926 [SYMBOL]
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CFG="${CFG:-deploy/config/collector.yaml}"
PY="${PY:-.venv/bin/python}"
DAY="${1:?用法: $0 YYYYMMDD [SYMBOL]}"
SYMS="${2:-BTCUSDT ETHUSDT}"
"$PY" -m data.collector -c "$CFG" verify "$DAY" ${2:+--symbol "$2"}
for S in $SYMS; do
  "$PY" -m data.collector -c "$CFG" replay "$S" "$DAY"
done
