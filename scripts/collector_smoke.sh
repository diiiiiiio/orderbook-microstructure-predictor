#!/usr/bin/env bash
# 有限时长真实行情 smoke test：运行 N 秒（默认 90），然后校验 + 回放比较。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
SECS="${1:-90}"
CFG="${CFG:-deploy/config/collector.yaml}"
PY="${PY:-.venv/bin/python}"
DAY="$(date -u +%Y%m%d)"
echo "== run ${SECS}s =="
"$PY" -m data.collector -c "$CFG" run --seconds "$SECS"
echo "== verify $DAY =="
"$PY" -m data.collector -c "$CFG" verify "$DAY" | tail -25
for S in BTCUSDT ETHUSDT; do
  echo "== replay $S $DAY =="
  "$PY" -m data.collector -c "$CFG" replay "$S" "$DAY" | grep -E '"(compared|matched|mismatched|only_in_replay|only_in_online|deterministic|ok)"' | head -8
done
echo "== health =="
"$PY" -c "import json;s=json.load(open('$( "$PY" -c "import sys;sys.path.insert(0,'.');from data.collector.config import load_config;print(load_config('$CFG').data_path)" )/state/status.json'));print(s['health']);print({k:s['pipeline'][k] for k in ('raw_records_written','raw_records_fsynced','overflow_total','queue_overflow_total','error')})"
