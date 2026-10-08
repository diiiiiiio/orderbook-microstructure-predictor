#!/usr/bin/env bash
# 故障注入 smoke：真实行情下 (1) 连接建立 20s 后强制断开一次，验证重连+重同步；
# (2) 写入 N 条后模拟磁盘错误，验证进入 failed 状态。各自独立运行。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="${PY:-.venv/bin/python}"
BASE="${CFG:-deploy/config/collector.yaml}"
TMP="$(mktemp -d)"
sed -e 's|^data_dir:.*|data_dir: '"$TMP"'/store_ws|' -e 's|^  inject_close_after_s:.*||' "$BASE" > "$TMP/ws.yaml"
printf '\n' >> "$TMP/ws.yaml"
"$PY" - "$TMP/ws.yaml" <<'PYEOF'
import sys, yaml
p=sys.argv[1]; c=yaml.safe_load(open(p)); c.setdefault('ws',{})['inject_close_after_s']=20; yaml.safe_dump(c, open(p,'w'), allow_unicode=True)
PYEOF
echo "== (1) ws 断开注入，运行 75s =="
"$PY" -m data.collector -c "$TMP/ws.yaml" run --seconds 75 2>&1 | grep -E "fault|连接结束|重连|已连接|停止|resync" || true
"$PY" - "$TMP/store_ws/state/status.json" <<'PYEOF'
import json,sys; s=json.load(open(sys.argv[1]))
print("health:", s["health"]); print({r:(c["reconnects"],c["last_error"]) for r,c in s["connections"].items()})
for sym,v in s["symbols"].items(): print(sym, "epoch", v["book"]["book_epoch"], "state", v["book"]["state"], "resyncs", v["book"]["resyncs"], "gaps", v["depth"]["gaps"])
PYEOF
sed -e 's|^data_dir:.*|data_dir: '"$TMP"'/store_disk|' "$BASE" > "$TMP/disk.yaml"
"$PY" - "$TMP/disk.yaml" <<'PYEOF'
import sys, yaml
p=sys.argv[1]; c=yaml.safe_load(open(p)); c.setdefault('storage',{})['inject_write_error_after_records']=300; yaml.safe_dump(c, open(p,'w'), allow_unicode=True)
PYEOF
echo "== (2) 写入错误注入，运行 40s =="
"$PY" -m data.collector -c "$TMP/disk.yaml" run --seconds 40 2>&1 | grep -E "写入失败|ENOSPC|停止|失败状态" || true
"$PY" - "$TMP/store_disk/state/status.json" <<'PYEOF'
import json,sys; s=json.load(open(sys.argv[1]))
print("health:", s["health"]); print("writer error:", s["pipeline"]["error"], "written:", s["pipeline"]["raw_records_written"], "overflow:", s["pipeline"]["overflow_total"])
PYEOF
echo "临时数据目录: $TMP"
