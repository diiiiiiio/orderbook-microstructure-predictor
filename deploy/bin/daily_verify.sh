#!/usr/bin/env bash
# 校验昨天（UTC）的 raw 分片 manifest/SHA-256，并回放比较盘口；结果写到 <data_dir>/reports/verify_<day>.json 并记 journal。
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CFG="${CFG:-$ROOT/deploy/config/collector.yaml}"
PY="${PY:-$ROOT/.venv/bin/python}"
DAY="${1:-$(date -u -d 'yesterday' +%Y%m%d)}"
cd "$ROOT"
DATA_DIR="$("$PY" -c "import sys; sys.path.insert(0,'.'); from data.collector.config import load_config; print(load_config('$CFG').data_path)")"
OUT="$DATA_DIR/reports/verify_${DAY}.json"
mkdir -p "$DATA_DIR/reports"
rc=0
"$PY" -m data.collector -c "$CFG" verify "$DAY" > "$OUT.verify.tmp" || rc=1
for S in $("$PY" -c "import sys; sys.path.insert(0,'.'); from data.collector.config import load_config; print(' '.join(load_config('$CFG').symbols))"); do
  "$PY" -m data.collector -c "$CFG" replay "$S" "$DAY" > "$OUT.replay.$S.tmp" || rc=1
done
"$PY" - "$OUT" "$DAY" <<'PYEOF'
import json, sys, glob, os
out, day = sys.argv[1], sys.argv[2]
res = {"day": day, "verify": json.load(open(out + ".verify.tmp")), "replay": {}}
for f in glob.glob(out + ".replay.*.tmp"):
    res["replay"][f.split(".replay.")[1][:-4]] = json.load(open(f))
    os.remove(f)
os.remove(out + ".verify.tmp")
json.dump(res, open(out, "w"), ensure_ascii=False, indent=1)
v = res["verify"]; ok = v["ok"] and all(r["ok"] for r in res["replay"].values())
print(f"VERIFY {day} ok={ok} raw_shards={len(v['raw'])} " +
      " ".join(f"{s}:matched={r['totals']['matched']}/{r['totals']['compared']} mism={r['totals']['mismatched']}" for s, r in res["replay"].items()))
sys.exit(0 if ok else 1)
PYEOF
