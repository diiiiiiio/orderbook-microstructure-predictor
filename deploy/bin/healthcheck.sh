#!/usr/bin/env bash
# 健康检查：读 status.json，打印一行摘要。退出码 0=ok 1=degraded 2=failed 3=采集器未运行/无状态。
# 被 binance-healthcheck.timer 每 5 分钟调用并写入 journal；也可手动运行。
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CFG="${CFG:-$ROOT/deploy/config/collector.yaml}"
PY="${PY:-$ROOT/.venv/bin/python}"
cd "$ROOT"
"$PY" - "$CFG" <<'PYEOF'
import json, sys, time
sys.path.insert(0, ".")
from data.collector.config import load_config
cfg = load_config(sys.argv[1])
p = cfg.data_path / "state" / "status.json"
if not p.exists():
    print(f"NO_STATUS data_dir={cfg.data_path} (采集器从未运行或目录不对)"); sys.exit(3)
s = json.load(open(p))
age = (time.time_ns() - s["time_ns"]) / 1e9
h = s["health"]; pipe = s["pipeline"]
books = ",".join(f"{k}:{v['book']['state']}" for k, v in s["symbols"].items())
open_gaps = sum(v[st]["open_gaps"] for v in s["symbols"].values() for st in ("depth", "aggTrade", "markPrice"))
conns = ",".join(f"{r}:{c['state']}/r{c['reconnects']}" for r, c in s["connections"].items())
line = (f"{h['level'].upper()} age={age:.0f}s books={books} conns={conns} raw_written={pipe['raw_records_written']} "
        f"fsynced={pipe['raw_records_fsynced']} q={pipe['queue_size']} wq={pipe['writer_queue_size']} "
        f"overflow={pipe['queue_overflow_total']}+{pipe['overflow_total']} open_gaps={open_gaps} "
        f"disk_free_gb={s['disk']['free_gb']} reasons={h['reasons']}")
if age > cfg.monitoring.status_interval_s * 6:
    print("STALE " + line + f" stop_reason={h.get('stop_reason')}"); sys.exit(3)
print(line)
sys.exit({"ok": 0, "degraded": 1, "failed": 2}.get(h["level"], 2))
PYEOF
