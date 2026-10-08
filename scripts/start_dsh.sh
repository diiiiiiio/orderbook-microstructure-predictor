#!/usr/bin/env bash
# Quick-start DeepSeek Harness Web UI with this repo as the workspace.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec "$ROOT/scripts/dsh.sh" start "$@"
