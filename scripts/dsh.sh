#!/usr/bin/env bash
# DeepSeek Harness helper for this repo.
# Usage:
#   ./scripts/dsh.sh start [port]
#   ./scripts/dsh.sh stop
#   ./scripts/dsh.sh status
#   ./scripts/dsh.sh url
#   ./scripts/dsh.sh headless "your task"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST="${DSH_HOST:-127.0.0.1}"
PORT="${DSH_PORT:-${2:-3080}}"
UNIT="dsh.service"
RUN_DIR="$ROOT/.dsh-run"
PID_FILE="$RUN_DIR/dsh.pid"
LOG_FILE="$RUN_DIR/dsh.log"
DSH_BIN="${DSH_BIN:-}"

export PATH="$HOME/.local/bin:$PATH"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"

find_dsh() {
  if [[ -n "$DSH_BIN" && -x "$DSH_BIN" ]]; then
    return 0
  fi
  if command -v dsh >/dev/null 2>&1; then
    DSH_BIN="$(command -v dsh)"
    return 0
  fi
  if [[ -x "$HOME/.local/bin/dsh" ]]; then
    DSH_BIN="$HOME/.local/bin/dsh"
    return 0
  fi
  if [[ -x "$HOME/deepseek-harness/node_modules/.bin/dsh" ]]; then
    DSH_BIN="$HOME/deepseek-harness/node_modules/.bin/dsh"
    return 0
  fi
  echo "error: dsh not found. Install with: npm install --prefix \"$HOME/deepseek-harness\" @deepseek-ai/dsh" >&2
  exit 1
}

has_systemd_unit() {
  systemctl --user cat "$UNIT" >/dev/null 2>&1
}

listening() {
  ss -tln 2>/dev/null | awk '{print $4}' | grep -qx "${HOST}:${PORT}" \
    || ss -tln 2>/dev/null | awk '{print $4}' | grep -qx "*:${PORT}"
}

print_url() {
  local url=""
  if has_systemd_unit; then
    url="$(journalctl --user -u "$UNIT" -n 80 --no-pager --output=cat 2>/dev/null \
      | sed -n 's/.*dsh web: //p' | tail -n 1 || true)"
  fi
  if [[ -z "$url" && -f "$LOG_FILE" ]]; then
    url="$(sed -n 's/.*dsh web: //p' "$LOG_FILE" | tail -n 1 || true)"
  fi
  if [[ -n "$url" ]]; then
    printf '%s\n' "$url"
    return 0
  fi
  echo "Web UI is listening on http://${HOST}:${PORT}/ but no launch token was found yet." >&2
  echo "Wait a few seconds and run: $0 url" >&2
  echo "http://${HOST}:${PORT}/"
  return 1
}

cmd_start() {
  if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
    PORT="$1"
  fi
  find_dsh
  cd "$ROOT"

  if listening; then
    echo "DeepSeek Harness already running on ${HOST}:${PORT}"
    print_url || true
    return 0
  fi

  if has_systemd_unit && [[ "$HOST" == "127.0.0.1" && "$PORT" == "3080" ]]; then
    systemctl --user start "$UNIT"
    echo "Started systemd user service ${UNIT}"
  else
    mkdir -p "$RUN_DIR"
    nohup "$DSH_BIN" web --no-open --host "$HOST" --port "$PORT" \
      >"$LOG_FILE" 2>&1 &
    echo $! >"$PID_FILE"
    echo "Started dsh (pid $(cat "$PID_FILE")), log: $LOG_FILE"
  fi

  for _ in $(seq 1 30); do
    if listening; then
      echo "Listening on ${HOST}:${PORT}"
      sleep 1
      print_url || true
      echo
      echo "Open the printed URL (it includes a one-time token)."
      echo "If you are on SSH, forward the port first:"
      echo "  ssh -L ${PORT}:${HOST}:${PORT} USER@HOST"
      return 0
    fi
    sleep 0.4
  done

  echo "error: dsh did not bind ${HOST}:${PORT} in time" >&2
  if has_systemd_unit; then
    systemctl --user --no-pager --full status "$UNIT" >&2 || true
  elif [[ -f "$LOG_FILE" ]]; then
    tail -n 40 "$LOG_FILE" >&2 || true
  fi
  exit 1
}

cmd_stop() {
  if has_systemd_unit && systemctl --user is-active --quiet "$UNIT"; then
    systemctl --user stop "$UNIT"
    echo "Stopped ${UNIT}"
  fi
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE")"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid"
      echo "Stopped pid ${pid}"
    fi
    rm -f "$PID_FILE"
  fi
  if listening; then
    echo "warning: ${HOST}:${PORT} is still listening" >&2
    exit 1
  fi
}

cmd_status() {
  if has_systemd_unit; then
    systemctl --user --no-pager --full status "$UNIT" || true
    echo
  fi
  if listening; then
    echo "Port ${HOST}:${PORT}: up"
    print_url || true
  else
    echo "Port ${HOST}:${PORT}: down"
  fi
}

cmd_headless() {
  find_dsh
  cd "$ROOT"
  if [[ $# -eq 0 ]]; then
    echo "usage: $0 headless \"your task\"" >&2
    exit 2
  fi
  exec "$DSH_BIN" --profile headless "$@"
}

usage() {
  cat <<EOF
DeepSeek Harness quick start for ${ROOT}

  $0 start [port]     Start Web UI (default ${HOST}:3080)
  $0 stop             Stop Web UI
  $0 status           Show service / port / launch URL
  $0 url              Print the current launch URL
  $0 headless "task"  Run one task without the Web UI

Environment:
  DSH_BIN   override dsh executable
  DSH_HOST  bind host (default 127.0.0.1)
  DSH_PORT  bind port (default 3080)
EOF
}

main() {
  local cmd="${1:-start}"
  shift || true
  case "$cmd" in
    start) cmd_start "$@" ;;
    stop) cmd_stop ;;
    status) cmd_status ;;
    url) print_url ;;
    headless) cmd_headless "$@" ;;
    -h|--help|help) usage ;;
    *)
      echo "unknown command: $cmd" >&2
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
