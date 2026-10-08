#!/usr/bin/env bash
# 停止并移除用户级服务（不删数据，不改 linger）。
set -uo pipefail
UNIT_DIR="$HOME/.config/systemd/user"
systemctl --user disable --now binance-collector.service binance-dashboard.service binance-healthcheck.timer binance-daily-verify.timer 2>/dev/null
rm -f "$UNIT_DIR"/binance-*.service "$UNIT_DIR"/binance-*.timer
systemctl --user daemon-reload
systemctl --user reset-failed 2>/dev/null
echo "已移除。数据目录未动。如需取消开机自启用户会话: sudo loginctl disable-linger $(id -un)"
