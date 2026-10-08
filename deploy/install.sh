#!/usr/bin/env bash
# 安装为当前用户的 systemd 用户级服务（不需要 root），并启用开机自启。
# 唯一需要 sudo 的一步：loginctl enable-linger（让用户级服务在无人登录时也随机器启动）。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_DIR="$HOME/.config/systemd/user"
ME="$(id -un)"

echo "== 前置检查 =="
[ -x "$ROOT/.venv/bin/python" ] || { echo "缺少 $ROOT/.venv/bin/python，先建虚拟环境"; exit 1; }
[ -f "$ROOT/deploy/config/collector.yaml" ] || { echo "缺少 deploy/config/collector.yaml（cp deploy/config/collector.example.yaml 后修改）"; exit 1; }
[ "$ROOT" = "$HOME/hf_forecasting" ] || echo "警告: 单元文件假定项目在 ~/hf_forecasting，当前是 $ROOT，请改 deploy/systemd/user/*.service 里的路径"
"$ROOT/.venv/bin/python" -c "import sys; sys.path.insert(0,'$ROOT'); from data.collector.config import load_config; c=load_config('$ROOT/deploy/config/collector.yaml'); print('配置 OK: symbols', c.symbols, 'data_dir', c.data_path, 'proxy', c.proxy)"
if systemctl --user list-unit-files clash.service >/dev/null 2>&1 && systemctl --user is-enabled clash.service >/dev/null 2>&1; then
  echo "clash.service 已启用（采集器 Wants=clash.service）"
else
  echo "提示: 未发现用户级 clash.service；若需代理请确认 config 里 proxy 指向的代理会随机器启动"
fi

echo "== 安装单元文件到 $UNIT_DIR =="
mkdir -p "$UNIT_DIR"
cp "$ROOT"/deploy/systemd/user/binance-*.service "$ROOT"/deploy/systemd/user/binance-*.timer "$UNIT_DIR/"
chmod +x "$ROOT"/deploy/bin/*
systemctl --user daemon-reload
systemctl --user enable --now binance-collector.service binance-dashboard.service binance-healthcheck.timer binance-daily-verify.timer
sleep 3
systemctl --user --no-pager --lines=0 status binance-collector.service | head -12 || true

echo
echo "== 开机自启（无人登录也启动）=="
if [ "$(loginctl show-user "$ME" -p Linger --value 2>/dev/null)" = "yes" ]; then
  echo "Linger 已启用"
else
  # 对自己启用 linger 通常走 polkit，活动会话下不需要 sudo；失败再提示管理员
  if loginctl enable-linger 2>/dev/null && [ "$(loginctl show-user "$ME" -p Linger --value 2>/dev/null)" = "yes" ]; then
    echo "已为 $ME 启用 Linger（无需 sudo）"
  else
    echo "Linger 未能自动启用。请让有 root 权限的人执行一次："
    echo "    sudo loginctl enable-linger $ME"
    echo "否则重启后要等你登录，用户级服务（clash 与采集器）才会启动。"
  fi
fi
echo
echo "完成。常用命令: deploy/bin/collectorctl {status|logs|health|stop|start|units}"
echo "面板: http://127.0.0.1:8787"
