"""命令行入口：python -m data.collector <command> [...]"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from data.collector.config import load_config


def cmd_run(a: argparse.Namespace) -> int:
    from data.collector.collector import Collector
    cfg = load_config(a.config)
    logging.basicConfig(level=getattr(logging, cfg.monitoring.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    c = Collector(cfg, run_seconds=a.seconds)
    return asyncio.run(c.run())


def cmd_verify(a: argparse.Namespace) -> int:
    from data.collector.verify import verify_day
    cfg = load_config(a.config)
    res = verify_day(cfg.data_path, a.day, a.symbol)
    print(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    return 0 if res["ok"] else 1


def cmd_replay(a: argparse.Namespace) -> int:
    from data.collector.replay import replay_and_compare
    cfg = load_config(a.config)
    res = replay_and_compare(cfg, a.symbol, a.day, write_parquet=a.out, session_id=a.session)
    print(json.dumps(res["summary"], ensure_ascii=False, indent=1, default=str))
    return 0 if res["summary"]["ok"] else 1


def cmd_status(a: argparse.Namespace) -> int:
    cfg = load_config(a.config)
    p = cfg.data_path / "state" / "status.json"
    if not p.exists():
        print("status.json 不存在", file=sys.stderr)
        return 1
    print(p.read_text(encoding="utf-8"))
    return 0


def cmd_dashboard(a: argparse.Namespace) -> int:
    from data.collector.dashboard import run_dashboard
    cfg = load_config(a.config)
    run_dashboard(cfg, a.host, a.port)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m data.collector")
    ap.add_argument("-c", "--config", default="deploy/config/collector.yaml")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="前台运行采集")
    r.add_argument("--seconds", type=float, default=None, help="运行指定秒数后优雅停止（smoke test）")
    r.set_defaults(fn=cmd_run)
    v = sub.add_parser("verify", help="按日期校验 raw 分片 manifest/SHA-256 与研究层")
    v.add_argument("day")
    v.add_argument("--symbol", default=None)
    v.set_defaults(fn=cmd_verify)
    p = sub.add_parser("replay", help="离线回放原始深度数据并与在线 book_top20 精确比较")
    p.add_argument("symbol")
    p.add_argument("day")
    p.add_argument("--out", default=None, help="把回放盘口写到该 parquet 路径")
    p.add_argument("--session", default=None, help="只回放指定 session_id")
    p.set_defaults(fn=cmd_replay)
    d = sub.add_parser("dashboard", help="启动只读监控面板（浏览器查看状态/数据/缺口）")
    d.add_argument("--host", default="127.0.0.1")
    d.add_argument("--port", type=int, default=8787)
    d.set_defaults(fn=cmd_dashboard)
    s = sub.add_parser("status", help="打印 status.json")
    s.set_defaults(fn=cmd_status)
    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
