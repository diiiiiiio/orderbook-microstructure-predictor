"""status.json 实时状态 + 每日报告（JSON/Markdown）。"""
from __future__ import annotations

import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def disk_free_gb(path: Path) -> float:
    try:
        return shutil.disk_usage(path).free / 1e9
    except FileNotFoundError:
        return float("nan")


def dir_size_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except FileNotFoundError:
                pass
    return total


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=1, default=str)
    os.replace(tmp, path)


def fmt_ns(ns: int | None) -> str:
    if ns is None:
        return "-"
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def render_markdown(status: dict[str, Any]) -> str:
    L: list[str] = []
    L.append(f"# 采集质量报告 {status.get('report_day', '')}")
    L.append("")
    L.append(f"- 生成时间: {fmt_ns(status.get('time_ns'))}")
    L.append(f"- session_id: {status.get('session_id')}  运行时长: {status.get('uptime_s')} s")
    L.append(f"- 整体状态: **{status.get('health', {}).get('level')}** {status.get('health', {}).get('reasons')}")
    L.append(f"- 剩余磁盘: {status.get('disk', {}).get('free_gb')} GB  数据目录大小: {status.get('disk', {}).get('data_dir_bytes')} B")
    L.append("")
    L.append("## 连接")
    for role, c in status.get("connections", {}).items():
        cur = c.get("current") or {}
        L.append(f"- {role}: {c.get('state')} 重连 {c.get('reconnects')} 轮换 {c.get('rotations')} "
                 f"当前连接 {cur.get('connection_id')} 消息 {cur.get('messages')} 队列丢弃 {cur.get('queue_drops')}")
    L.append("")
    L.append("## 队列/落盘")
    qs = status.get("pipeline", {})
    L.append(f"- 队列积压: {qs.get('queue_size')} / {qs.get('queue_max')}  溢出丢弃: {qs.get('queue_overflow_total')}")
    L.append(f"- raw 已写入 {qs.get('raw_records_written')} 行，已 fsync {qs.get('raw_records_fsynced')} 行；"
             f"normalized 待写 {qs.get('normalized_pending')} 行")
    L.append(f"- 落盘延迟(写线程滞后) : {qs.get('writer_lag_s')} s")
    L.append("")
    L.append("## 按交易对/数据流")
    for sym, streams in status.get("symbols", {}).items():
        L.append(f"### {sym}")
        book = streams.get("book", {})
        L.append(f"- 盘口状态 {book.get('state')} epoch {book.get('book_epoch')} 有效行 {book.get('rows_emitted')} "
                 f"重同步 {book.get('resyncs')} 断档 {book.get('seq_gaps')} 交叉 {book.get('crosses')} "
                 f"覆盖失败 {book.get('coverage_failures')} 快照失败 {book.get('snapshots_failed')}")
        for name in ("depth", "aggTrade", "markPrice"):
            s = streams.get(name)
            if not s:
                continue
            lat = s.get("recv_minus_exchange_ms") or {}
            L.append(f"- {name}: 收到 {s.get('received')} 接受 {s.get('accepted')} 重复 {s.get('duplicates')} "
                     f"乱序/过旧 {s.get('old_or_out_of_order')} 解析错误 {s.get('parse_errors')} "
                     f"首 {fmt_ns(s.get('first_recv_time_ns'))} 末 {fmt_ns(s.get('last_recv_time_ns'))}")
            L.append(f"  - recv-E ms P50/P95/P99: {lat.get('p50')}/{lat.get('p95')}/{lat.get('p99')} (n={lat.get('n')})；"
                     f"gaps: {s.get('gaps')}；未修复缺口 {s.get('open_gaps')}；最长缺口 {s.get('longest_gap')}")
            if s.get("extra"):
                L.append(f"  - 其他计数: {s.get('extra')}")
    L.append("")
    L.append("## 时钟")
    L.append(f"- {status.get('clock')}")
    L.append("")
    L.append("## 补数")
    L.append(f"- {status.get('backfill')}")
    L.append("")
    L.append("> 说明：recv-E 是本机接收时间减交易所事件时间，含时钟偏差，不是精确单向延迟。"
             "序号连续与文件校验通过不代表上游市场信息绝对完整。缺口恢复只恢复当前盘口，不补齐历史。")
    return "\n".join(L) + "\n"
