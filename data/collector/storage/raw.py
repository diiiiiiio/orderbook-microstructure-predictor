"""Raw 原始层：可追加的 JSONL 分片。

目录：<data_dir>/raw/<stream_kind>/<SYMBOL>/<YYYYMMDD>/<session>_<conn|rest>_<seq>.jsonl[.gz]
  - 活动分片不压缩、按 flush_interval 刷到 OS、按 fsync_interval 落盘。
  - 按大小或时长滚动；关闭后校验、生成 manifest、再可选 gzip 压缩。
  - 只追加，永不改写；重启时对遗留的 .jsonl 做 recover()：
    读出完整行、把损坏尾部隔离到 .tail.corrupt、不覆盖任何旧文件。

"已收到 / 已写入(write) / 已持久化(fsync)" 三个位置分别跟踪，
checkpoint 只使用 fsync 过的位置。
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

log = logging.getLogger("collector.raw")


def utc_day(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y%m%d")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


@dataclass
class ShardStats:
    records: int = 0
    first_recv_time_ns: int | None = None
    last_recv_time_ns: int | None = None
    first_id: int | None = None
    last_id: int | None = None
    bytes_written: int = 0
    bytes_fsynced: int = 0
    records_fsynced: int = 0
    last_id_fsynced: int | None = None
    last_recv_time_fsynced: int | None = None


class RawShard:
    def __init__(self, path: Path, schema_version: int, meta: dict[str, Any]):
        self.path = path
        self.schema_version = schema_version
        self.meta = meta
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # 只追加；文件已存在（异常情况）也不覆盖
        self.fh = open(self.path, "ab")
        self.opened_mono = time.monotonic()
        self.stats = ShardStats()
        self._last_flush = self.opened_mono
        self._last_fsync = self.opened_mono
        self._pending_records = 0

    def write(self, rec: dict[str, Any], id_value: int | None = None) -> int:
        line = (json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        self.fh.write(line)
        st = self.stats
        st.records += 1
        st.bytes_written += len(line)
        rt = rec.get("recv_time_ns")
        if rt is not None:
            st.first_recv_time_ns = rt if st.first_recv_time_ns is None else st.first_recv_time_ns
            st.last_recv_time_ns = rt
        if id_value is not None:
            st.first_id = id_value if st.first_id is None else st.first_id
            st.last_id = id_value
        self._pending_records += 1
        return len(line)

    def maybe_flush(self, flush_interval_s: float, fsync_interval_s: float, force: bool = False) -> bool:
        """返回是否做了 fsync。"""
        now = time.monotonic()
        synced = False
        if force or now - self._last_flush >= flush_interval_s:
            self.fh.flush()
            self._last_flush = now
        if force or now - self._last_fsync >= fsync_interval_s:
            self.fh.flush()
            os.fsync(self.fh.fileno())
            self._last_fsync = now
            self.stats.bytes_fsynced = self.stats.bytes_written
            self.stats.records_fsynced = self.stats.records
            self.stats.last_id_fsynced = self.stats.last_id
            self.stats.last_recv_time_fsynced = self.stats.last_recv_time_ns
            self._pending_records = 0
            synced = True
        return synced

    def should_rotate(self, max_bytes: int, max_seconds: float) -> bool:
        return (self.stats.bytes_written >= max_bytes
                or time.monotonic() - self.opened_mono >= max_seconds)

    def close(self, compress: bool, manifest_dir: Path | None = None) -> dict[str, Any]:
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.close()
        self.stats.bytes_fsynced = self.stats.bytes_written
        self.stats.records_fsynced = self.stats.records
        self.stats.last_id_fsynced = self.stats.last_id
        self.stats.last_recv_time_fsynced = self.stats.last_recv_time_ns
        final_path = self.path
        if compress and self.stats.records > 0:
            final_path = self.path.with_suffix(self.path.suffix + ".gz")
            tmp = final_path.with_suffix(final_path.suffix + ".tmp")
            with open(self.path, "rb") as src, gzip.open(tmp, "wb", compresslevel=6) as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
                dst.flush()
                os.fsync(dst.fileno())
            # 压缩完成后再校验一次行数，再删未压缩文件
            n = sum(1 for _ in gzip.open(tmp, "rt", encoding="utf-8"))
            if n != self.stats.records:
                tmp.unlink(missing_ok=True)
                log.error("压缩校验失败 %s: %d != %d，保留未压缩文件", self.path, n, self.stats.records)
                final_path = self.path
            else:
                os.replace(tmp, final_path)
                self.path.unlink()
        manifest = self.build_manifest(final_path)
        write_manifest(final_path, manifest)
        return manifest

    def build_manifest(self, final_path: Path) -> dict[str, Any]:
        st = self.stats
        return {
            "path": str(final_path),
            "records": st.records,
            "first_recv_time_ns": st.first_recv_time_ns,
            "last_recv_time_ns": st.last_recv_time_ns,
            "first_id": st.first_id,
            "last_id": st.last_id,
            "schema_version": self.schema_version,
            "size_bytes": final_path.stat().st_size if final_path.exists() else 0,
            "sha256": sha256_file(final_path) if final_path.exists() else None,
            "closed_time_ns": time.time_ns(),
            "note": "sha256 只证明文件未被改动，不证明上游交易所没有漏推",
            **self.meta,
        }


def manifest_path(data_path: Path) -> Path:
    return data_path.with_name(data_path.name + ".manifest.json")


def write_manifest(data_path: Path, manifest: dict[str, Any]) -> None:
    mp = manifest_path(data_path)
    tmp = mp.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, mp)


def verify_manifest(data_path: Path) -> dict[str, Any]:
    mp = manifest_path(data_path)
    result: dict[str, Any] = {"path": str(data_path), "manifest": str(mp)}
    if not mp.exists():
        result["ok"] = False
        result["error"] = "manifest_missing"
        return result
    m = json.loads(mp.read_text(encoding="utf-8"))
    result["sha256_ok"] = sha256_file(data_path) == m.get("sha256")
    n = 0
    bad = 0
    with open_text(data_path) as fh:
        for line in fh:
            try:
                json.loads(line)
                n += 1
            except json.JSONDecodeError:
                bad += 1
    result["records_ok"] = (n == m.get("records"))
    result["records"] = n
    result["bad_lines"] = bad
    result["ok"] = result["sha256_ok"] and result["records_ok"] and bad == 0
    return result


def recover_open_shard(path: Path) -> dict[str, Any]:
    """处理上次崩溃遗留的未关闭 .jsonl：保留完整行，隔离损坏尾部，写 manifest。

    不覆盖旧文件：损坏尾部另存 <name>.tail.corrupt，主文件原地截断到最后一个完整行。
    截断是唯一的"修改"，且被截掉的字节完整保存在 .tail.corrupt 里。
    """
    result: dict[str, Any] = {"path": str(path), "records": 0, "truncated_bytes": 0}
    good_end = 0
    n = 0
    first_id = last_id = None
    first_rt = last_rt = None
    with open(path, "rb") as fh:
        while True:
            pos = fh.tell()
            line = fh.readline()
            if not line:
                break
            if not line.endswith(b"\n"):
                break
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                break
            n += 1
            good_end = pos + len(line)
            iv = rec.get("id")
            if iv is not None:
                first_id = iv if first_id is None else first_id
                last_id = iv
            rt = rec.get("recv_time_ns")
            if rt is not None:
                first_rt = rt if first_rt is None else first_rt
                last_rt = rt
        fh.seek(0, os.SEEK_END)
        total = fh.tell()
    if good_end < total:
        with open(path, "rb") as fh:
            fh.seek(good_end)
            tail = fh.read()
        corrupt = path.with_name(path.name + ".tail.corrupt")
        with open(corrupt, "ab") as out:
            out.write(tail)
            out.flush()
            os.fsync(out.fileno())
        with open(path, "r+b") as fh:
            fh.truncate(good_end)
            fh.flush()
            os.fsync(fh.fileno())
        result["truncated_bytes"] = total - good_end
        result["corrupt_tail"] = str(corrupt)
    result["records"] = n
    manifest = {
        "path": str(path), "records": n, "first_recv_time_ns": first_rt, "last_recv_time_ns": last_rt,
        "first_id": first_id, "last_id": last_id, "schema_version": None,
        "size_bytes": path.stat().st_size, "sha256": sha256_file(path),
        "closed_time_ns": time.time_ns(), "recovered": True,
        "truncated_bytes": result["truncated_bytes"],
        "note": "崩溃恢复：进程异常退出，该分片之后到重启之间的数据不存在（见 gaps downtime）",
    }
    write_manifest(path, manifest)
    result["manifest"] = manifest
    return result


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with open_text(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                log.warning("跳过损坏行 %s", path)


class RawWriter:
    """管理多个 (stream_kind, symbol) 的活动分片。同步 API，由专用写线程/任务调用。"""

    def __init__(self, data_dir: Path, session_id: str, schema_version: int,
                 max_bytes: int, max_seconds: float, flush_interval_s: float,
                 fsync_interval_s: float, compress: bool, extra_meta: dict[str, Any] | None = None):
        self.root = data_dir / "raw"
        self.session_id = session_id
        self.schema_version = schema_version
        self.max_bytes = max_bytes
        self.max_seconds = max_seconds
        self.flush_interval_s = flush_interval_s
        self.fsync_interval_s = fsync_interval_s
        self.compress = compress
        self.extra_meta = extra_meta or {}
        self.shards: dict[tuple[str, str], RawShard] = {}
        self._seq: dict[tuple[str, str], int] = {}
        self.manifests: list[dict[str, Any]] = []
        self.records_written = 0
        self.bytes_written = 0
        self.records_fsynced = 0
        self.inject_error_after: int | None = None
        self._fsynced: dict[str, dict[str, Any]] = {}     # "kind/SYMBOL" -> 已 fsync 的最后位置（跨分片保留）

    def _new_shard(self, kind: str, symbol: str, day: str) -> RawShard:
        key = (kind, symbol)
        seq = self._seq.get(key, 0) + 1
        self._seq[key] = seq
        d = self.root / kind / symbol / day
        name = f"{self.session_id}_{seq:06d}.jsonl"
        shard = RawShard(d / name, self.schema_version,
                         {"stream_kind": kind, "symbol": symbol, "session_id": self.session_id,
                          "utc_day": day, **self.extra_meta})
        return shard

    def write(self, kind: str, symbol: str, rec: dict[str, Any], id_value: int | None = None) -> None:
        if self.inject_error_after is not None and self.records_written >= self.inject_error_after:
            raise OSError(28, "fault injection: simulated ENOSPC")
        key = (kind, symbol)
        day = utc_day(rec["recv_time_ns"])
        shard = self.shards.get(key)
        if shard is not None and (shard.meta["utc_day"] != day
                                  or shard.should_rotate(self.max_bytes, self.max_seconds)):
            self._close(key)
            shard = None
        if shard is None:
            shard = self._new_shard(kind, symbol, day)
            self.shards[key] = shard
        n = shard.write(rec, id_value)
        self.records_written += 1
        self.bytes_written += n

    def maybe_flush(self, force: bool = False) -> None:
        for key, shard in self.shards.items():
            if shard.maybe_flush(self.flush_interval_s, self.fsync_interval_s, force=force):
                self._record_fsynced(key, shard)
        self.records_fsynced = sum(s.stats.records_fsynced for s in self.shards.values()) \
            + sum(m["records"] for m in self.manifests)

    def _record_fsynced(self, key: tuple[str, str], shard: RawShard) -> None:
        st = shard.stats
        if st.records_fsynced > 0:
            self._fsynced[f"{key[0]}/{key[1]}"] = {
                "last_id": st.last_id_fsynced, "last_recv_time_ns": st.last_recv_time_fsynced,
                "records_fsynced_in_shard": st.records_fsynced, "path": str(shard.path)}

    def fsynced_positions(self) -> dict[str, dict[str, Any]]:
        """每个 (kind,symbol) 已 fsync 到磁盘的最后 id / 接收时间。仅用于 checkpoint。"""
        return {k: dict(v) for k, v in self._fsynced.items()}

    def _close(self, key: tuple[str, str]) -> None:
        shard = self.shards.pop(key)
        m = shard.close(self.compress)
        self._record_fsynced(key, shard)
        self.manifests.append(m)
        log.info("关闭分片 %s (%d 行, %d B)", m["path"], m["records"], m["size_bytes"])

    def close_all(self) -> None:
        for key in list(self.shards):
            self._close(key)
        self.records_fsynced = sum(m["records"] for m in self.manifests)

    def active_files(self) -> list[str]:
        return [str(s.path) for s in self.shards.values()]
