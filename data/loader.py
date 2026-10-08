"""研究层读取入口：把 normalized 下的小 Parquet 文件按 symbol + 日期范围合并成一张表。

  load(table, symbol, start_day, end_day, data_dir=...) -> pyarrow.Table

处理：
- 按 UTC 日期分区目录扫描（分区键见 storage/normalized.py：book/mark 用 recv_time_ns，
  trades 用 known_time_ns，所以 backfill 的成交可能落在比交易所时间晚的分区）；
- 跨会话/重连去重（book: (symbol,u,book_epoch)，trades: (symbol,a)，mark: (symbol,E_ms)），
  保留最早收到的一条；
- 按交易所时间 + recv_seq 排序；
- 只取 book_top20 的 is_valid 行（可关）。
价格/数量仍是字符串；需要数值时用 to_float()。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "store"

# 表 -> (去重键, 排序键, 同键保留哪一条)。gaps 一条记录会随修复进度写多个版本，保留最新。
_TABLE_KEYS: dict[str, tuple[list[str], list[str], str]] = {
    "book_top20": (["symbol", "book_epoch", "u"], ["T_ms", "u", "recv_seq"], "first"),
    "agg_trades": (["symbol", "a"], ["T_ms", "a"], "first"),
    "mark_price": (["symbol", "E_ms"], ["E_ms", "recv_seq"], "first"),
    "gaps": (["gap_id"], ["detected_time_ns", "update_time_ns"], "last"),
    "quality_events": ([], ["time_ns"], "first"),
}


def _to_day(d: str | date | datetime) -> date:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return datetime.strptime(d.replace("-", ""), "%Y%m%d").date()


def days_between(start: str | date, end: str | date | None = None) -> list[str]:
    """闭区间 [start, end] 的 YYYYMMDD 列表；end 省略则只取 start。"""
    s, e = _to_day(start), _to_day(end or start)
    if e < s:
        raise ValueError(f"end {e} < start {s}")
    return [(s + timedelta(i)).strftime("%Y%m%d") for i in range((e - s).days + 1)]


def list_files(table: str, symbol: str, days: Iterable[str],
               data_dir: Path = DEFAULT_DATA_DIR) -> list[Path]:
    root = data_dir / "normalized" / table / symbol
    out: list[Path] = []
    for day in days:
        out.extend(sorted((root / day).glob("*.parquet")))
    return out


def available_days(table: str, symbol: str, data_dir: Path = DEFAULT_DATA_DIR) -> list[str]:
    root = data_dir / "normalized" / table / symbol
    if not root.is_dir():
        return []
    return sorted(p.name for p in root.iterdir() if p.is_dir() and p.name.isdigit())


def _dedup(tbl: pa.Table, keys: list[str], order: list[str], keep: str = "first") -> pa.Table:
    """按 order 排序后对 keys 去重，每组保留第一/最后一行，结果仍按 order 有序。"""
    if tbl.num_rows == 0 or not keys:
        return tbl
    tbl = tbl.sort_by([(c, "ascending") for c in order])
    idx = pa.array(range(tbl.num_rows), pa.int64())
    tbl = tbl.append_column("__row", idx)
    agg = "min" if keep == "first" else "max"
    firsts = tbl.group_by(keys).aggregate([("__row", agg)]).column(f"__row_{agg}")
    keep = firsts.take(pc.sort_indices(firsts))
    return tbl.take(keep).drop_columns(["__row"])


def load(table: str, symbol: str, start_day: str | date, end_day: str | date | None = None,
         *, data_dir: Path = DEFAULT_DATA_DIR, columns: list[str] | None = None,
         valid_only: bool = True) -> pa.Table:
    """读取一张 normalized 表；无文件时返回空表（带 schema，若能从任一文件推断）。"""
    if table not in _TABLE_KEYS:
        raise ValueError(f"未知表 {table}，可选 {sorted(_TABLE_KEYS)}")
    keys, order, keep = _TABLE_KEYS[table]
    files = list_files(table, symbol, days_between(start_day, end_day), data_dir)
    if not files:
        return pa.table({})
    need = None
    if columns is not None:
        need = list(dict.fromkeys([*columns, *keys, *order, *(["is_valid"] if table == "book_top20" else [])]))
    tbl = pa.concat_tables([pq.read_table(f, columns=need) for f in files], promote_options="default")
    if table == "book_top20" and valid_only:
        tbl = tbl.filter(pc.equal(tbl["is_valid"], True))
    tbl = _dedup(tbl, keys, order, keep)
    if columns is not None:
        tbl = tbl.select(columns)
    return tbl


def to_float(tbl: pa.Table, columns: list[str] | None = None) -> pa.Table:
    """把字符串价格/数量列转成 float64。columns 省略时转所有 *_px_*/*_qty_* 及 p/q/nq/ap/i/P/r。"""
    if columns is None:
        columns = [c for c in tbl.column_names
                   if "_px_" in c or "_qty_" in c or c in ("p", "q", "nq", "ap", "i", "P", "r")]
    for c in columns:
        if c in tbl.column_names and pa.types.is_string(tbl.schema.field(c).type):
            i = tbl.schema.get_field_index(c)
            tbl = tbl.set_column(i, c, pc.cast(tbl[c], pa.float64()))
    return tbl


def summary(tbl: pa.Table, time_col: str = "T_ms", gap_ms: int = 5_000) -> dict:
    """行数、时间跨度、最大相邻间隔、超过 gap_ms 的间隔数。"""
    n = tbl.num_rows
    if n == 0 or time_col not in tbl.column_names:
        return {"rows": n}
    t = tbl[time_col]
    tmin, tmax = pc.min(t).as_py(), pc.max(t).as_py()
    out = {"rows": n, "start": _ms_iso(tmin), "end": _ms_iso(tmax), "span_s": (tmax - tmin) / 1e3}
    if n > 1:
        d = pc.subtract(t.slice(1), t.slice(0, n - 1))
        out["max_gap_ms"] = pc.max(d).as_py()
        out["n_gaps_over"] = pc.sum(pc.greater(d, gap_ms)).as_py()
        out["gap_threshold_ms"] = gap_ms
    return out


def segments(tbl: pa.Table, time_col: str = "T_ms", max_gap_ms: int = 5_000) -> list[tuple[int, int]]:
    """把表按相邻间隔 > max_gap_ms 切成连续段，返回 [(start_idx, end_idx_exclusive), ...]。

    研究时特征/标签只在段内计算，避免跨缺口（重启、断线）拼接。"""
    n = tbl.num_rows
    if n == 0:
        return []
    t = tbl[time_col]
    d = pc.subtract(t.slice(1), t.slice(0, n - 1))
    breaks = pc.indices_nonzero(pc.greater(d, max_gap_ms)).to_pylist()
    bounds = [0, *(b + 1 for b in breaks), n]
    return list(zip(bounds[:-1], bounds[1:]))


def _ms_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1e3, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"
