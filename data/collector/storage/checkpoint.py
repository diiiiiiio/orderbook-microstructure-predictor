"""checkpoint：只记录已 fsync 的进度。原子写（tmp + rename）。"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any


class Checkpoint:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            bad = self.path.with_suffix(".corrupt." + str(int(time.time())))
            os.replace(self.path, bad)
            return {}

    def commit(self, updates: dict[str, Any]) -> None:
        self.data.update(updates)
        self.data["committed_time_ns"] = time.time_ns()
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self.data, fh, ensure_ascii=False, indent=1, default=str)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)
