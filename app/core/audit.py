from __future__ import annotations

from datetime import datetime
from typing import List

from . import io_utils


def append_action(paths, entry: dict) -> None:
    item = dict(entry)
    item.setdefault("ts", datetime.utcnow().isoformat() + "Z")
    io_utils.append_jsonl(paths.audit_log_path, item)


def read_audit_log(paths) -> List[dict]:
    return io_utils.read_jsonl(paths.audit_log_path)
