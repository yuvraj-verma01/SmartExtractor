from __future__ import annotations

from datetime import datetime
import copy
from typing import List, Optional
from uuid import uuid4

from . import io_utils
from .paths import JOBS_ROOT, JobPaths, job_paths


DEFAULT_PIPELINE = {
    "stage1": {"status": "pending", "message": None},
    "stage2": {"status": "pending", "message": None},
    "stage3": {"status": "pending", "message": None},
}


def ensure_job_dirs(paths: JobPaths) -> None:
    for p in [
        paths.input_dir,
        paths.stage1_dir,
        paths.stage2_dir,
        paths.stage3_dir,
        paths.final_dir,
        paths.export_dir,
        paths.workspace_dir,
    ]:
        p.mkdir(parents=True, exist_ok=True)


def new_job(job_id: Optional[str] = None) -> JobPaths:
    JOBS_ROOT.mkdir(parents=True, exist_ok=True)
    jid = job_id or uuid4().hex
    paths = job_paths(jid)
    ensure_job_dirs(paths)
    meta = {
        "id": jid,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "status": "created",
        "pipeline": copy.deepcopy(DEFAULT_PIPELINE),
        "llm_status": "unknown",
        "last_error": None,
    }
    io_utils.write_json(paths.job_meta_path, meta)
    return paths


def load_job_meta(job_id: str) -> dict:
    paths = job_paths(job_id)
    return io_utils.read_json(paths.job_meta_path, default={}) or {}


def save_job_meta(job_id: str, meta: dict) -> None:
    paths = job_paths(job_id)
    io_utils.write_json(paths.job_meta_path, meta)


def list_jobs() -> List[dict]:
    if not JOBS_ROOT.exists():
        return []
    items: List[dict] = []
    for child in JOBS_ROOT.iterdir():
        if not child.is_dir():
            continue
        meta_path = child / "job_meta.json"
        if not meta_path.exists():
            continue
        meta = io_utils.read_json(meta_path, default={}) or {}
        if meta:
            items.append(meta)
    items.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return items


def job_exists(job_id: str) -> bool:
    return job_paths(job_id).root.exists()
