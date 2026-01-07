from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

from . import io_utils
from .jobs import load_job_meta, save_job_meta
from .ollama import check_ollama
from .paths import REPO_ROOT, JobPaths, job_paths
from .state import init_working_state, merge_working_state

LLM_MODEL = "qwen2.5:7b-instruct-q4_K_M"


def _run_script(
    script_path: Path,
    cwd: Path,
    args: Optional[list] = None,
    env_overrides: Optional[dict] = None,
) -> Tuple[str, str]:
    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)
    env = os.environ.copy()
    repo_root = str(REPO_ROOT)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = repo_root if not existing else f"{repo_root}{os.pathsep}{existing}"
    if env_overrides:
        env.update({k: str(v) for k, v in env_overrides.items() if v is not None})
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stdout or "") + "\n" + (proc.stderr or ""))
    return proc.stdout, proc.stderr


def _copy_files(src_dir: Path, dest_dir: Path, filenames: Optional[list] = None) -> None:
    if not src_dir.exists():
        return
    dest_dir.mkdir(parents=True, exist_ok=True)
    if filenames is None:
        for src in src_dir.iterdir():
            if src.is_file():
                shutil.copy2(src, dest_dir / src.name)
        return

    for name in filenames:
        src = src_dir / name
        if src.exists():
            shutil.copy2(src, dest_dir / name)


def _reset_workspace(paths: JobPaths) -> None:
    if paths.workspace_dir.exists():
        shutil.rmtree(paths.workspace_dir)
    paths.workspace_dir.mkdir(parents=True, exist_ok=True)


def _reset_stage_dirs(paths: JobPaths) -> None:
    for target in [paths.stage1_dir, paths.stage2_dir, paths.stage3_dir]:
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)


def _reset_review_artifacts(paths: JobPaths) -> None:
    if paths.audit_log_path.exists():
        paths.audit_log_path.unlink()
    for target in [paths.final_dir, paths.export_dir]:
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)


def _ensure_workspace_ocr(paths: JobPaths) -> Optional[Path]:
    if paths.workspace_ocr.exists():
        return paths.workspace_ocr
    if not paths.stage1_dir.exists():
        return None
    candidates = [p for p in paths.stage1_dir.iterdir() if p.is_file() and p.suffix.lower() == ".txt"]
    if not candidates:
        return None
    paths.workspace_ocr.mkdir(parents=True, exist_ok=True)
    src = candidates[0]
    dest = paths.workspace_ocr / src.name
    shutil.copy2(src, dest)
    return paths.workspace_ocr


def _ensure_workspace_outputs_from_stage2(paths: JobPaths) -> bool:
    outputs_dir = paths.workspace_outputs
    outputs_dir.mkdir(parents=True, exist_ok=True)
    required = ["lease_validated.json", "review_queue.json"]
    ok = True
    for name in required:
        src = paths.stage2_dir / name
        if not src.exists():
            ok = False
            continue
        shutil.copy2(src, outputs_dir / name)
    anchors_src = paths.stage2_dir / "lease_anchors.json"
    if anchors_src.exists():
        shutil.copy2(anchors_src, outputs_dir / "lease_anchors.json")
    return ok


def _ensure_anchor_bundle(paths: JobPaths) -> Optional[str]:
    outputs_dir = paths.workspace_outputs
    outputs_dir.mkdir(parents=True, exist_ok=True)
    target = outputs_dir / "lease_anchors.json"
    if target.exists():
        return None
    candidates = list(outputs_dir.glob("*_anchors.json"))
    if len(candidates) == 1:
        shutil.copy2(candidates[0], target)
        return f"anchors_copied_from_{candidates[0].name}"
    io_utils.write_json(target, {})
    return "anchors_missing_created_empty"


def _update_stage_status(meta: dict, stage: str, status: str, message: Optional[str] = None) -> None:
    meta.setdefault("pipeline", {})
    meta["pipeline"][stage] = {"status": status, "message": message}


def _update_meta(job_id: str, updates: dict) -> dict:
    meta = load_job_meta(job_id)
    meta.update(updates)
    save_job_meta(job_id, meta)
    return meta


def _load_stage2_state(paths: JobPaths) -> Tuple[dict, dict, list]:
    validated_path = paths.stage2_dir / "lease_validated.json"
    validated = io_utils.read_json(validated_path, default={}) or {}
    row = validated.get("row") or {}
    confidence = validated.get("confidence") or {}
    derived = validated.get("derived_suggestions") or []
    return row, confidence, derived


def _load_llm_state(paths: JobPaths) -> dict:
    llm_path = paths.stage3_dir / "lease_llm_suggestions.json"
    llm = io_utils.read_json(llm_path, default={}) or {}
    if not isinstance(llm, dict):
        return {}
    return llm


def run_pipeline(job_id: str) -> dict:
    paths = job_paths(job_id)
    if not paths.root.exists():
        raise FileNotFoundError(f"Job {job_id} not found")

    try:
        meta = load_job_meta(job_id)
        meta["status"] = "running"
        meta["last_error"] = None
        meta.setdefault("pipeline", {})
        for stage in ["stage1", "stage2", "stage3"]:
            meta["pipeline"].setdefault(stage, {"status": "pending", "message": None})
        save_job_meta(job_id, meta)

        _reset_workspace(paths)
        _reset_stage_dirs(paths)
        _reset_review_artifacts(paths)
        paths.workspace_inputs.mkdir(parents=True, exist_ok=True)

        stage1_status = "skipped_input_missing"
        if paths.input_pdf.exists():
            meta["pipeline"]["stage1"] = {"status": "running", "message": None}
            save_job_meta(job_id, meta)
            shutil.copy2(paths.input_pdf, paths.workspace_inputs / "lease.pdf")
            try:
                _run_script(REPO_ROOT / "main.py", cwd=paths.workspace_dir)
                _copy_files(paths.workspace_ocr, paths.stage1_dir)
                stage1_status = "success"
            except Exception as exc:
                stage1_status = "error"
                meta["pipeline"]["stage1"] = {"status": "error", "message": str(exc)}
                meta["status"] = "error"
                meta["last_error"] = "stage1_failed"
                save_job_meta(job_id, meta)
                init_working_state(paths, llm_status=meta.get("llm_status", "unknown"))
                return meta

        message = None if stage1_status != "skipped_input_missing" else "input_pdf_missing"
        meta["pipeline"]["stage1"] = {"status": stage1_status, "message": message}
        save_job_meta(job_id, meta)

        stage2_status = "skipped_no_ocr"
        stage2_warning = None
        if stage1_status == "success":
            meta["pipeline"]["stage2"] = {"status": "running", "message": None}
            save_job_meta(job_id, meta)
            try:
                _ensure_workspace_ocr(paths)
                _run_script(REPO_ROOT / "extract" / "anchors.py", cwd=paths.workspace_dir)
                stage2_warning = _ensure_anchor_bundle(paths)
                _run_script(REPO_ROOT / "extract" / "parse_fields.py", cwd=paths.workspace_dir)
                _run_script(REPO_ROOT / "extract" / "validate_and_fill.py", cwd=paths.workspace_dir)
                _copy_files(
                    paths.workspace_outputs,
                    paths.stage2_dir,
                    [
                        "lease_anchors.json",
                        "lease_extracted.json",
                        "lease_validated.json",
                        "review_queue.json",
                    ],
                )
                stage2_status = "success"
            except Exception as exc:
                stage2_status = "error"
                meta["pipeline"]["stage2"] = {"status": "error", "message": str(exc)}
                meta["status"] = "error"
                meta["last_error"] = "stage2_failed"
                save_job_meta(job_id, meta)
                init_working_state(paths, llm_status=meta.get("llm_status", "unknown"))
                return meta

        if stage2_status == "skipped_no_ocr":
            message = "missing_ocr_output"
        elif stage2_warning:
            message = stage2_warning
        else:
            message = None
        meta["pipeline"]["stage2"] = {"status": stage2_status, "message": message}
        save_job_meta(job_id, meta)

        llm_status = "not_attempted"
        stage3_status = "skipped_no_stage2"
        if stage2_status == "success":
            llm_model = meta.get("llm_model") or LLM_MODEL
            available, reason = check_ollama(llm_model)
            if not available:
                llm_status = "unavailable"
                stage3_status = f"skipped_{reason}"
            else:
                try:
                    meta["pipeline"]["stage3"] = {"status": "running", "message": None}
                    save_job_meta(job_id, meta)
                    _run_script(
                        REPO_ROOT / "extract" / "llm_fallback.py",
                        cwd=paths.workspace_dir,
                        env_overrides={"OLLAMA_MODEL": llm_model},
                    )
                    _copy_files(
                        paths.workspace_outputs,
                        paths.stage3_dir,
                        ["lease_llm_suggestions.json"],
                    )
                    llm_status = "ran"
                    stage3_status = "success"
                except Exception as exc:
                    llm_status = "error"
                    stage3_status = "error"
                    meta["pipeline"]["stage3"] = {"status": "error", "message": str(exc)}
                    meta["status"] = "error"
                    meta["last_error"] = "stage3_failed"
                    save_job_meta(job_id, meta)
                    row, confidence, derived = _load_stage2_state(paths)
                    init_working_state(
                        paths,
                        row=row,
                        confidence=confidence,
                        derived_suggestions=derived,
                        llm_suggestions=_load_llm_state(paths),
                        llm_status=llm_status,
                    )
                    return meta

        if stage2_status != "success" and llm_status == "not_attempted":
            llm_status = "skipped"
        meta["pipeline"]["stage3"] = {"status": stage3_status, "message": None}
        meta["llm_status"] = llm_status
        if stage2_status == "success":
            meta["status"] = "ready_for_review"
        elif stage1_status == "skipped_input_missing":
            meta["status"] = "needs_input"
        else:
            meta["status"] = meta.get("status")
        save_job_meta(job_id, meta)

        row, confidence, derived = _load_stage2_state(paths)
        init_working_state(
            paths,
            row=row,
            confidence=confidence,
            derived_suggestions=derived,
            llm_suggestions=_load_llm_state(paths),
            llm_status=llm_status,
        )
        return meta
    except Exception as exc:
        meta = load_job_meta(job_id)
        meta["status"] = "error"
        meta["last_error"] = "pipeline_crash"
        pipeline = meta.setdefault("pipeline", {})
        current_stage = None
        for stage in ["stage3", "stage2", "stage1"]:
            if pipeline.get(stage, {}).get("status") == "running":
                current_stage = stage
                break
        if current_stage is None:
            current_stage = "stage1"
        pipeline[current_stage] = {"status": "error", "message": str(exc)}
        save_job_meta(job_id, meta)
        return meta


def run_stage1(job_id: str) -> dict:
    paths = job_paths(job_id)
    if not paths.root.exists():
        raise FileNotFoundError(f"Job {job_id} not found")

    meta = load_job_meta(job_id)
    meta["status"] = "running"
    meta["last_error"] = None
    _update_stage_status(meta, "stage1", "running")
    _update_stage_status(meta, "stage2", "pending")
    _update_stage_status(meta, "stage3", "pending")
    save_job_meta(job_id, meta)

    _reset_workspace(paths)
    _reset_stage_dirs(paths)
    _reset_review_artifacts(paths)
    paths.workspace_inputs.mkdir(parents=True, exist_ok=True)

    if not paths.input_pdf.exists():
        _update_stage_status(meta, "stage1", "skipped_input_missing", "input_pdf_missing")
        meta["status"] = "needs_input"
        save_job_meta(job_id, meta)
        init_working_state(paths, llm_status=meta.get("llm_status", "unknown"))
        return meta

    shutil.copy2(paths.input_pdf, paths.workspace_inputs / "lease.pdf")
    try:
        _run_script(REPO_ROOT / "main.py", cwd=paths.workspace_dir)
        _copy_files(paths.workspace_ocr, paths.stage1_dir)
        _update_stage_status(meta, "stage1", "success")
        meta["status"] = "stage1_complete"
        save_job_meta(job_id, meta)
        return meta
    except Exception as exc:
        _update_stage_status(meta, "stage1", "error", str(exc))
        meta["status"] = "error"
        meta["last_error"] = "stage1_failed"
        save_job_meta(job_id, meta)
        init_working_state(paths, llm_status=meta.get("llm_status", "unknown"))
        return meta


def run_stage2(job_id: str) -> dict:
    paths = job_paths(job_id)
    if not paths.root.exists():
        raise FileNotFoundError(f"Job {job_id} not found")

    meta = load_job_meta(job_id)
    meta["status"] = "running"
    meta["last_error"] = None
    _update_stage_status(meta, "stage2", "running")
    _update_stage_status(meta, "stage3", "pending")
    save_job_meta(job_id, meta)

    _reset_workspace(paths)
    if paths.stage2_dir.exists():
        shutil.rmtree(paths.stage2_dir)
    paths.stage2_dir.mkdir(parents=True, exist_ok=True)
    if paths.stage3_dir.exists():
        shutil.rmtree(paths.stage3_dir)
    paths.stage3_dir.mkdir(parents=True, exist_ok=True)

    if not _ensure_workspace_ocr(paths):
        _update_stage_status(meta, "stage2", "skipped_no_ocr", "missing_ocr_output")
        meta["status"] = "error"
        meta["last_error"] = "stage2_failed"
        save_job_meta(job_id, meta)
        return meta

    try:
        _run_script(REPO_ROOT / "extract" / "anchors.py", cwd=paths.workspace_dir)
        warning = _ensure_anchor_bundle(paths)
        _run_script(REPO_ROOT / "extract" / "parse_fields.py", cwd=paths.workspace_dir)
        _run_script(REPO_ROOT / "extract" / "validate_and_fill.py", cwd=paths.workspace_dir)
        _copy_files(
            paths.workspace_outputs,
            paths.stage2_dir,
            [
                "lease_anchors.json",
                "lease_extracted.json",
                "lease_validated.json",
                "review_queue.json",
            ],
        )
        message = warning
        _update_stage_status(meta, "stage2", "success", message)
        meta["status"] = "ready_for_review"
        save_job_meta(job_id, meta)

        row, confidence, derived = _load_stage2_state(paths)
        merge_working_state(
            paths,
            row=row,
            confidence=confidence,
            derived_suggestions=derived,
            llm_suggestions=_load_llm_state(paths),
            llm_status=meta.get("llm_status"),
            preserve_review=True,
        )
        return meta
    except Exception as exc:
        _update_stage_status(meta, "stage2", "error", str(exc))
        meta["status"] = "error"
        meta["last_error"] = "stage2_failed"
        save_job_meta(job_id, meta)
        init_working_state(paths, llm_status=meta.get("llm_status", "unknown"))
        return meta


def run_stage3(job_id: str, llm_model: Optional[str] = None) -> dict:
    paths = job_paths(job_id)
    if not paths.root.exists():
        raise FileNotFoundError(f"Job {job_id} not found")

    meta = load_job_meta(job_id)
    meta["status"] = "running"
    meta["last_error"] = None
    if llm_model:
        meta["llm_model"] = llm_model
    _update_stage_status(meta, "stage3", "running")
    save_job_meta(job_id, meta)

    _reset_workspace(paths)
    if paths.stage3_dir.exists():
        shutil.rmtree(paths.stage3_dir)
    paths.stage3_dir.mkdir(parents=True, exist_ok=True)

    if not _ensure_workspace_outputs_from_stage2(paths):
        _update_stage_status(meta, "stage3", "skipped_no_stage2", "missing_stage2_outputs")
        meta["status"] = "error"
        meta["last_error"] = "stage3_failed"
        save_job_meta(job_id, meta)
        return meta

    model_to_use = llm_model or meta.get("llm_model") or LLM_MODEL
    available, reason = check_ollama(model_to_use)
    if not available:
        _update_stage_status(meta, "stage3", f"skipped_{reason}", None)
        meta["llm_status"] = "unavailable"
        meta["status"] = "ready_for_review"
        save_job_meta(job_id, meta)
        row, confidence, derived = _load_stage2_state(paths)
        merge_working_state(
            paths,
            row=row,
            confidence=confidence,
            derived_suggestions=derived,
            llm_suggestions=_load_llm_state(paths),
            llm_status=meta.get("llm_status"),
            preserve_review=True,
        )
        return meta

    try:
        _run_script(
            REPO_ROOT / "extract" / "llm_fallback.py",
            cwd=paths.workspace_dir,
            env_overrides={"OLLAMA_MODEL": model_to_use},
        )
        _copy_files(paths.workspace_outputs, paths.stage3_dir, ["lease_llm_suggestions.json"])
        _update_stage_status(meta, "stage3", "success")
        meta["llm_status"] = "ran"
        meta["status"] = "ready_for_review"
        save_job_meta(job_id, meta)
        row, confidence, derived = _load_stage2_state(paths)
        merge_working_state(
            paths,
            row=row,
            confidence=confidence,
            derived_suggestions=derived,
            llm_suggestions=_load_llm_state(paths),
            llm_status=meta.get("llm_status"),
            preserve_review=True,
        )
        return meta
    except Exception as exc:
        _update_stage_status(meta, "stage3", "error", str(exc))
        meta["llm_status"] = "error"
        meta["status"] = "error"
        meta["last_error"] = "stage3_failed"
        save_job_meta(job_id, meta)
        return meta
