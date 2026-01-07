from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from . import io_utils
from .schema import FIELDS, SCHEMA_VERSION


def _index_suggestions(suggestions: Any) -> Dict[str, list]:
    indexed: Dict[str, list] = {}
    if not isinstance(suggestions, list):
        return indexed
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        field = item.get("field")
        if not field:
            continue
        indexed.setdefault(field, []).append(item)
    return indexed


def init_working_state(
    paths,
    row: Optional[dict] = None,
    confidence: Optional[dict] = None,
    derived_suggestions: Optional[list] = None,
    llm_suggestions: Optional[dict] = None,
    llm_status: Optional[str] = None,
) -> dict:
    row = row or {}
    confidence = confidence or {}
    derived_by_field = _index_suggestions(derived_suggestions or [])
    fields: Dict[str, dict] = {}
    for field in FIELDS:
        fields[field] = {
            "value": row.get(field),
            "confidence": confidence.get(field),
            "review": {
                "status": "unreviewed",
                "action": None,
                "reviewed_at": None,
            },
            "suggestions": {
                "derived": derived_by_field.get(field, []),
                "llm": (llm_suggestions or {}).get(field),
            },
        }

    state = {
        "schema_version": SCHEMA_VERSION,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "llm_status": llm_status or "unknown",
        "fields": fields,
    }
    io_utils.write_json(paths.working_state_path, state)
    return state


def load_working_state(paths) -> dict:
    state = io_utils.read_json(paths.working_state_path, default=None)
    if isinstance(state, dict) and "fields" in state:
        if "updated_at" not in state:
            state["updated_at"] = state.get("created_at") or datetime.utcnow().isoformat() + "Z"
            io_utils.write_json(paths.working_state_path, state)
        return state
    return init_working_state(paths)


def save_working_state(paths, state: dict) -> None:
    state["updated_at"] = datetime.utcnow().isoformat() + "Z"
    io_utils.write_json(paths.working_state_path, state)


def merge_working_state(
    paths,
    row: Optional[dict] = None,
    confidence: Optional[dict] = None,
    derived_suggestions: Optional[list] = None,
    llm_suggestions: Optional[dict] = None,
    llm_status: Optional[str] = None,
    preserve_review: bool = True,
) -> dict:
    row = row or {}
    confidence = confidence or {}
    derived_by_field = _index_suggestions(derived_suggestions or [])

    state = load_working_state(paths)
    fields = state.get("fields", {}) or {}

    for field in FIELDS:
        entry = fields.get(field)
        if entry is None:
            entry = {
                "value": row.get(field),
                "confidence": confidence.get(field),
                "review": {
                    "status": "unreviewed",
                    "action": None,
                    "reviewed_at": None,
                },
                "suggestions": {
                    "derived": derived_by_field.get(field, []),
                    "llm": (llm_suggestions or {}).get(field),
                },
            }
            fields[field] = entry
            continue

        reviewed = entry.get("review", {}).get("status") == "reviewed"
        if not preserve_review or not reviewed:
            entry["value"] = row.get(field)
            if not preserve_review:
                entry["review"] = {
                    "status": "unreviewed",
                    "action": None,
                    "reviewed_at": None,
                }
        entry["confidence"] = confidence.get(field)
        entry.setdefault("suggestions", {})
        entry["suggestions"]["derived"] = derived_by_field.get(field, [])
        entry["suggestions"]["llm"] = (llm_suggestions or {}).get(field)

    state["fields"] = fields
    if llm_status is not None:
        state["llm_status"] = llm_status
    save_working_state(paths, state)
    return state
