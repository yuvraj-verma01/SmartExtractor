# extract/validate_and_fill.py
"""
Stage 2: Validate + confidence + review-queue generator (LOCAL, deterministic)
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as dateparser

# --- constraints layer ---
from extract.constraints import (
    apply_suggestions_if_missing,
    find_conflicts,
    suggest_dates,
    suggest_numeric,
)

# -----------------------------
# Config
# -----------------------------
OUT_DIR = Path("data/outputs")

FIELDS = [
    "city","building_name","floors_units",
    "lease_start_date","lease_end_date","rent_start_date","handover_date",
    "lease_tenure_months","lock_in_period","lock_in_end_date",
    "rent_free_period_months","termination_notice_period_months",
    "renewal_notice_period_months",
    "carpet_area_sqft","super_builtup_area_sqft","efficiency","cam_area_sqft",
    "parking_4w_included","parking_2w_included",
    "monthly_cam_rs","monthly_rent_rs","rate_per_sqft_rs",
    "parking_charges_rs","renewal_option","stamp_duty_rs","ifrsd_rs",
]

CONF_OK = 0.75
CONF_REVIEW = 0.55

MAX_EVIDENCE_SNIPPETS = 10
MAX_SNIPPET_CHARS = 1050

# -----------------------------
# Utilities
# -----------------------------
def read_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))

def write_json(p: Path, obj: dict) -> None:
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def parse_date_iso(s: Any) -> Optional[Tuple[int, int, int]]:
    if not isinstance(s, str):
        return None
    try:
        dt = dateparser.parse(s, dayfirst=True, fuzzy=True)
        return (dt.year, dt.month, dt.day) if dt else None
    except Exception:
        return None

def date_to_ordinal(d: Tuple[int,int,int]) -> int:
    y,m,dd = d
    return y*372 + m*31 + dd

def is_number(x: Any) -> bool:
    return isinstance(x,(int,float)) and not math.isnan(x)

def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))

def flatten(s: str) -> str:
    return " ".join((s or "").split())

# -----------------------------
# Evidence ranking
# -----------------------------
DATE_RE = re.compile(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}")
MONEY_RE = re.compile(r"(₹|rs\.?|inr)\s*[0-9,]+", re.I)
NUM_RE = re.compile(r"[0-9][0-9,]*(?:\.\d+)?")
CLAUSE_NO_RE = re.compile(r"^\s*\d+[\.\)]")

DATE_FIELDS = {"lease_start_date","lease_end_date","rent_start_date","handover_date","lock_in_end_date"}
AREA_FIELDS = {"carpet_area_sqft","super_builtup_area_sqft","cam_area_sqft"}
MONEY_FIELDS = {"monthly_rent_rs","monthly_cam_rs","parking_charges_rs","stamp_duty_rs","ifrsd_rs"}

def score_snippet(field: str, snippet: str) -> float:
    s = flatten(snippet).lower()
    score = 0.0
    if field in DATE_FIELDS:
        score += 3 if DATE_RE.search(s) else -1
    if field in MONEY_FIELDS:
        score += 5 if MONEY_RE.search(s) else -2
        if CLAUSE_NO_RE.search(s) and not MONEY_RE.search(s):
            score -= 6
    if NUM_RE.search(s):
        score += 1
    if len(s) < 30:
        score -= 1
    return score

def collect_top_evidence(anchor_bundle, extracted_evidence, field):
    cands = []
    for a in anchor_bundle.get(field,[]):
        txt = a.get("snippet","")[:MAX_SNIPPET_CHARS]
        cands.append({
            "source_field":field,
            "page":a.get("page"),
            "line_no":a.get("line_no"),
            "text":txt,
            "score":score_snippet(field,txt)
        })
    cands.sort(key=lambda x:x["score"], reverse=True)
    return {"snippets":cands[:MAX_EVIDENCE_SNIPPETS]}

# -----------------------------
# Confidence logic
# -----------------------------
@dataclass
class RuleResult:
    ok: bool
    penalty: float
    reason: str

def base_conf_from_evidence(extracted_evidence, field):
    return 0.72 if extracted_evidence.get(field,{}).get("evidence") else 0.35

def compute_confidence(row, extracted_evidence, applied, conflicts):
    confidences = {}
    for f in FIELDS:
        base = base_conf_from_evidence(extracted_evidence,f)
        if row.get(f) in (None,""):
            confidences[f] = clamp01(min(base,0.4))
            continue

        # STEP 3A: derived autofill cap
        if any(a["field"]==f for a in applied):
            base = min(base,0.65)

        # STEP 3B: conflict hard penalty
        if any(c["field"]==f for c in conflicts):
            base = min(base,0.40)

        confidences[f] = clamp01(base)
    return confidences, []

# -----------------------------
# Review queue
# -----------------------------
def build_review_queue(row, confidences, anchor_bundle, extracted_evidence, conflicts):
    items=[]
    conflict_fields={c["field"] for c in conflicts}

    for f in FIELDS:
        val=row.get(f)
        conf=confidences.get(f,0)

        if f in conflict_fields:
            reason="conflict_with_derived"
        elif val in (None,""):
            reason="missing"
        elif conf<CONF_REVIEW:
            reason="low_confidence"
        else:
            reason="ok"

        items.append({
            "field":f,
            "current_value":val,
            "confidence":conf,
            "reason":reason,
            "evidence":collect_top_evidence(anchor_bundle,extracted_evidence,f)
        })

    # STEP 2: conflicts first
    priority={"conflict_with_derived":0,"missing":1,"low_confidence":2,"ok":3}
    items.sort(key=lambda x:(priority[x["reason"]],x["confidence"]))
    return {"items":items}

# -----------------------------
# Entry point
# -----------------------------
def main():
    extracted=read_json(OUT_DIR/"lease_extracted.json")
    anchors=read_json(OUT_DIR/"lease_anchors.json")

    row=extracted.get("row",{})
    extracted_evidence=extracted.get("evidence",{})

    # STEP 1
    suggestions=suggest_dates(row)+suggest_numeric(row)
    applied=apply_suggestions_if_missing(row,suggestions)
    conflicts=find_conflicts(row,suggestions)

    # STEP 3
    confidences,_=compute_confidence(row,extracted_evidence,applied,conflicts)

    validated={
        "row":row,
        "confidence":confidences,
        "derived_suggestions":[s.__dict__ for s in suggestions],
        "applied_suggestions":applied,
        "derived_conflicts":conflicts,
    }
    write_json(OUT_DIR/"lease_validated.json",validated)

    review=build_review_queue(row,confidences,anchors,extracted_evidence,conflicts)
    write_json(OUT_DIR/"review_queue.json",review)

    print("validate_and_fill complete")

if __name__=="__main__":
    main()
