# extract/evidence_ranker.py
from __future__ import annotations
import re
from typing import Any, Dict, List

TOP_K_EVIDENCE = 6

DATE_RE = re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|\b\d{1,2}\s+[A-Za-z]{3,}\s+\d{2,4}\b")
MONEY_RE = re.compile(r"(₹|rs\.?|inr)\s*[0-9][0-9,]*(?:\.\d+)?", re.IGNORECASE)
SQFT_RE = re.compile(r"(sq\.?\s*ft|sqft|sq\s*feet|square\s*feet)", re.IGNORECASE)
NUM_RE  = re.compile(r"[0-9][0-9,]*(?:\.\d+)?")
CLAUSE_NO_RE = re.compile(r"^\s*\d+\s*[\.\)]\s+")  # e.g. "6." "10)" at line start

DATE_FIELDS = {"lease_start_date","lease_end_date","rent_start_date","handover_date","lock_in_end_date"}
AREA_FIELDS = {"carpet_area_sqft","super_builtup_area_sqft","cam_area_sqft"}
MONEY_FIELDS = {"monthly_rent_rs","monthly_cam_rs","parking_charges_rs","stamp_duty_rs","ifrsd_rs"}
INT_FIELDS = {"lease_tenure_months","lock_in_period","rent_free_period_months","termination_notice_period_months",
              "renewal_notice_period_months","parking_4w_included","parking_2w_included"}

DEF_PATTERNS = [
    re.compile(r"carpet\s*area\s*=\s*chargeable\s*area\s*(×|x)\s*0\.75", re.IGNORECASE),
    re.compile(r"chargeable\s*area\s*=\s*carpet\s*area\s*/\s*0\.75", re.IGNORECASE),
    re.compile(r"efficienc(y|ies)\s*[:=]\s*0\.\d+", re.IGNORECASE),
]

ANTI_FOR_AREA = [
    re.compile(r"per\s+square\s+feet\s+per\s+month", re.IGNORECASE),
    re.compile(r"per\s+sq\.?\s*ft\s+per\s+month", re.IGNORECASE),
]

def flatten(text: str) -> str:
    return " ".join((text or "").split())

def score_snippet(field: str, snippet: str, kw_map: Dict[str, List[str]]) -> float:
    s = flatten(snippet)
    low = s.lower()
    score = 0.0

    kws = kw_map.get(field, [])
    for k in kws:
        kl = k.lower()
        if kl in low:
            score += 2.0
            score += 0.15 * low.count(kl)

    # Type/unit compatibility
    if field in DATE_FIELDS:
        score += 3.0 if DATE_RE.search(s) else -1.0

    if field in MONEY_FIELDS:
        # BIG: money snippets must contain Rs/₹/INR, otherwise punish.
        if MONEY_RE.search(s):
            score += 5.0
        else:
            score -= 2.0

        # If line starts like "6." and has no currency, it's likely clause numbering: punish hard.
        if CLAUSE_NO_RE.search(s) and not MONEY_RE.search(s):
            score -= 6.0

    if field in AREA_FIELDS:
        if SQFT_RE.search(s):
            score += 3.0
        elif NUM_RE.search(s):
            score += 0.3
        else:
            score -= 1.0
        if any(p.search(s) for p in ANTI_FOR_AREA):
            score -= 3.0

    if field in INT_FIELDS:
        score += 1.5 if NUM_RE.search(s) else -0.5

    # Definition boosts
    if any(p.search(s) for p in DEF_PATTERNS):
        score += 6.0

    # Heuristic quality
    L = len(s)
    if L < 25:
        score -= 1.0
    if L > 520:
        score -= 0.5

    # Keyword + number proximity
    if any(k.lower() in low for k in kws) and NUM_RE.search(s):
        score += 1.0

    return score

def collect_top_evidence(
    anchor_bundle: dict,
    extracted_evidence: dict,
    field: str,
    kw_map: Dict[str, List[str]],
    max_snippet_chars: int = 600,
    k: int = TOP_K_EVIDENCE,
) -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = []

    # 1) direct extracted evidence
    ev = extracted_evidence.get(field)
    if ev and isinstance(ev, dict) and ev.get("evidence"):
        txt = str(ev.get("evidence"))[:max_snippet_chars]
        candidates.append({
            "source_field": "extracted_evidence",
            "page": ev.get("page"),
            "line_no": ev.get("line_no"),
            "text": txt,
            "score": score_snippet(field, txt, kw_map) + 0.8,  # small boost
        })

    # 2) same-field anchors
    for c in (anchor_bundle.get(field, []) or []):
        snip = c.get("snippet", "")
        if not snip:
            continue
        txt = " ".join(snip.split())[:max_snippet_chars]
        candidates.append({
            "source_field": field,
            "page": c.get("page"),
            "line_no": c.get("line_no"),
            "text": txt,
            "score": score_snippet(field, txt, kw_map),
        })

    # 3) global keyword hits across all fields
    kws = kw_map.get(field, [])
    if kws:
        for src_field, lst in anchor_bundle.items():
            if not isinstance(lst, list):
                continue
            for c in lst:
                snip = c.get("snippet", "")
                if not snip:
                    continue
                low = snip.lower()
                if any(k.lower() in low for k in kws):
                    txt = " ".join(snip.split())[:max_snippet_chars]
                    candidates.append({
                        "source_field": src_field,
                        "page": c.get("page"),
                        "line_no": c.get("line_no"),
                        "text": txt,
                        "score": score_snippet(field, txt, kw_map) + 0.4,
                    })

    # de-dup
    seen = set()
    uniq = []
    for c in candidates:
        key = (c.get("page"), c.get("line_no"), flatten(c.get("text", "")).lower())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(c)

    uniq.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    return {"snippets": uniq[:k]}
