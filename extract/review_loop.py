# extract/review_loop.py
"""
Stage 3A (upgraded): FULL local terminal review loop + optional Local LLM suggestions

Reads:
  data/outputs/lease_validated.json
  data/outputs/review_queue.json              (used only for evidence snippets; NOT as the driver)
  data/outputs/lease_llm_suggestions.json     (optional; produced by llm_fallback.py)

Writes:
  data/outputs/lease_final.json

What it does:
- Reviews EVERY field in the schema (not only low-confidence ones)
- For each field:
    - shows current value + confidence
    - shows evidence snippets if present in review_queue for that field
    - shows derived suggestions/conflicts (if present in validated json)
    - shows LLM suggestion (if present)
- You can:
    - press Enter to keep current
    - type a new value
    - type 'd' to accept best derived suggestion (when shown)
    - type 'y' to accept LLM suggestion (when shown)
    - type 'none' to set None

NOTES:
- This version does NOT auto-accept LLM (so nothing gets silently skipped).
- This version does NOT allow 'skip' (since you want to review everything).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

OUT_DIR = Path("data/outputs")
VALIDATED_PATH = OUT_DIR / "lease_validated.json"
REVIEW_QUEUE_PATH = OUT_DIR / "review_queue.json"
LLM_SUGGESTIONS_PATH = OUT_DIR / "lease_llm_suggestions.json"
FINAL_PATH = OUT_DIR / "lease_final.json"

# -----------------------------
# Schema fields (must match validate_and_fill.py)
# -----------------------------
# Replace the FIELDS list in review_loop.py with this:

FIELDS = [
    # --- Identity / header ---
    "city",
    "building_name",
    "floors_units",

    # --- Lease timeline core (dependency-first) ---
    "lease_start_date",
    "lease_tenure_months",
    "lease_end_date",

    # --- Possession & rent commencement ---
    "handover_date",
    "rent_free_period_months",
    "rent_start_date",

    # --- Lock-in ---
    "lock_in_period",
    "lock_in_end_date",

    # --- Notices / renewal ---
    "termination_notice_period_months",
    "renewal_notice_period_months",
    "renewal_option",

    # --- Areas & derived efficiency ---
    "super_builtup_area_sqft",
    "carpet_area_sqft",
    "efficiency",
    "cam_area_sqft",

    # --- Commercials ---
    "monthly_rent_rs",
    "rate_per_sqft_rs",
    "monthly_cam_rs",

    # --- Parking ---
    "parking_4w_included",
    "parking_2w_included",
    "parking_charges_rs",

    # --- One-time / deposits ---
    "ifrsd_rs",
    "stamp_duty_rs",
]

# ---- typing helpers ----
DATE_FIELDS = {
    "lease_start_date",
    "lease_end_date",
    "rent_start_date",
    "handover_date",
    "lock_in_end_date",
}

INT_FIELDS = {
    "lease_tenure_months",
    "lock_in_period",
    "rent_free_period_months",
    "termination_notice_period_months",
    "renewal_notice_period_months",
    "parking_4w_included",
    "parking_2w_included",
}

FLOAT_FIELDS = {
    "carpet_area_sqft",
    "super_builtup_area_sqft",
    "cam_area_sqft",
    "efficiency",
    "rate_per_sqft_rs",
}

MONEY_FIELDS = {
    "monthly_cam_rs",
    "monthly_rent_rs",
    "parking_charges_rs",
    "stamp_duty_rs",
    "ifrsd_rs",
}


def read_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def write_json(p: Path, obj: dict) -> None:
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def coerce_value(field: str, raw: str) -> Any:
    """
    Convert typed input to the appropriate type.
    """
    s = raw.strip()
    if s == "":
        return None

    if s.lower() in {"none", "null", "na", "n/a"}:
        return None

    if field in DATE_FIELDS:
        # keep as string; user/LLM should give ISO or a readable date
        return s

    if field in INT_FIELDS:
        s2 = s.replace(",", "")
        return int(float(s2))

    if field in FLOAT_FIELDS or field in MONEY_FIELDS:
        s2 = s.replace(",", "")
        return float(s2)

    return s


def format_current(val: Any) -> str:
    if val is None:
        return "<None>"
    return str(val)


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def print_snippets(snips: List[dict]) -> None:
    """
    Expect snippets to be dicts like:
      {score, source_field, page, line_no, text}
    (Still handles legacy formats gracefully.)
    """
    if not snips:
        print("  (no evidence snippets found)")
        return

    for i, e in enumerate(snips, 1):
        if isinstance(e, dict):
            score = _safe_float(e.get("score"))
            src = e.get("source_field", "?")
            page = e.get("page")
            line_no = e.get("line_no")
            text = (e.get("text") or "").strip()
        else:
            score = None
            src = "legacy"
            page = None
            line_no = None
            text = str(e).strip()

        score_str = f"{score:.2f}" if isinstance(score, float) else "?"
        header = f"  Evidence #{i}  (score={score_str}, source={src}, page={page}, line={line_no})"

        print("\n" + header)
        print("  " + "-" * 70)
        if len(text) > 1450:
            text = text[:1450] + "..."
        print("  " + text)
        print("  " + "-" * 70)


# -----------------------------
# Derived suggestions/conflicts
# -----------------------------
def _index_by_field(items: Any) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {}
    if not items or not isinstance(items, list):
        return out
    for it in items:
        if not isinstance(it, dict):
            continue
        f = it.get("field")
        if not f:
            continue
        out.setdefault(f, []).append(it)
    return out


def _best_derived(sugs: List[dict]) -> Optional[dict]:
    if not sugs:
        return None
    return sorted(sugs, key=lambda x: float(x.get("strength", 0.0)), reverse=True)[0]


def _print_derived_suggestions(sugs: List[dict]) -> None:
    if not sugs:
        return
    sugs_sorted = sorted(sugs, key=lambda x: float(x.get("strength", 0.0)), reverse=True)
    print("\nDerived suggestions:")
    for i, s in enumerate(sugs_sorted[:3], 1):
        v = s.get("value")
        strength = s.get("strength")
        reason = s.get("reason")
        deps = s.get("depends_on") or []
        kind = s.get("kind", "derived")
        print(f"  D{i}: {v}  (strength={strength}, kind={kind})")
        if reason:
            print(f"      reason: {reason}")
        if deps:
            print(f"      depends_on: {deps}")


def _print_conflicts(conflicts: List[dict]) -> None:
    if not conflicts:
        return
    print("\n!! Conflicts detected (derived vs current):")
    for c in conflicts[:3]:
        print(f" - field={c.get('field')} current={c.get('current_value')} vs derived={c.get('suggested_value')}")
        if c.get("reason"):
            print(f"   reason: {c.get('reason')}")
        deps = c.get("depends_on") or []
        if deps:
            print(f"   depends_on: {deps}")


# -----------------------------
# LLM suggestions
# -----------------------------
def get_llm_suggestion(llm: dict, field: str) -> Optional[dict]:
    s = llm.get(field)
    if isinstance(s, dict):
        return s
    return None


def pretty_llm(s: dict) -> str:
    v = s.get("value")
    unit = s.get("unit")
    page = s.get("page")
    quote = s.get("quote")

    out = "LLM: " + ("<null>" if v is None else str(v))
    if v is not None and unit:
        out += f" {unit}"
    if page is not None:
        out += f"  (page={page})"
    if quote:
        q = quote.strip()
        if len(q) > 250:
            q = q[:250] + "..."
        out += f"\n     quote: “{q}”"
    return out


# -----------------------------
# Review queue evidence lookup
# -----------------------------
def _review_items_by_field(review: dict) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    items = review.get("items", []) or []
    for it in items:
        f = it.get("field")
        if f:
            out[f] = it
    return out


def main():
    if not VALIDATED_PATH.exists():
        raise FileNotFoundError(f"Missing {VALIDATED_PATH}. Run validate_and_fill.py first.")
    if not REVIEW_QUEUE_PATH.exists():
        raise FileNotFoundError(f"Missing {REVIEW_QUEUE_PATH}. Run validate_and_fill.py first.")

    validated = read_json(VALIDATED_PATH)
    review = read_json(REVIEW_QUEUE_PATH)

    # Derived suggestions/conflicts
    derived_sugs_by_field = _index_by_field(validated.get("derived_suggestions"))
    derived_conf_by_field = _index_by_field(validated.get("derived_conflicts"))

    # LLM suggestions optional
    llm: Dict[str, Any] = {}
    if LLM_SUGGESTIONS_PATH.exists():
        try:
            llm = read_json(LLM_SUGGESTIONS_PATH)
        except Exception:
            llm = {}

    row: Dict[str, Any] = validated.get("row", {}) or {}
    conf: Dict[str, float] = validated.get("confidence", {}) or {}

    validation_notes = review.get("validation_notes", []) or []
    evidence_lookup = _review_items_by_field(review)

    print("\n=== Lease Review Loop (FULL review) ===\n")
    if validation_notes:
        print("Validation notes (FYI):")
        for n in validation_notes[:10]:
            print(" -", n)
        print()

    audit_log: List[Dict[str, Any]] = []

    for idx, field in enumerate(FIELDS, 1):
        current = row.get(field)
        confidence = float(conf.get(field, 0.0))

        # evidence snippets only if field existed in review_queue
        it = evidence_lookup.get(field, {}) or {}
        snippets = (it.get("evidence") or {}).get("snippets") or []

        llm_s = get_llm_suggestion(llm, field)

        field_sugs = derived_sugs_by_field.get(field, []) or []
        field_conflicts = derived_conf_by_field.get(field, []) or []
        best_der = _best_derived(field_sugs)

        print("\n" + "=" * 80)
        print(f"[{idx}/{len(FIELDS)}] Field: {field}")
        print(f"Confidence: {confidence:.2f}")
        print(f"Current value: {format_current(current)}")
        print("=" * 80)

        if field_conflicts:
            _print_conflicts(field_conflicts)

        print_snippets(snippets)

        if field_sugs:
            _print_derived_suggestions(field_sugs)

        if llm_s:
            print("\n" + pretty_llm(llm_s))

        print("\nOptions:")
        if best_der is not None:
            print("  d     : accept best DERIVED suggestion")
        if llm_s and llm_s.get("value") is not None:
            print("  y     : accept LLM suggestion")
        print("  none  : set None")
        print("  Enter : keep current")
        print("  or type a new value and press Enter")

        raw = input("> ").strip()

        if raw.lower() in {"none", "null", "na", "n/a"}:
            row[field] = None
            audit_log.append({"field": field, "action": "set_none", "old": current, "new": None, "confidence": confidence})
            continue

        if raw.lower() == "d" and best_der is not None:
            try:
                new_val = coerce_value(field, str(best_der.get("value")))
                row[field] = new_val
                audit_log.append(
                    {
                        "field": field,
                        "action": "accepted_derived",
                        "old": current,
                        "new": new_val,
                        "confidence": confidence,
                        "derived": best_der,
                    }
                )
            except Exception as e:
                print(f"!! Could not apply derived value: {e}. Keeping current.")
                audit_log.append(
                    {
                        "field": field,
                        "action": "accept_derived_failed_keep",
                        "old": current,
                        "new": current,
                        "confidence": confidence,
                        "derived": best_der,
                    }
                )
            continue

        if raw.lower() == "y" and llm_s and llm_s.get("value") is not None:
            try:
                new_val = coerce_value(field, str(llm_s.get("value")))
                row[field] = new_val
                audit_log.append(
                    {
                        "field": field,
                        "action": "accepted_llm",
                        "old": current,
                        "new": new_val,
                        "confidence": confidence,
                        "llm": llm_s,
                    }
                )
            except Exception as e:
                print(f"!! Could not apply LLM value: {e}. Keeping current.")
                audit_log.append(
                    {
                        "field": field,
                        "action": "accept_llm_failed_keep",
                        "old": current,
                        "new": current,
                        "confidence": confidence,
                        "llm": llm_s,
                    }
                )
            continue

        if raw == "":
            audit_log.append({"field": field, "action": "kept", "old": current, "new": current, "confidence": confidence})
            continue

        try:
            new_val = coerce_value(field, raw)
        except Exception as e:
            print(f"!! Could not parse value: {e}. Keeping current.")
            audit_log.append({"field": field, "action": "parse_failed_keep", "old": current, "new": current, "confidence": confidence})
            continue

        row[field] = new_val
        audit_log.append({"field": field, "action": "edited", "old": current, "new": new_val, "confidence": confidence})

    # Post-fix derivations
    try:
        rent = row.get("monthly_rent_rs")
        sba = row.get("super_builtup_area_sqft")
        if rent is not None and sba not in (None, 0):
            row["rate_per_sqft_rs"] = float(rent) / float(sba)
    except Exception:
        pass

    final = {
        "row": row,
        "audit_log": audit_log,
        "source": {
            "validated_path": str(VALIDATED_PATH),
            "review_queue_path": str(REVIEW_QUEUE_PATH),
            "llm_suggestions_path": str(LLM_SUGGESTIONS_PATH) if LLM_SUGGESTIONS_PATH.exists() else None,
        },
    }

    write_json(FINAL_PATH, final)
    print("\nWrote:", FINAL_PATH)


if __name__ == "__main__":
    main()
