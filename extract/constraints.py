# extract/constraints.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from dateutil.relativedelta import relativedelta
from dateutil import parser as dateparser


@dataclass
class Suggestion:
    field: str
    value: Any
    reason: str
    strength: float  # 0..1
    depends_on: List[str]
    kind: str = "derived"  # derived | inferred | heuristic


def parse_date(s: Any) -> Optional[date]:
    if not s or not isinstance(s, str):
        return None
    try:
        dt = dateparser.parse(s, dayfirst=True, fuzzy=True)
        return dt.date() if dt else None
    except Exception:
        return None


def iso(d: date) -> str:
    return d.isoformat()


def approx_equal(a: Any, b: Any, *, rel: float = 0.02, abs_tol: float = 1e-6) -> bool:
    """Relative compare for numbers with tolerance (default 2%)."""
    try:
        af = float(a)
        bf = float(b)
    except Exception:
        return False
    diff = abs(af - bf)
    if diff <= abs_tol:
        return True
    denom = max(abs(af), abs(bf), abs_tol)
    return (diff / denom) <= rel


def suggest_dates(row: Dict[str, Any]) -> List[Suggestion]:
    sug: List[Suggestion] = []

    ls = parse_date(row.get("lease_start_date"))
    tenure = row.get("lease_tenure_months")

    # lease_end_date from lease_start_date + tenure months
    if ls and isinstance(tenure, (int, float)) and tenure and tenure > 0:
        m = int(tenure)
        end_plus = ls + relativedelta(months=m)
        end_minus = end_plus - timedelta(days=1)

        sug.append(
            Suggestion(
                field="lease_end_date",
                value=iso(end_plus),
                reason="derived: lease_start_date + lease_tenure_months",
                strength=0.75,
                depends_on=["lease_start_date", "lease_tenure_months"],
                kind="derived",
            )
        )
        sug.append(
            Suggestion(
                field="lease_end_date",
                value=iso(end_minus),
                reason="derived: lease_start_date + lease_tenure_months - 1 day (common convention)",
                strength=0.70,
                depends_on=["lease_start_date", "lease_tenure_months"],
                kind="derived",
            )
        )

    # rent_start_date from rent_free_period_months (two conventions)
    rfp = row.get("rent_free_period_months")
    if isinstance(rfp, (int, float)) and rfp is not None and rfp >= 0:
        rfp_m = int(rfp)
        hd = parse_date(row.get("handover_date"))

        if ls:
            rs1 = ls + relativedelta(months=rfp_m)
            sug.append(
                Suggestion(
                    field="rent_start_date",
                    value=iso(rs1),
                    reason="derived: lease_start_date + rent_free_period_months",
                    strength=0.60,
                    depends_on=["lease_start_date", "rent_free_period_months"],
                    kind="derived",
                )
            )
        if hd:
            rs2 = hd + relativedelta(months=rfp_m)
            sug.append(
                Suggestion(
                    field="rent_start_date",
                    value=iso(rs2),
                    reason="derived: handover_date + rent_free_period_months",
                    strength=0.60,
                    depends_on=["handover_date", "rent_free_period_months"],
                    kind="derived",
                )
            )

    # lock_in_end_date from lease_start_date + lock_in_period
    lock = row.get("lock_in_period")
    if ls and isinstance(lock, (int, float)) and lock and lock > 0:
        lm = int(lock)
        li_plus = ls + relativedelta(months=lm)
        li_minus = li_plus - timedelta(days=1)

        sug.append(
            Suggestion(
                field="lock_in_end_date",
                value=iso(li_plus),
                reason="derived: lease_start_date + lock_in_period",
                strength=0.60,
                depends_on=["lease_start_date", "lock_in_period"],
                kind="derived",
            )
        )
        sug.append(
            Suggestion(
                field="lock_in_end_date",
                value=iso(li_minus),
                reason="derived: lease_start_date + lock_in_period - 1 day",
                strength=0.55,
                depends_on=["lease_start_date", "lock_in_period"],
                kind="derived",
            )
        )

    return sug


def suggest_numeric(row: Dict[str, Any]) -> List[Suggestion]:
    sug: List[Suggestion] = []

    carpet = row.get("carpet_area_sqft")
    sba = row.get("super_builtup_area_sqft")
    eff = row.get("efficiency")
    rent = row.get("monthly_rent_rs")

    # If efficiency exists and looks sane, infer missing area
    if isinstance(eff, (int, float)) and eff and 0 < eff <= 1.5:
        if isinstance(carpet, (int, float)) and carpet and not (isinstance(sba, (int, float)) and sba):
            inferred_sba = float(carpet) / float(eff)
            sug.append(
                Suggestion(
                    field="super_builtup_area_sqft",
                    value=inferred_sba,
                    reason="derived: super_builtup_area_sqft = carpet_area_sqft / efficiency",
                    strength=0.70,
                    depends_on=["carpet_area_sqft", "efficiency"],
                    kind="derived",
                )
            )
        if isinstance(sba, (int, float)) and sba and not (isinstance(carpet, (int, float)) and carpet):
            inferred_carpet = float(sba) * float(eff)
            sug.append(
                Suggestion(
                    field="carpet_area_sqft",
                    value=inferred_carpet,
                    reason="derived: carpet_area_sqft = super_builtup_area_sqft * efficiency",
                    strength=0.70,
                    depends_on=["super_builtup_area_sqft", "efficiency"],
                    kind="derived",
                )
            )

    # If both areas exist, infer efficiency (informational)
    if isinstance(carpet, (int, float)) and isinstance(sba, (int, float)) and carpet and sba:
        inferred_eff = float(carpet) / float(sba)
        sug.append(
            Suggestion(
                field="efficiency",
                value=inferred_eff,
                reason="inferred: efficiency = carpet_area_sqft / super_builtup_area_sqft",
                strength=0.55,
                depends_on=["carpet_area_sqft", "super_builtup_area_sqft"],
                kind="inferred",
            )
        )

    # Rate per sqft
    if isinstance(rent, (int, float)) and rent and isinstance(sba, (int, float)) and sba:
        rpsf = float(rent) / float(sba)
        sug.append(
            Suggestion(
                field="rate_per_sqft_rs",
                value=rpsf,
                reason="derived: rate_per_sqft_rs = monthly_rent_rs / super_builtup_area_sqft",
                strength=0.65,
                depends_on=["monthly_rent_rs", "super_builtup_area_sqft"],
                kind="derived",
            )
        )

    return sug


def apply_suggestions_if_missing(row: Dict[str, Any], suggestions: List[Suggestion]) -> List[dict]:
    """
    Only fills fields that are missing.
    If multiple suggestions exist for a field, applies the highest strength one.
    Returns applied suggestions as dicts (for audit).
    """
    by_field: Dict[str, List[Suggestion]] = {}
    for s in suggestions:
        by_field.setdefault(s.field, []).append(s)

    applied: List[dict] = []
    for field, ss in by_field.items():
        if row.get(field) not in (None, "", []):
            continue
        ss_sorted = sorted(ss, key=lambda x: x.strength, reverse=True)
        chosen = ss_sorted[0]
        row[field] = chosen.value
        applied.append(chosen.__dict__)
    return applied


def find_conflicts(row: Dict[str, Any], suggestions: List[Suggestion]) -> List[dict]:
    """
    If a field exists but strongly disagrees with the best derived suggestion,
    emit a conflict note (for review).
    """
    conflicts: List[dict] = []
    by_field: Dict[str, List[Suggestion]] = {}
    for s in suggestions:
        by_field.setdefault(s.field, []).append(s)

    for field, ss in by_field.items():
        cur = row.get(field)
        if cur in (None, "", []):
            continue

        best = max(ss, key=lambda x: x.strength)

        # numeric vs numeric
        if isinstance(cur, (int, float)) and isinstance(best.value, (int, float)):
            if not approx_equal(cur, best.value, rel=0.02):
                conflicts.append(
                    {
                        "field": field,
                        "current_value": cur,
                        "suggested_value": best.value,
                        "reason": f"conflict: current != derived (best: {best.reason})",
                        "depends_on": best.depends_on,
                    }
                )

        # date string vs date string (ISO)
        if field.endswith("_date") and isinstance(cur, str) and isinstance(best.value, str):
            if cur != best.value:
                conflicts.append(
                    {
                        "field": field,
                        "current_value": cur,
                        "suggested_value": best.value,
                        "reason": f"conflict: current != derived (best: {best.reason})",
                        "depends_on": best.depends_on,
                    }
                )

    return conflicts
