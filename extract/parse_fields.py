# extract/parse_fields.py
"""
Lease field parser (v3) — fixes the issues you just hit:

✅ Fix 1: super_builtup_area_sqft picking "7 and 8 month" from a rate line
- We now ONLY accept an AREA if we find a number *attached to sqft/sq ft/square feet units*.
- We explicitly REJECT "per square feet per month" lines (they're rates, not areas).
- No more grabbing 7/8/12 from “month” lines as area.

✅ Fix 2: efficiency became 1228 (ratio parsing bug)
- We now support BOTH ratio directions:
    (A) Carpet = Chargeable x 0.75  (multiplier AFTER x)
    (B) Carpet = 0.75 x Chargeable  (multiplier BEFORE x)
    (C) Carpet is 75% of Chargeable
    (D) "Carpet area by 0.75 (i.e. Carpet Area=Chargeable Area x 0.75)"
- We also sanity-check ratio to be in a sensible range (0 < r <= 1.2). If not, we ignore it.

✅ Fix 3: monthly_rent_rs became IFRSD
- monthly rent now has its own picker: requires rent context + monthly context and penalizes deposit/security/IFRSD mentions.

✅ Fix 4: renewal_option line truncated
- we store a multi-line excerpt (up to N lines) from the snippet around the first renewal line.

✅ Fix 5: ratio clause is authoritative and derives missing/wrong side:
- If carpet present + ratio r => chargeable = carpet / r (and override wrong chargeable)
- If chargeable present + ratio r => carpet = chargeable * r
- efficiency is set to r directly and NOT recomputed.

Input:
  data/outputs/lease_anchors.json

Output:
  data/outputs/lease_extracted.json

Run:
  pip install python-dateutil
  python .\\extract\\parse_fields.py
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as dateparser


# -----------------------------
# Tunables
# -----------------------------
MAX_ANCHORS_PER_FIELD = 20
DATE_DAYFIRST = True

# Ratio sanity bounds (avoid “efficiency = 1228”)
RATIO_MIN = 0.05
RATIO_MAX = 1.20

# If ratio exists and extracted chargeable equals carpet, override chargeable when r < this
OVERRIDE_CHARGEABLE_IF_EQUALS_CARPET_WHEN_R_LT = 0.98

# Renewal excerpt size (lines)
RENEWAL_EXCERPT_LINES = 4


# -----------------------------
# Keyword gates
# -----------------------------
KW_CARPET = re.compile(r"(carpet\s*area|\bcarpet\b|net\s*usable\s*area|\bnua\b)", re.I)
KW_CHARGEABLE = re.compile(r"(chargeable\s*area|super\s*built[- ]?up|\bsba\b|saleable\s*area)", re.I)
KW_CAM = re.compile(r"(cam\s*area|common\s*area\s*maintenance\s*area|common\s*area)", re.I)

KW_RENT = re.compile(r"(monthly\s*rent|\brent\b|license\s*fee|licence\s*fee)", re.I)
KW_MONTHLY = re.compile(r"(per\s*month|monthly|\bpm\b|p\.m\.)", re.I)
KW_RATE_PHRASE = re.compile(r"(per\s*square\s*feet|per\s*sq\.?\s*ft|per\s*sqft)", re.I)

KW_DEPOSIT = re.compile(r"(ifrsd|security\s*deposit|refundable\s*security\s*deposit|interest[- ]?free)", re.I)
KW_STAMP = re.compile(r"(stamp\s*duty|stamping)", re.I)

KW_TENURE = re.compile(r"(tenure|term\s*of\s*(this\s*)?lease|lease\s*term|period\s*of\s*(this\s*)?lease)", re.I)
KW_LOCKIN = re.compile(r"(lock[- ]?in|non[- ]?cancellable|minimum\s*commitment)", re.I)

KW_RENEWAL = re.compile(r"(renewal|extend|extension|option\s*to\s*renew|renew)", re.I)


# -----------------------------
# Regex primitives
# -----------------------------
NUM_RE = re.compile(r"([0-9][0-9,]*(?:\.\d+)?)")

SQFT_UNIT_RE = re.compile(r"(sq\.?\s*ft|sqft|square\s*feet|sq\s*feet|sq\.?\s*feet)", re.I)
RS_UNIT_RE = re.compile(r"(₹|rs\.?|inr)", re.I)

# STRICT area token: number followed by sqft-ish units
AREA_TOKEN_RE = re.compile(
    r"([0-9][0-9,]*(?:\.\d+)?)\s*(?:sq\.?\s*ft|sqft|square\s*feet|sq\s*feet|sq\.?\s*feet)\b",
    re.I,
)

# Date-ish signal
DATE_SIGNAL_RE = re.compile(
    r"(date|dated|commence|commencement|rent|handover|possession|expiry|expire|term)",
    re.I,
)
DURATION_RE = re.compile(r"\b(\d{1,3})\s*(months?|mos?|years?|yrs?)\b", re.I)

# same-as relation for lock-in and tenure
SAME_REL_RE_LIST = [
    re.compile(r"lock[- ]?in.*(same as|equal to|co-terminus with|coterminous with).*lease\s*(term|tenure)", re.I),
    re.compile(r"lease\s*(term|tenure).*(same as|equal to|co-terminus with|coterminous with).*lock[- ]?in", re.I),
]

# Parking patterns (pair-aware)
CAR_PARK_RE = re.compile(r"\b(\d{1,4})\s*(?:car|4)\s*parking\b", re.I)
CAR_PARK_RE2 = re.compile(r"\b(\d{1,4})\s*car\s*parking\s*spaces?\b", re.I)
TWO_PARK_RE = re.compile(r"\b(\d{1,4})\s*(?:two|2)\s*wheeler\s*parking\b", re.I)
TWO_PARK_RE2 = re.compile(r"\b(\d{1,4})\s*two\s*wheeler\s*parking\s*spaces?\b", re.I)


# Ratio patterns (multiple directions)
# 1) Carpet Area = Chargeable Area x 0.75  (multiplier AFTER x)
RATIO_AFTER_X_RE = re.compile(
    r"(carpet\s*area)\s*=\s*(?:the\s*)?(chargeable\s*area|super\s*built[- ]?up|\bsba\b)\s*(?:x|\*)\s*(\d+(?:\.\d+)?)",
    re.I,
)

# 2) Carpet Area = 0.75 x Chargeable Area  (multiplier BEFORE x)
RATIO_BEFORE_X_RE = re.compile(
    r"(carpet\s*area)\s*=\s*(\d+(?:\.\d+)?)\s*(?:x|\*)\s*(?:the\s*)?(chargeable\s*area|super\s*built[- ]?up|\bsba\b)",
    re.I,
)

# 3) Carpet area is 75% of chargeable area
RATIO_PCT_RE = re.compile(
    r"(carpet\s*area).*?(\d+(?:\.\d+)?)\s*(?:%|percent)\s*(?:of)?\s*(chargeable\s*area|super\s*built[- ]?up|\bsba\b)",
    re.I,
)

# 4) Looser: “Carpet area ... by 0.75 (i.e. Carpet Area=Chargeable Area x 0.75)”
RATIO_LOOSE_BY_RE = re.compile(
    r"(carpet\s*area).*?\bby\b\s*(\d+(?:\.\d+)?)",
    re.I,
)


def normalize_num(s: str) -> float:
    return float(s.replace(",", ""))


def to_months(n: int, unit: str) -> int:
    unit = unit.lower()
    if unit.startswith("year") or unit.startswith("yr"):
        return n * 12
    return n


@dataclass
class Candidate:
    field: str
    page: int
    line_no: int
    snippet: str


@dataclass
class Pick:
    value: Any
    evidence: str
    page: int
    line_no: int
    score: float
    rationale: str


# -----------------------------
# Helpers
# -----------------------------
def iter_candidates(bundle: dict, field: str) -> List[Candidate]:
    raw = bundle.get(field, [])[:MAX_ANCHORS_PER_FIELD]
    out: List[Candidate] = []
    for c in raw:
        out.append(Candidate(field=field, page=int(c["page"]), line_no=int(c["line_no"]), snippet=c["snippet"]))
    return out


def best_pick(picks: List[Pick]) -> Optional[Pick]:
    if not picks:
        return None
    picks.sort(key=lambda p: p.score, reverse=True)
    return picks[0]


def clean_lines(snippet: str) -> List[str]:
    return [ln.strip() for ln in snippet.splitlines() if ln.strip()]


# -----------------------------
# Date picking
# -----------------------------
def try_parse_date_from_line(line: str) -> Optional[str]:
    if not (DATE_SIGNAL_RE.search(line) or re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", line)):
        return None
    try:
        dt = dateparser.parse(line, dayfirst=DATE_DAYFIRST, fuzzy=True)
        if dt:
            return dt.date().isoformat()
    except Exception:
        return None
    return None


def pick_date(field: str, cands: List[Candidate]) -> Optional[Pick]:
    picks: List[Pick] = []
    for c in cands:
        for ln in clean_lines(c.snippet):
            d = try_parse_date_from_line(ln)
            if not d:
                continue
            low = ln.lower()
            sc = 1.0
            if field == "lease_start_date" and ("commence" in low or "commencement" in low):
                sc += 2.0
            if field == "lease_end_date" and ("expiry" in low or "expire" in low or "end" in low):
                sc += 2.0
            if field == "rent_start_date" and ("rent" in low and ("commence" in low or "payable" in low or "from" in low)):
                sc += 2.0
            if field == "handover_date" and ("handover" in low or "possession" in low):
                sc += 2.0
            picks.append(Pick(d, ln, c.page, c.line_no, sc, "parsed date from contextual line"))
    return best_pick(picks)


# -----------------------------
# Duration picking
# -----------------------------
def extract_duration_months(text: str) -> Optional[int]:
    s = " ".join(text.split())
    m = DURATION_RE.search(s)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    return to_months(n, unit)


def pick_duration(field: str, cands: List[Candidate], context_kw: re.Pattern) -> Optional[Pick]:
    picks: List[Pick] = []
    for c in cands:
        for ln in clean_lines(c.snippet):
            if not context_kw.search(ln):
                continue
            dur = extract_duration_months(ln)
            if dur is None:
                continue
            sc = 2.0
            if "month" in ln.lower() or "year" in ln.lower() or "yr" in ln.lower():
                sc += 1.5
            picks.append(Pick(dur, ln, c.page, c.line_no, sc, "parsed duration from contextual line"))
    return best_pick(picks)


def detect_same_relationship(snippet: str) -> bool:
    s = " ".join(snippet.split())
    return any(p.search(s) for p in SAME_REL_RE_LIST)


# -----------------------------
# Area picking (STRICT: require sqft token)
# -----------------------------
def extract_area_from_line(line: str) -> Optional[float]:
    """
    Returns area if we find `<number> <sqft unit>` in the line.
    Rejects rate lines like "per square feet per month".
    """
    low = line.lower()
    # Reject rate lines
    if KW_RATE_PHRASE.search(low) and ("month" in low or KW_MONTHLY.search(low)):
        return None

    m = AREA_TOKEN_RE.search(line)
    if not m:
        return None
    return normalize_num(m.group(1))


def pick_area(field: str, kw: re.Pattern, cands: List[Candidate]) -> Optional[Pick]:
    picks: List[Pick] = []
    for c in cands:
        for ln in clean_lines(c.snippet):
            if not kw.search(ln):
                continue
            val = extract_area_from_line(ln)
            if val is None:
                continue

            sc = 3.0
            # Stronger if keyword and unit both present
            if SQFT_UNIT_RE.search(ln):
                sc += 1.5
            # Penalize if line is clearly about rate (still might slip)
            if KW_RATE_PHRASE.search(ln.lower()):
                sc -= 2.5

            picks.append(Pick(val, ln, c.page, c.line_no, sc, "picked area from keyword+sqft line"))
    return best_pick(picks)


def pick_cam_area(cands: List[Candidate]) -> Optional[Pick]:
    return pick_area("cam_area_sqft", KW_CAM, cands)


# -----------------------------
# Ratio extraction (ROBUST + sanity)
# -----------------------------
def extract_ratio_from_snippet(snippet: str) -> Optional[float]:
    s = " ".join(snippet.split())
    low = s.lower()

    # Priority: explicit equation forms
    m = RATIO_AFTER_X_RE.search(s)
    if m:
        r = float(m.group(3))
        return r

    m = RATIO_BEFORE_X_RE.search(s)
    if m:
        r = float(m.group(2))
        return r

    m = RATIO_PCT_RE.search(s)
    if m:
        return float(m.group(2)) / 100.0

    # Loose "by 0.75" only if the snippet also mentions chargeable/SBA somewhere (avoid picking 1228)
    m = RATIO_LOOSE_BY_RE.search(s)
    if m and KW_CHARGEABLE.search(low):
        r = float(m.group(2))
        return r

    return None


def find_ratio(bundle: dict) -> Optional[Tuple[float, Candidate]]:
    # search in area anchors first
    for c in (iter_candidates(bundle, "carpet_area_sqft") + iter_candidates(bundle, "super_builtup_area_sqft")):
        r = extract_ratio_from_snippet(c.snippet)
        if r is None:
            continue
        # sanity
        if not (RATIO_MIN <= r <= RATIO_MAX):
            continue
        return r, c
    return None


# -----------------------------
# Money picking (specialize rent + ifrsd)
# -----------------------------
def extract_all_numbers(line: str) -> List[float]:
    return [normalize_num(m.group(1)) for m in NUM_RE.finditer(line)]


def pick_monthly_rent(cands: List[Candidate]) -> Optional[Pick]:
    """
    Must be rent-ish AND monthly-ish.
    Avoid deposit/IFRSD lines even if they contain a big number.
    """
    picks: List[Pick] = []
    for c in cands:
        for ln in clean_lines(c.snippet):
            low = ln.lower()
            if not (KW_RENT.search(low) and KW_MONTHLY.search(low)):
                continue
            if KW_DEPOSIT.search(low):
                continue  # don't confuse rent with deposit
            if SQFT_UNIT_RE.search(ln) and KW_RATE_PHRASE.search(low):
                # rate line (Rs / sqft / month) usually not total rent
                continue

            nums = extract_all_numbers(ln)
            if not nums:
                continue

            # Rent amount on that line usually the largest number
            val = max(nums)
            sc = 4.0
            if RS_UNIT_RE.search(ln):
                sc += 1.0
            picks.append(Pick(val, ln, c.page, c.line_no, sc, "picked monthly rent from rent+monthly line"))
    return best_pick(picks)


def pick_generic_money(field: str, cands: List[Candidate]) -> Optional[Pick]:
    """
    For CAM, stamp duty, parking charges (not IFRSD, not rent).
    """
    picks: List[Pick] = []
    for c in cands:
        for ln in clean_lines(c.snippet):
            low = ln.lower()
            nums = extract_all_numbers(ln)
            if not nums:
                continue

            # Context filters
            if field == "monthly_cam_rs":
                if not ("cam" in low or "maintenance" in low):
                    continue
            if field == "parking_charges_rs":
                if not ("parking" in low and "charges" in low):
                    continue
            if field == "stamp_duty_rs":
                if not KW_STAMP.search(low):
                    continue

            # Avoid area lines
            if SQFT_UNIT_RE.search(ln):
                continue

            val = max(nums)
            sc = 2.0
            if RS_UNIT_RE.search(ln):
                sc += 1.0
            picks.append(Pick(val, ln, c.page, c.line_no, sc, f"picked {field} from context line"))
    return best_pick(picks)


def pick_ifrsd(cands: List[Candidate]) -> Optional[Pick]:
    """
    IFRSD/security deposit often appears without Rs/₹ in OCR, so keyword gating is key.
    Also avoid accidentally picking small numbers (months/years).
    """
    picks: List[Pick] = []
    for c in cands:
        for ln in clean_lines(c.snippet):
            low = ln.lower()
            if not KW_DEPOSIT.search(low):
                continue
            # Avoid rate/area lines
            if SQFT_UNIT_RE.search(ln) and KW_RATE_PHRASE.search(low):
                continue

            nums = extract_all_numbers(ln)
            if not nums:
                continue

            # Prefer "big" deposit-looking numbers
            big = [x for x in nums if x >= 50_000]
            val = max(big) if big else max(nums)

            sc = 4.0
            if "ifrsd" in low:
                sc += 2.0
            if "security deposit" in low:
                sc += 1.5
            if RS_UNIT_RE.search(ln):
                sc += 0.5

            picks.append(Pick(val, ln, c.page, c.line_no, sc, "picked IFRSD/deposit from deposit-context line"))
    return best_pick(picks)


# -----------------------------
# Parking (pair-aware)
# -----------------------------
def pick_parking_pair(cands: List[Candidate]) -> Tuple[Optional[Pick], Optional[Pick]]:
    best_both: Optional[Tuple[Pick, Pick, float]] = None
    best_4w: Optional[Pick] = None
    best_2w: Optional[Pick] = None

    for c in cands:
        s = " ".join(c.snippet.split())
        car = CAR_PARK_RE.search(s) or CAR_PARK_RE2.search(s)
        two = TWO_PARK_RE.search(s) or TWO_PARK_RE2.search(s)

        if car:
            p = Pick(int(car.group(1)), s, c.page, c.line_no, 3.0, "parsed 4w parking count")
            if best_4w is None or p.score > best_4w.score:
                best_4w = p

        if two:
            p = Pick(int(two.group(1)), s, c.page, c.line_no, 3.0, "parsed 2w parking count")
            if best_2w is None or p.score > best_2w.score:
                best_2w = p

        if car and two:
            p4 = Pick(int(car.group(1)), s, c.page, c.line_no, 6.0, "parsed both parking counts from same snippet")
            p2 = Pick(int(two.group(1)), s, c.page, c.line_no, 6.0, "parsed both parking counts from same snippet")
            score_both = p4.score + p2.score
            if best_both is None or score_both > best_both[2]:
                best_both = (p4, p2, score_both)

    if best_both:
        return best_both[0], best_both[1]
    return best_4w, best_2w


# -----------------------------
# Renewal option (multi-line excerpt)
# -----------------------------
def pick_renewal_excerpt(cands: List[Candidate]) -> Optional[Pick]:
    picks: List[Pick] = []
    for c in cands:
        lns = clean_lines(c.snippet)
        for i, ln in enumerate(lns):
            if KW_RENEWAL.search(ln):
                excerpt = "\n".join(lns[i : i + RENEWAL_EXCERPT_LINES])
                sc = 2.0 + min(2.0, len(excerpt) / 200.0)
                picks.append(Pick(excerpt, excerpt, c.page, c.line_no, sc, "kept multi-line renewal excerpt"))
                break
    return best_pick(picks)


# -----------------------------
# Ratio application (authoritative)
# -----------------------------
def apply_ratio_authoritative(bundle: dict, row: Dict[str, Any], evidence: Dict[str, Any], notes: List[str]) -> None:
    found = find_ratio(bundle)
    ca = row.get("carpet_area_sqft")
    ch = row.get("super_builtup_area_sqft")

    if not found:
        # no ratio clause => compute efficiency if both exist
        if ca is not None and ch is not None and ch != 0:
            row["efficiency"] = ca / ch
        else:
            row["efficiency"] = None
        return

    r, cand = found
    row["efficiency"] = r
    evidence["efficiency"] = {
        "page": cand.page,
        "line_no": cand.line_no,
        "evidence": "ratio clause detected",
        "rationale": "efficiency set directly from clause (Carpet = Chargeable × r)",
        "score": 6.0,
    }

    # Prefer deriving chargeable if carpet exists (your case)
    if ca is not None and r != 0:
        derived_ch = ca / r
        should_override = ch is None

        if ch is not None:
            if abs(ch - ca) < 1e-6 and r < OVERRIDE_CHARGEABLE_IF_EQUALS_CARPET_WHEN_R_LT:
                should_override = True
            if ch < ca and r <= 1.0:
                should_override = True

        if should_override:
            row["super_builtup_area_sqft"] = derived_ch
            notes.append("Derived chargeable/super_builtup_area_sqft from carpet_area_sqft / r (ratio clause authoritative).")
            evidence["super_builtup_area_sqft"] = evidence.get("super_builtup_area_sqft") or {
                "page": cand.page,
                "line_no": cand.line_no,
                "evidence": "Derived from carpet / r",
                "rationale": "chargeable area computed from ratio clause",
                "score": 5.0,
            }
        return

    # Else derive carpet from chargeable
    if ch is not None:
        derived_ca = ch * r
        if ca is None:
            row["carpet_area_sqft"] = derived_ca
            notes.append("Derived carpet_area_sqft from chargeable/super_builtup_area_sqft × r (ratio clause authoritative).")
            evidence["carpet_area_sqft"] = evidence.get("carpet_area_sqft") or {
                "page": cand.page,
                "line_no": cand.line_no,
                "evidence": "Derived from chargeable × r",
                "rationale": "carpet area computed from ratio clause",
                "score": 5.0,
            }


# -----------------------------
# Derived fields
# -----------------------------
def derive_fields(row: Dict[str, Any]) -> None:
    # rate per sqft on chargeable/super built-up (your definition)
    rent = row.get("monthly_rent_rs")
    ch = row.get("super_builtup_area_sqft")
    if rent is not None and ch is not None and ch != 0:
        row["rate_per_sqft_rs"] = rent / ch
    else:
        row["rate_per_sqft_rs"] = None


def soft_sanity(row: Dict[str, Any], notes: List[str]) -> None:
    ca = row.get("carpet_area_sqft")
    ch = row.get("super_builtup_area_sqft")
    r = row.get("efficiency")

    if r is not None and not (RATIO_MIN <= r <= RATIO_MAX):
        notes.append("Suspicious: efficiency outside expected range; ratio parsing may be wrong.")
    if ca is not None and ch is not None:
        if ca > ch:
            notes.append("Suspicious: carpet_area_sqft > chargeable/super_builtup_area_sqft.")
        if abs(ca - ch) < 1e-6 and r is not None and r < 0.99:
            notes.append("Suspicious: carpet equals chargeable but efficiency < 1.0 (likely extraction conflict).")


# -----------------------------
# Main extraction
# -----------------------------
def extract_row(bundle: dict) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    row: Dict[str, Any] = {}
    evidence: Dict[str, Any] = {}
    notes: List[str] = []

    # Dates
    for f in ["lease_start_date", "lease_end_date", "rent_start_date", "handover_date", "lock_in_end_date"]:
        p = pick_date(f, iter_candidates(bundle, f))
        row[f] = p.value if p else None
        if p:
            evidence[f] = {"page": p.page, "line_no": p.line_no, "evidence": p.evidence, "rationale": p.rationale, "score": p.score}

    # Durations
    tenure = pick_duration("lease_tenure_months", iter_candidates(bundle, "lease_tenure_months"), KW_TENURE)
    row["lease_tenure_months"] = tenure.value if tenure else None
    if tenure:
        evidence["lease_tenure_months"] = {"page": tenure.page, "line_no": tenure.line_no, "evidence": tenure.evidence, "rationale": tenure.rationale, "score": tenure.score}

    lockin = pick_duration("lock_in_period", iter_candidates(bundle, "lock_in_period"), KW_LOCKIN)
    row["lock_in_period"] = lockin.value if lockin else None
    if lockin:
        evidence["lock_in_period"] = {"page": lockin.page, "line_no": lockin.line_no, "evidence": lockin.evidence, "rationale": lockin.rationale, "score": lockin.score}

    for f, kw in [
        ("termination_notice_period_months", re.compile(r"(termination|notice)", re.I)),
        ("renewal_notice_period_months", re.compile(r"(renewal|notice)", re.I)),
        ("rent_free_period_months", re.compile(r"(rent[- ]?free|free\s*rent|rent\s*holiday)", re.I)),
    ]:
        p = pick_duration(f, iter_candidates(bundle, f), kw)
        row[f] = p.value if p else None
        if p:
            evidence[f] = {"page": p.page, "line_no": p.line_no, "evidence": p.evidence, "rationale": p.rationale, "score": p.score}

    # Areas (strict: must include number+sqft unit)
    ch = pick_area("super_builtup_area_sqft", KW_CHARGEABLE, iter_candidates(bundle, "super_builtup_area_sqft"))
    row["super_builtup_area_sqft"] = ch.value if ch else None
    if ch:
        evidence["super_builtup_area_sqft"] = {"page": ch.page, "line_no": ch.line_no, "evidence": ch.evidence, "rationale": ch.rationale, "score": ch.score}

    ca = pick_area("carpet_area_sqft", KW_CARPET, iter_candidates(bundle, "carpet_area_sqft"))
    row["carpet_area_sqft"] = ca.value if ca else None
    if ca:
        evidence["carpet_area_sqft"] = {"page": ca.page, "line_no": ca.line_no, "evidence": ca.evidence, "rationale": ca.rationale, "score": ca.score}

    cam = pick_cam_area(iter_candidates(bundle, "cam_area_sqft"))
    row["cam_area_sqft"] = cam.value if cam else None
    if cam:
        evidence["cam_area_sqft"] = {"page": cam.page, "line_no": cam.line_no, "evidence": cam.evidence, "rationale": cam.rationale, "score": cam.score}

    # Ratio clause authoritative (sets efficiency + derives the missing/wrong side)
    apply_ratio_authoritative(bundle, row, evidence, notes)

    # Money
    rent = pick_monthly_rent(iter_candidates(bundle, "monthly_rent_rs"))
    row["monthly_rent_rs"] = rent.value if rent else None
    if rent:
        evidence["monthly_rent_rs"] = {"page": rent.page, "line_no": rent.line_no, "evidence": rent.evidence, "rationale": rent.rationale, "score": rent.score}

    for f in ["monthly_cam_rs", "parking_charges_rs", "stamp_duty_rs"]:
        p = pick_generic_money(f, iter_candidates(bundle, f))
        row[f] = p.value if p else None
        if p:
            evidence[f] = {"page": p.page, "line_no": p.line_no, "evidence": p.evidence, "rationale": p.rationale, "score": p.score}

    ifrsd = pick_ifrsd(iter_candidates(bundle, "ifrsd_rs"))
    row["ifrsd_rs"] = ifrsd.value if ifrsd else None
    if ifrsd:
        evidence["ifrsd_rs"] = {"page": ifrsd.page, "line_no": ifrsd.line_no, "evidence": ifrsd.evidence, "rationale": ifrsd.rationale, "score": ifrsd.score}
    if row.get("ifrsd_rs") is not None and row["ifrsd_rs"] < 50_000:
        notes.append("Suspicious: IFRSD extracted value is small; may have picked a duration/other number.")

    # Parking (pair-aware)
    park_cands = iter_candidates(bundle, "parking_4w_included") + iter_candidates(bundle, "parking_2w_included")
    p4, p2 = pick_parking_pair(park_cands)
    row["parking_4w_included"] = p4.value if p4 else None
    row["parking_2w_included"] = p2.value if p2 else None
    if p4:
        evidence["parking_4w_included"] = {"page": p4.page, "line_no": p4.line_no, "evidence": p4.evidence, "rationale": p4.rationale, "score": p4.score}
    if p2:
        evidence["parking_2w_included"] = {"page": p2.page, "line_no": p2.line_no, "evidence": p2.evidence, "rationale": p2.rationale, "score": p2.score}

    # Renewal option (multi-line)
    ren = pick_renewal_excerpt(iter_candidates(bundle, "renewal_option"))
    row["renewal_option"] = ren.value if ren else None
    if ren:
        evidence["renewal_option"] = {"page": ren.page, "line_no": ren.line_no, "evidence": ren.evidence, "rationale": ren.rationale, "score": ren.score}

    # Tenure/lock-in same-as enforcement
    same_clause = False
    for c in (iter_candidates(bundle, "lock_in_period") + iter_candidates(bundle, "lease_tenure_months")):
        if detect_same_relationship(c.snippet):
            same_clause = True
            break
    if same_clause:
        lt = row.get("lease_tenure_months")
        li = row.get("lock_in_period")
        if lt is not None and li is None:
            row["lock_in_period"] = lt
            notes.append("Applied same-as: lock_in_period = lease_tenure_months.")
        elif li is not None and lt is None:
            row["lease_tenure_months"] = li
            notes.append("Applied same-as: lease_tenure_months = lock_in_period.")
        elif lt is not None and li is not None and lt != li:
            notes.append("Inconsistency: same-as clause detected but lock-in and tenure differ. Review required.")
        evidence["lock_in_equals_tenure_detected"] = True

    # Derived fields
    derive_fields(row)

    # Sanity
    soft_sanity(row, notes)

    return row, evidence, notes


def main():
    bundle_path = Path("data/outputs/lease_anchors.json")
    if not bundle_path.exists():
        raise FileNotFoundError(f"Missing anchor bundle: {bundle_path}. Run extract/anchors.py first.")

    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    row, evidence, notes = extract_row(bundle)

    out = {"row": row, "evidence": evidence, "notes": notes}
    out_path = Path("data/outputs/lease_extracted.json")
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Wrote:", out_path)


if __name__ == "__main__":
    main()
