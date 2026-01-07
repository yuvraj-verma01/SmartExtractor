from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple
from pathlib import Path
import json

FIELDS = [
    "city",
    "building_name",
    "floors_units",
    "lease_start_date",
    "lease_end_date",
    "rent_start_date",
    "handover_date",
    "lease_tenure_months",
    "lock_in_period",
    "lock_in_end_date",
    "rent_free_period_months",
    "termination_notice_period_months",
    "renewal_notice_period_months",
    "carpet_area_sqft",
    "super_builtup_area_sqft",
    "efficiency",
    "cam_area_sqft",
    "parking_4w_included",
    "parking_2w_included",
    "monthly_cam_rs",
    "monthly_rent_rs",
    "rate_per_sqft_rs",
    "parking_charges_rs",
    "renewal_option",
    "stamp_duty_rs",
    "ifrsd_rs",
]

@dataclass
class Hit:
    field: str
    page: int
    line_no: int
    snippet: str

# ------------------------------
# Anchor patterns (regex strings)
# ------------------------------
FIELD_ANCHORS: Dict[str, List[str]] = {

    # --- Identity ---
    "city": [
        r"\bcity\b",
        r"\blocation\b",
        r"\bsituated\s+at\b",
        r"\baddress\b",
    ],

    "building_name": [
        r"\bbuilding\s+name\b",
        r"\bproperty\s+name\b",
        r"\bpremises\s+known\s+as\b",
        r"\bcomplex\b",
        r"\btower\b",
    ],

    "floors_units": [
        r"\bfloor(s)?\b",
        r"\blevel(s)?\b",
        r"\bunit(s)?\b",
        r"\bsuite\b",
        r"\bshop\b",
        r"\bflat\b",
        r"\bno\.?\s*\d+\b",
    ],

    # --- Dates ---
    "lease_start_date": [
        r"lease\s+commencement\s+date",
        r"commencement\s+date",
        r"date\s+of\s+commencement",
        r"term\s+of\s+(this\s+)?lease.*commence",
        r"lease\s+shall\s+commence",
        r"this\s+lease\s+commences?\s+on",
    ],

    "lease_end_date": [
        r"lease\s+end\s+date",
        r"expiry\s+date",
        r"date\s+of\s+expiry",
        r"term.*expire",
        r"valid\s+until",
        r"this\s+lease\s+expires?\s+on",
    ],

    "rent_start_date": [
        r"rent\s+start\s+date",
        r"rent\s+shall\s+commence",
        r"rent\s+commencement",
        r"rent\s+payable\s+from",
        r"rent\s+shall\s+be\s+paid.*from",

        # License fee often means rent in leases
        r"(license|licence)\s+fee\s+commencement",
        r"(license|licence)\s+fee.*(commence|commencement)",
        r"(license|licence)\s+fee\s+shall\s+commence",
        r"(license|licence)\s+fee\s+payable\s+from",
    ],

    "handover_date": [
        r"handover\s+date",
        r"possession\s+date",
        r"handover\s+of\s+(the\s+)?premises",
        r"date\s+of\s+handover",
        r"date\s+of\s+possession",

        # Occupancy Certificate references are often tied to handover/possession readiness
        r"occupancy\s+certificate",
        r"occupation\s+certificate",
        r"\boc\b.*(obtained|received|granted)",
        r"grant\s+of\s+occupancy",
    ],

    # --- Tenure/Lock-in ---
    "lease_tenure_months": [
        r"lease\s+tenure",
        r"term\s+of\s+(this\s+)?lease",
        r"lease\s+term",
        r"period\s+of\s+(this\s+)?lease",
        r"for\s+a\s+period\s+of",
        r"this\s+lease\s+shall\s+be\s+for\s+a\s+period\s+of",
        r"the\s+term\s+shall\s+be",
    ],

    "lock_in_period": [
        r"lock[- ]?in",
        r"minimum\s+commitment",
        r"non[- ]?cancellable",
        r"non[- ]?terminable",
    ],

    "lock_in_end_date": [
        r"lock[- ]?in.*(end|expiry|expires?|till|until|up\s+to)",
        r"non[- ]?cancellable.*(till|until|up\s+to)",
        r"lock[- ]?in\s+period\s+ending\s+on",
        r"lock[- ]?in.*(terminate|termination)\s+after",
        r"lock[- ]?in.*(commence|start).*until",
        r"lock[- ]?in.*(for\s+a\s+period\s+of).*until",
        r"non[- ]?cancellable.*period.*until",
        r"lock[- ]?in.*(expires?|expiry).*on",
    ],

    # Rent-free / “rent starts after X months/years”
    "rent_free_period_months": [
        r"rent[- ]?free",
        r"free\s+rent",
        r"rent\s+holiday",
        r"(license|licence)\s+fee\s+holiday",
        r"rent\s+shall\s+commence\s+after",
        r"(license|licence)\s+fee\s+shall\s+commence\s+after",
        r"rent\s+payable\s+after",
        r"(license|licence)\s+fee\s+payable\s+after",
        r"after\s+\d+\s*(months?|years?)\b",  # sometimes appears standalone on next line
    ],

    "termination_notice_period_months": [
        r"termination\s+notice",
        r"notice\s+of\s+termination",
        r"notice\s+period.*termination",
        r"either\s+party.*(notice|terminate)",
        r"terminate.*\bwith\s+notice\b",
        r"terminate.*\bafter\s+\d+\s+(days?|months?)\b",
        r"\btermination\b.*\bprior\s+notice\b",
        r"\bnotice\b.*\bprior\b.*\btermination\b",
        r"\bnotice\b.*\bperiod\b.*\bmonths?\b",
    ],

    "renewal_notice_period_months": [
        r"renewal\s+notice",
        r"notice.*renewal",
        r"notice\s+to\s+renew",
        r"prior\s+written\s+notice.*renew",
        r"advance\s+notice.*renew",
        r"written\s+notice\s+of\s+renewal",
        r"renewal.*(notice|notify|notification)",
        r"notice.*(extend|extension)",
        r"option\s+to\s+renew.*(notice|notify)",
        r"exercise.*option.*renew.*notice",
        r"notice.*(exercise|exercising).*option",
        r"renewal\s+shall\s+be.*notice",
        r"(months?|days?)\s+prior\s+notice.*renew",
    ],

    # --- Areas ---
    "carpet_area_sqft": [
        r"carpet\s+area",
        r"net\s+usable\s+area",
        r"\bnua\b",
    ],

    "super_builtup_area_sqft": [
        r"super\s+built[- ]?up",
        r"\bsba\b",
        r"saleable\s+area",
        r"chargeable\s+area",
        r"chargeable\s+area\s*\(",  # common formatting
    ],

    "cam_area_sqft": [
        r"cam\s+area",
        r"common\s+area\s+maintenance\s+area",
        r"common\s+area\b",
        r"maintenance\s+area",
    ],

    "efficiency": [
        r"\befficiency\b",
        r"\befficiency\s+ratio\b",
        r"\bloading\b",
        r"\bloading\s+factor\b",
    ],

    # --- Money ---
    "monthly_rent_rs": [
        r"monthly\s+rent",
        r"rent\s+shall\s+be",
        r"rent\s+payable",
        r"(license|licence)\s+fee",

        # strong hints it is monthly recurring:
        r"payable\s+in\s+advance",
        r"every\s+calendar\s+month",
        r"per\s+calendar\s+month",
        r"per\s+month",
    ],

    "monthly_cam_rs": [
        r"monthly\s+cam",
        r"cam\s+charges",
        r"maintenance\s+charges",
        r"common\s+area\s+maintenance\s+charges",
        r"\bcam\b.*charges",
        r"common\s+area\s+maintenance",
        r"common\s+area\s+charges",
        r"maintenance\s+fee",
        r"area\s+maintenance\s+charges",
        r"cam\s+payable",
        r"cam\s+shall\s+be",
        r"maintenance\s+shall\s+be",
    ],

    "rate_per_sqft_rs": [
        r"rate\s+per\s+(sq\.?\s*ft|sqft|square\s+foot)",
        r"rent\s+per\s+(sq\.?\s*ft|sqft|square\s+foot)",
        r"\brs\.?\s*/\s*(sq\.?\s*ft|sqft)\b",
        r"per\s+(sq\.?\s*ft|sqft|square\s+foot)\s+per\s+month",
        r"per\s+(sq\.?\s*ft|sqft|square\s+foot)\s+per\s+annum",
        r"rate\s+of\s+rent\s+@?\s*(rs\.?|inr)\s*[0-9,]+\s*/\s*(sq\.?\s*ft|sqft)",
        r"rent\s+@\s*(rs\.?|inr)\s*[0-9,]+\s*/\s*(sq\.?\s*ft|sqft)",
        r"rate\s+@\s*(rs\.?|inr)\s*[0-9,]+.*(sq\.?\s*ft|sqft)",
        r"\b₹\s*/\s*(sq\.?\s*ft|sqft)\b",
    ],

    # --- Parking ---
    "parking_4w_included": [
        r"4\s*[- ]?\s*wheeler",
        r"four\s+wheeler",
        r"car\s+parking",
        r"parking\s+spaces?\b",
    ],

    "parking_2w_included": [
        r"2\s*[- ]?\s*wheeler",
        r"two\s+wheeler",
        r"bike\s+parking",
        r"scooter\s+parking",
    ],

    "parking_charges_rs": [
        r"parking\s+charges",
        r"car\s+parking\s+charges",
        r"charges\s+for\s+parking",
        r"parking\s+fee",
        r"parking\s+rent",
        r"parking\s+license\s+fee",
        r"parking\s+shall\s+be\s+charged",
        r"charges\s+for\s+parking\s+space",
    ],

    "stamp_duty_rs": [
        r"stamp\s+duty",
        r"stamping",
    ],

    "ifrsd_rs": [
        r"\bifrsd\b",
        r"interest[- ]?free\s+refundable\s+security\s+deposit",
        r"refundable\s+security\s+deposit",
        r"security\s+deposit",
    ],

    "renewal_option": [
        r"renewal",
        r"extension",
        r"option\s+to\s+renew",
        r"entitled\s+to\s+renew",
    ],
}

missing = [f for f in FIELDS if f not in FIELD_ANCHORS]
extra = [f for f in FIELD_ANCHORS if f not in FIELDS]
if missing:
    raise ValueError(f"Missing anchors for fields: {missing}")
if extra:
    print(f"Warning: extra anchors defined not in FIELDS: {extra}")

PAGE_SPLIT_RE = re.compile(r"---\s*PAGE\s+(\d+)\s*---", re.IGNORECASE)

# Optional: patterns that are “too generic” and cause spam hits if alone.
GENERIC_SPAM_RE = re.compile(r"^\s*(months?|years?|per\s+month)\s*$", re.IGNORECASE)

# Special: rent-free period numeric pattern (used to boost hits)
AFTER_PERIOD_RE = re.compile(r"\b(after|upon)\s+(\d+)\s*(month|months|year|years)\b", re.IGNORECASE)


def parse_pages(text: str) -> List[Tuple[int, List[str]]]:
    """
    Returns [(page_number, [lines...]), ...]
    """
    parts = PAGE_SPLIT_RE.split(text)
    pages: List[Tuple[int, List[str]]] = []
    for i in range(1, len(parts), 2):
        page_no = int(parts[i])
        page_text = parts[i + 1]
        lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
        pages.append((page_no, lines))
    return pages


def find_hits(pages: List[Tuple[int, List[str]]], window: int = 3) -> List[Hit]:
    hits: List[Hit] = []
    compiled: Dict[str, List[re.Pattern]] = {
        f: [re.compile(p, re.IGNORECASE) for p in pats]
        for f, pats in FIELD_ANCHORS.items()
    }

    for page_no, lines in pages:
        for idx, line in enumerate(lines):
            # skip extremely generic lines that cause noise
            if GENERIC_SPAM_RE.match(line):
                continue

            for field, patterns in compiled.items():
                if any(p.search(line) for p in patterns):
                    start = max(0, idx - window)
                    end = min(len(lines), idx + window + 1)
                    snippet = "\n".join(lines[start:end])
                    hits.append(Hit(field=field, page=page_no, line_no=idx, snippet=snippet))

            # Extra capture: if "rent shall commence" is on one line and "after X months" on next line,
            # the next line contains the numeric period and should be included.
            if AFTER_PERIOD_RE.search(line):
                start = max(0, idx - window)
                end = min(len(lines), idx + window + 1)
                snippet = "\n".join(lines[start:end])
                hits.append(Hit(field="rent_free_period_months", page=page_no, line_no=idx, snippet=snippet))

    return hits


def top_hits_by_field(hits: List[Hit], max_per_field: int = 12) -> Dict[str, List[dict]]:
    """
    Keep more hits per field so the evidence ranker / validator can pick best ones later.
    """
    out: Dict[str, List[dict]] = {}
    for h in hits:
        out.setdefault(h.field, [])
        if len(out[h.field]) < max_per_field:
            out[h.field].append({
                "page": h.page,
                "line_no": h.line_no,
                "snippet": h.snippet
            })
    return out


def main(ocr_txt_path: str):
    p = Path(ocr_txt_path)
    text = p.read_text(encoding="utf-8", errors="ignore")
    pages = parse_pages(text)
    hits = find_hits(pages, window=3)

    # Keep more candidates now; your later evidence ranker will pick top 5–6.
    bundle = {f: [] for f in FIELDS}
    bundle.update(top_hits_by_field(hits, max_per_field=12))

    out_path = Path("data/outputs") / (p.stem.replace("_ocr", "") + "_anchors.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(bundle, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Wrote anchor bundle:", out_path)


if __name__ == "__main__":
    main("data/ocr_text/lease_ocr.txt")
