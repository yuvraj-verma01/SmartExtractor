# extract/llm_fallback.py
"""
Stage 3C: Local LLM fallback using Ollama (OFFLINE)

UPGRADED BEHAVIOR (rule-aware disambiguation):
- If a field has multiple derived candidates (from constraints layer),
  the LLM is asked to CHOOSE the correct candidate using lease language evidence.
  (LLM does NOT do math; it only selects among precomputed options.)
- For other uncertain fields, it can still do "extract if explicitly stated" fallback.

Reads:
  data/outputs/review_queue.json
  data/outputs/lease_validated.json         (for derived_suggestions + row facts)
  data/outputs/lease_anchors.json           (optional extra context; not required)

Writes:
  data/outputs/lease_llm_suggestions.json

Run:
  python .\\extract\\llm_fallback.py

Notes:
- Output is a dict keyed by field; each value is a dict compatible with review_loop.py:
    { "value": ..., "unit": ..., "page": ..., "quote": ... }
  Additional keys may be included (method, chosen_candidate, confidence, etc.).
"""

from __future__ import annotations

import json
from pathlib import Path
import os
from typing import Any, Dict, List, Optional, Tuple

import requests

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:7b-instruct-q4_K_M")

OUT_DIR = Path("data/outputs")
REVIEW_QUEUE_PATH = OUT_DIR / "review_queue.json"
VALIDATED_PATH = OUT_DIR / "lease_validated.json"
ANCHORS_PATH = OUT_DIR / "lease_anchors.json"  # optional
OUT_PATH = OUT_DIR / "lease_llm_suggestions.json"

# How much context to show
MAX_REVIEW_SNIPPETS = 6  # pull more now that you have ranked evidence
MAX_DERIVED_CANDIDATES = 4
TIMEOUT = 180  # seconds

# Only do "candidate selection" for these fields (others remain extraction-only)
DISAMBIGUATE_FIELDS = {
    "lease_end_date",
    "rent_start_date",
    "lock_in_end_date",
}

# Confidence threshold: only call LLM for fields below this
CONF_CALL_LLM_IF_LT = 0.75


# -----------------------------
# IO
# -----------------------------
def read_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def write_json(p: Path, obj: dict) -> None:
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


# -----------------------------
# Helpers: derived suggestions indexing
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


def _sorted_candidates(cands: List[dict]) -> List[dict]:
    # sort by strength desc, then prefer kind="derived" over inferred
    def key(x: dict) -> Tuple[float, int]:
        strength = float(x.get("strength", 0.0) or 0.0)
        kind = (x.get("kind") or "derived").lower()
        kind_rank = 0 if kind == "derived" else 1
        return (strength, -kind_rank)  # strength high first; derived first

    return sorted(cands, key=key, reverse=True)


def _dedupe_by_value(cands: List[dict]) -> List[dict]:
    seen = set()
    out = []
    for c in cands:
        v = c.get("value")
        k = str(v)
        if k in seen:
            continue
        seen.add(k)
        out.append(c)
    return out


# -----------------------------
# Evidence normalization
# -----------------------------
def normalize_snippets(snippets: List[Any], max_n: int) -> List[Dict[str, Any]]:
    """
    Convert snippets to uniform list[dict] with keys: text, page, line_no, source_field, score (optional).
    Handles old format (strings) and new format (dicts).
    """
    out: List[Dict[str, Any]] = []
    for s in (snippets or [])[:max_n]:
        if isinstance(s, dict):
            txt = (s.get("text") or "").strip()
            if not txt:
                continue
            out.append(
                {
                    "text": txt,
                    "page": s.get("page"),
                    "line_no": s.get("line_no"),
                    "source_field": s.get("source_field"),
                    "score": s.get("score"),
                }
            )
        elif isinstance(s, str):
            txt = s.strip()
            if not txt:
                continue
            out.append({"text": txt, "page": None, "line_no": None, "source_field": "old_format", "score": None})
        else:
            continue
    return out


def _format_context(sn: List[Dict[str, Any]]) -> str:
    blocks: List[str] = []
    for i, e in enumerate(sn, 1):
        page = e.get("page")
        line_no = e.get("line_no")
        src = e.get("source_field")
        score = e.get("score")
        text = e.get("text", "")
        blocks.append(
            f"""[Snippet {i}]
Page: {page}
Line: {line_no}
Source: {src}
Score: {score}
Text:
\"\"\"
{text}
\"\"\""""
        )
    return "\n\n".join(blocks)


# -----------------------------
# Prompt builders
# -----------------------------
def build_prompt_disambiguate(
    field: str,
    row: Dict[str, Any],
    candidates: List[dict],
    snippets: List[Any],
) -> str:
    """
    Ask LLM to CHOOSE among precomputed candidates using evidence.
    """
    sn = normalize_snippets(snippets, MAX_REVIEW_SNIPPETS)

    # Keep top candidates, dedupe by value
    c_sorted = _dedupe_by_value(_sorted_candidates(candidates))[:MAX_DERIVED_CANDIDATES]

    # Collect "known facts" from depends_on of the candidates
    deps_set = []
    seen = set()
    for c in c_sorted:
        for d in (c.get("depends_on") or []):
            if d not in seen:
                seen.add(d)
                deps_set.append(d)

    facts_lines = []
    for d in deps_set:
        facts_lines.append(f"- {d} = {row.get(d)}")
    facts = "\n".join(facts_lines) if facts_lines else "- (none)"

    cand_lines = []
    for i, c in enumerate(c_sorted, 1):
        cand_lines.append(
            f"""{i}) {c.get("value")}
   Rule: {c.get("reason")}
   kind: {c.get("kind")} | strength: {c.get("strength")}
"""
        )
    cand_text = "\n".join(cand_lines).strip() if cand_lines else "(no candidates)"

    context = _format_context(sn)

    # IMPORTANT: force candidate choice or ambiguity
    return f"""
You are reviewing a commercial lease and must DISAMBIGUATE a field using ONLY the snippets.

Field: "{field}"

Known facts (already extracted):
{facts}

Derived candidates (precomputed by deterministic rules):
{cand_text}

LEASE SNIPPETS:
{context}

TASK:
- Choose the correct candidate number based on explicit lease language.
- If NONE of the candidates is clearly supported, return chosen_candidate=null and value=null.
- If the lease explicitly states a value that matches one candidate, choose that candidate.
- Do NOT do any date math or calculations. Only pick among candidates.

OUTPUT: Return STRICT JSON ONLY (no markdown, no extra text):

{{
  "field": "{field}",
  "method": "choose_candidate",
  "chosen_candidate": <number|null>,
  "value": <string|number|null>,
  "unit": <string|null>,
  "page": <number|null>,
  "quote": <string|null>,
  "confidence": <number>
}}

Rules:
- value must equal exactly the candidate's value when chosen_candidate is set.
- quote must be copied verbatim from snippet text (or null if none).
- page must be the snippet page if quote is used.
""".strip()


def build_prompt_extract(field: str, snippets: List[Any]) -> str:
    """
    Traditional extraction: only return value if explicitly stated, else null.
    """
    sn = normalize_snippets(snippets, MAX_REVIEW_SNIPPETS)
    context = _format_context(sn)

    return f"""
You are extracting information from a commercial lease.

TASK:
Extract the value for the field: "{field}"

RULES (IMPORTANT):
- Use ONLY the text provided below.
- If the value is not explicitly stated, return null.
- Do NOT guess.
- Return STRICT JSON ONLY (no explanation, no markdown).
- Include the exact quote you used (copy the relevant phrase).
- Include the page number from the snippet you used, if available.

OUTPUT JSON SCHEMA:
{{
  "field": "{field}",
  "method": "extract",
  "value": <number|string|null>,
  "unit": <string|null>,
  "page": <number|null>,
  "quote": <string|null>,
  "confidence": <number>
}}

LEASE SNIPPETS:
{context}
""".strip()


# -----------------------------
# Ollama calling + JSON extraction
# -----------------------------
def extract_json_object(text: str) -> str:
    """
    If model outputs extra text, extract the first {...} JSON object using brace matching.
    """
    text = (text or "").strip()
    if text.startswith("{") and text.endswith("}"):
        return text

    start = text.find("{")
    if start == -1:
        raise ValueError("No '{' found in model output")

    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]

    raise ValueError("Could not extract complete JSON object from model output")


def call_ollama(prompt: str) -> Dict[str, Any]:
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.0,
            "top_p": 1.0,
            "num_predict": 768,
        },
    }

    r = requests.post(OLLAMA_URL, json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    out = r.json()

    raw = (out.get("response") or "").strip()
    json_text = extract_json_object(raw)
    return json.loads(json_text)


# -----------------------------
# Main
# -----------------------------
def main():
    if not REVIEW_QUEUE_PATH.exists():
        raise FileNotFoundError(f"Missing {REVIEW_QUEUE_PATH}. Run validate_and_fill.py first.")
    if not VALIDATED_PATH.exists():
        raise FileNotFoundError(f"Missing {VALIDATED_PATH}. Run validate_and_fill.py first.")

    review = read_json(REVIEW_QUEUE_PATH)
    validated = read_json(VALIDATED_PATH)

    # optional (not required for current prompts)
    if ANCHORS_PATH.exists():
        try:
            _ = read_json(ANCHORS_PATH)
        except Exception:
            pass

    row: Dict[str, Any] = validated.get("row", {}) or {}

    derived_by_field = _index_by_field(validated.get("derived_suggestions"))
    conflicts_by_field = _index_by_field(validated.get("derived_conflicts"))

    items = review.get("items", []) or []
    suggestions: Dict[str, Any] = {}

    print(f"Running LLM fallback on {len(items)} review fields (local Ollama)...\n")

    for it in items:
        field = it.get("field")
        if not field:
            continue

        conf = float(it.get("confidence", 0.0))
        evidence = it.get("evidence") or {}
        snippets = evidence.get("snippets") or []

        # only call LLM when low confidence AND we have context
        if conf >= CONF_CALL_LLM_IF_LT or not snippets:
            continue

        derived_candidates = derived_by_field.get(field, []) or []
        has_multiple_candidates = len(_dedupe_by_value(derived_candidates)) >= 2

        # Prefer disambiguation when field is one of the ambiguous date fields
        do_disambiguate = field in DISAMBIGUATE_FIELDS and has_multiple_candidates

        # Also disambiguate if there is an explicit derived conflict for this field
        if not do_disambiguate and conflicts_by_field.get(field):
            # if we have any candidates at all, it's useful to ask the model to pick
            do_disambiguate = field in DISAMBIGUATE_FIELDS and len(derived_candidates) >= 1

        if do_disambiguate:
            print(f"Disambiguating '{field}' among derived candidates via LLM...")
            prompt = build_prompt_disambiguate(field, row, derived_candidates, snippets)
        else:
            print(f"Extracting '{field}' via LLM...")
            prompt = build_prompt_extract(field, snippets)

        try:
            result = call_ollama(prompt)

            # ensure minimal keys exist for downstream pretty_llm()
            if isinstance(result, dict):
                if "value" not in result:
                    result["value"] = None
                if "unit" not in result:
                    result["unit"] = None
                if "page" not in result:
                    result["page"] = None
                if "quote" not in result:
                    result["quote"] = None
                suggestions[field] = result
            else:
                suggestions[field] = {
                    "field": field,
                    "method": "error",
                    "value": None,
                    "unit": None,
                    "page": None,
                    "quote": None,
                    "confidence": 0.0,
                    "error": "Non-dict JSON returned by model",
                }

        except Exception as e:
            print(f"  !! LLM failed for {field}: {e}")
            suggestions[field] = {
                "field": field,
                "method": "error",
                "value": None,
                "unit": None,
                "page": None,
                "quote": None,
                "confidence": 0.0,
                "error": str(e),
            }

    write_json(OUT_PATH, suggestions)
    print("\nWrote:", OUT_PATH)


if __name__ == "__main__":
    main()
