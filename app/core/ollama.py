from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Tuple


def check_ollama(model: str, timeout: float = 1.5) -> Tuple[bool, str]:
    url = "http://localhost:11434/api/tags"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError:
        return False, "unreachable"
    except Exception:
        return False, "error"

    models = payload.get("models") if isinstance(payload, dict) else None
    if not isinstance(models, list):
        return False, "invalid_response"

    for item in models:
        if not isinstance(item, dict):
            continue
        if item.get("name") == model:
            return True, "available"
    return False, "model_missing"
