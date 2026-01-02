from __future__ import annotations
import re

def norm_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_multiselect_raw(s: str) -> str:
    s = (s or "").strip()
    # normalize separators to comma
    s = s.replace(";", ",").replace("\n", ",")
    # collapse multiple commas/spaces
    s = re.sub(r"\s*,\s*", ", ", s.strip())
    s = re.sub(r"(,\s*){2,}", ", ", s)
    s = s.strip().strip(",")
    return s

def split_tokens(s: str) -> list[str]:
    s = norm_multiselect_raw(s)
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]
