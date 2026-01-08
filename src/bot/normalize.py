from __future__ import annotations
import re
import unicodedata

_QUOTE_MAP = {
    "’": "'",
    "‘": "'",
    "“": "\"",
    "”": "\"",
}

def _nfkc_normalize(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    for src, dst in _QUOTE_MAP.items():
        s = s.replace(src, dst)
    return s

def norm_text(s: str) -> str:
    s = _nfkc_normalize(s or "")
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_answer_text(s: str) -> str:
    s = _nfkc_normalize(s or "")
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    while s and s[-1] in ".!?,":
        s = s[:-1]
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _strip_punctuation(s: str) -> str:
    if not s:
        return ""
    cleaned = []
    for ch in s:
        if unicodedata.category(ch).startswith("P"):
            cleaned.append(" ")
        else:
            cleaned.append(ch)
    return "".join(cleaned)

def norm_cmp_text(s: str) -> str:
    normalized = norm_answer_text(s)
    normalized = _strip_punctuation(normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized.casefold()

def norm_multiselect_raw(s: str) -> str:
    s = _nfkc_normalize(s or "")
    s = s.strip()
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
