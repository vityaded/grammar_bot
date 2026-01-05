from __future__ import annotations
import json
from dataclasses import dataclass
from typing import List, TYPE_CHECKING
from .normalize import norm_answer_text, norm_cmp_text, split_tokens
from .choices import resolve_choice

if TYPE_CHECKING:
    from .llm import LLMClient
else:
    class LLMClient:  # pragma: no cover - typing only
        pass

@dataclass
class GradeResult:
    verdict: str            # correct | almost | wrong
    user_answer_norm: str
    canonical: str

def grade_freetext(user: str, canonical: str, accepted_variants: List[str], mode: str) -> GradeResult:
    user_norm = norm_answer_text(user)
    user_cmp = norm_cmp_text(user)
    canonical_display = (canonical or "").strip()
    targets = [norm_cmp_text(canonical_display)] + [
        norm_cmp_text(x) for x in (accepted_variants or []) if x
    ]
    if user_cmp and user_cmp in targets:
        return GradeResult("correct", user_norm, canonical_display)

    close = False
    if user_cmp and canonical_display:
        close = any(_is_close(user_cmp, t) for t in targets if t)

    if close and mode == "easy":
        return GradeResult("correct", user_norm, canonical_display)
    if close and mode in ("normal", "strict"):
        return GradeResult("almost", user_norm, canonical_display)
    return GradeResult("wrong", user_norm, canonical_display)

def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        cur = [i]
        for j, cb in enumerate(b, start=1):
            insert = cur[j - 1] + 1
            delete = prev[j] + 1
            replace = prev[j - 1] + (ca != cb)
            cur.append(min(insert, delete, replace))
        prev = cur
    return prev[-1]

def _is_close(a: str, b: str) -> bool:
    if not a or not b:
        return False
    limit = 2 if max(len(a), len(b)) <= 20 else 3
    return _levenshtein(a, b) <= limit

def grade_mcq(user: str, canonical: str, accepted_variants: List[str], options: List[str], mode: str) -> GradeResult:
    resolved = resolve_choice(user, options)
    answer_text = resolved if resolved is not None else user
    user_norm = norm_answer_text(answer_text)
    user_cmp = norm_cmp_text(answer_text)
    canonical_display = (canonical or "").strip()
    targets = [norm_cmp_text(canonical_display)] + [
        norm_cmp_text(x) for x in (accepted_variants or []) if x
    ]
    if user_cmp and user_cmp in targets:
        return GradeResult("correct", user_norm, canonical_display)
    return GradeResult("wrong", user_norm, canonical_display)

def grade_multiselect(
    user: str,
    canonical: str,
    option_order: List[str],
    accepted_variants: List[str],
    *,
    order_sensitive: bool,
) -> GradeResult:
    # canonical: "opt1, opt3" (comma-separated option texts in correct order)
    tokens = split_tokens(user)
    canonical_display = (canonical or "").strip()
    options = [str(x) for x in (option_order or [])]
    if not tokens:
        return GradeResult("wrong", "—", canonical_display)

    letters = {chr(ord("A") + i): options[i] for i in range(len(options))}
    cmp_to_option: dict[str, str] = {}
    for opt in options:
        cmp_key = norm_cmp_text(opt)
        if cmp_key and cmp_key not in cmp_to_option:
            cmp_to_option[cmp_key] = opt

    mapped: list[str | None] = []
    for t in tokens:
        t0 = t.strip()
        if not t0:
            continue
        if len(t0) == 1 and t0.upper() in letters:
            mapped.append(letters[t0.upper()])
            continue
        if t0.isdigit():
            idx = int(t0)
            if 1 <= idx <= len(options):
                mapped.append(options[idx - 1])
            else:
                mapped.append(None)
            continue
        t_cmp = norm_cmp_text(t0)
        mapped.append(cmp_to_option.get(t_cmp))

    valid = [m for m in mapped if m is not None]
    if not valid:
        return GradeResult("wrong", "—", canonical_display)

    dedup: list[str] = []
    seen: set[str] = set()
    for v in valid:
        if v not in seen:
            dedup.append(v)
            seen.add(v)

    user_norm = norm_answer_text(", ".join(dedup))
    canon_list = [x.strip() for x in canonical_display.split(",") if x.strip()]
    canon_cmp = [norm_cmp_text(x) for x in canon_list]
    user_cmp = [norm_cmp_text(x) for x in dedup]

    if order_sensitive:
        if len(user_cmp) == len(canon_cmp) and all(a == b for a, b in zip(user_cmp, canon_cmp)):
            return GradeResult("correct", user_norm, canonical_display)
        return GradeResult("wrong", user_norm, canonical_display)

    if set(user_cmp) == set(canon_cmp):
        return GradeResult("correct", user_norm, canonical_display)
    return GradeResult("wrong", user_norm, canonical_display)

def maybe_llm_regrade(*, llm: LLMClient | None, prompt: str, canonical: str, user_answer_norm: str, mode: str, ui_lang: str) -> tuple[bool, str]:
    if llm is None:
        return (False, "")
    out = llm.explain_and_regrade(
        prompt=prompt,
        canonical=canonical,
        user_answer=user_answer_norm,
        mode=mode,
        ui_lang=ui_lang,
    )
    return (True, out or "")
