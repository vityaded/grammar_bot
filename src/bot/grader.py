from __future__ import annotations
import json
from dataclasses import dataclass
from typing import List, TYPE_CHECKING
from .normalize import norm_text, norm_multiselect_raw, split_tokens
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

def grade_freetext(user: str, canonical: str, accepted_variants: List[str]) -> GradeResult:
    u = norm_text(user)
    c = (canonical or "").strip()
    acc = [norm_text(x) for x in (accepted_variants or []) if x]
    if u and (u == norm_text(c) or u in acc):
        return GradeResult("correct", u, c)
    # "almost" (still counts as wrong) — very strict heuristic
    if u and c and _close(u, norm_text(c)):
        return GradeResult("almost", u, c)
    return GradeResult("wrong", u, c)

def _close(a: str, b: str) -> bool:
    if a == b:
        return True
    if abs(len(a)-len(b)) > 1:
        return False
    mism = sum(1 for x,y in zip(a,b) if x!=y) + abs(len(a)-len(b))
    return mism == 1

def grade_mcq(user: str, canonical: str, accepted_variants: List[str], options: List[str]) -> GradeResult:
    resolved = resolve_choice(user, options)
    u = norm_text(resolved if resolved is not None else user)
    c = (canonical or "").strip()
    acc = [norm_text(x) for x in (accepted_variants or []) if x]
    if u and (u == norm_text(c) or u in acc):
        return GradeResult("correct", u, c)
    return GradeResult("wrong", u, c)

def grade_multiselect(user: str, canonical: str, option_order: List[str], accepted_variants: List[str]) -> GradeResult:
    # canonical: "opt1, opt3" (comma-separated option texts in correct order)
    u_norm = norm_multiselect_raw(user)
    toks = split_tokens(user)
    if not toks:
        return GradeResult("wrong", u_norm, (canonical or "").strip())

    opt = [str(x) for x in (option_order or [])]
    letters = {chr(ord("A")+i): opt[i] for i in range(len(opt))}
    lower_text = {o.casefold(): o for o in opt}

    mapped = []
    for t in toks:
        t0 = t.strip()
        t_cf = t0.casefold()
        if len(t0) == 1 and t0.upper() in letters:
            mapped.append(letters[t0.upper()])
            continue
        if t0.isdigit():
            idx = int(t0)
            if 1 <= idx <= len(opt):
                mapped.append(opt[idx-1])
            else:
                mapped.append(None)
            continue
        if t_cf in lower_text:
            mapped.append(lower_text[t_cf])
            continue
        mapped.append(None)

    valid = [m for m in mapped if m is not None]
    if not valid:
        return GradeResult("wrong", u_norm, (canonical or "").strip())

    # dedup same option
    dedup = []
    seen = set()
    for v in valid:
        if v not in seen:
            dedup.append(v)
            seen.add(v)

    canon_list = [norm_text(x) for x in (canonical or "").split(",") if x.strip()]
    canon_list = [x.strip() for x in canon_list]

    if dedup == canon_list:
        return GradeResult("correct", ", ".join(dedup), (canonical or "").strip())

    return GradeResult("wrong", u_norm, (canonical or "").strip())

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
