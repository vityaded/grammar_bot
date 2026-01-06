from __future__ import annotations
import json
from dataclasses import dataclass
import logging
import re
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
    note: str = ""
    missing: list[str] | None = None
    extra: list[str] | None = None


@dataclass
class OptionItemConfig:
    selection_policy: str
    correct_options: list[str]
    needs_review: bool
    explicit_correct_options: bool

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

def _legacy_mcq_match(user: str, canonical: str, accepted_variants: List[str], options: List[str]) -> GradeResult:
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


def _normalize_option_map(options: list[str]) -> dict[str, str]:
    mapped: dict[str, str] = {}
    for opt in options:
        cmp_key = norm_cmp_text(opt)
        if cmp_key and cmp_key not in mapped:
            mapped[cmp_key] = opt
    return mapped


def _resolve_correct_options(item: dict) -> list[str]:
    options = [str(x) for x in (item.get("options") or [])]
    normalized = _normalize_option_map(options)
    correct = item.get("correct_options")
    if isinstance(correct, list) and correct:
        resolved: list[str] = []
        for raw in correct:
            key = norm_cmp_text(str(raw))
            if key in normalized:
                resolved.append(normalized[key])
        return resolved
    canonical = (item.get("canonical") or "").strip()
    if not canonical:
        return []
    parts = [p.strip() for p in canonical.split(",") if p.strip()]
    if not parts:
        return []
    if len(parts) == 1:
        key = norm_cmp_text(parts[0])
        resolved = normalized.get(key)
        return [resolved] if resolved else []
    resolved: list[str] = []
    for part in parts:
        key = norm_cmp_text(part)
        matched = normalized.get(key)
        if not matched:
            return []
        resolved.append(matched)
    return resolved


def _format_correct_answer(
    correct_options: list[str],
    selection_policy: str,
    fallback_canonical: str,
) -> str:
    if correct_options:
        if selection_policy == "any" and len(correct_options) > 1:
            return " / ".join(correct_options)
        return ", ".join(correct_options)
    return (fallback_canonical or "").strip()


def _legacy_canonical_matches_multiple(canonical: str, options: list[str]) -> bool:
    if not canonical:
        return False
    parts = [p.strip() for p in canonical.split(",") if p.strip()]
    if len(parts) <= 1:
        return False
    mapped = _normalize_option_map(options)
    return all(norm_cmp_text(part) in mapped for part in parts)


def _infer_selection_policy(instruction: str | None) -> str | None:
    if not instruction:
        return None
    text = instruction.lower()
    any_cues = (
        "one letter",
        "one answer",
        "choose one",
        "single answer",
        "reply with one",
    )
    all_cues = (
        "select all",
        "choose all",
        "all that apply",
        "all correct",
        "both",
        "two correct answers",
    )
    if any(cue in text for cue in all_cues):
        return "all"
    if any(cue in text for cue in any_cues):
        return "any"
    return None


def resolve_option_item_config(
    *,
    item_type: str,
    item: dict,
    instruction: str | None = None,
    logger: logging.Logger | None = None,
    context: dict | None = None,
) -> OptionItemConfig:
    options = [str(x) for x in (item.get("options") or [])]
    selection_policy = item.get("selection_policy")
    if selection_policy not in ("any", "all"):
        selection_policy = None
    explicit_correct_options = isinstance(item.get("correct_options"), list)
    correct_options = _resolve_correct_options({**item, "options": options})
    needs_review = False

    if selection_policy is None:
        if item_type == "mcq":
            selection_policy = "any"
        elif item_type == "multiselect":
            selection_policy = "all"
            canonical = str(item.get("canonical") or "")
            if _legacy_canonical_matches_multiple(canonical, options):
                needs_review = True
                inferred = _infer_selection_policy(instruction)
                if inferred:
                    selection_policy = inferred

    if needs_review and logger:
        suffix = ""
        if context:
            parts = [f"{k}={v}" for k, v in context.items() if v is not None]
            if parts:
                suffix = " " + " ".join(parts)
        logger.warning("ambiguous_multiselect_item needs_review=true%s", suffix)

    return OptionItemConfig(
        selection_policy=selection_policy or "all",
        correct_options=correct_options,
        needs_review=needs_review,
        explicit_correct_options=explicit_correct_options,
    )


def _split_user_tokens(user: str) -> list[str]:
    tokens = split_tokens(user)
    if len(tokens) == 1:
        raw = user.strip()
        if raw and "," not in raw:
            space_parts = re.split(r"\s+", raw)
            if len(space_parts) > 1 and all(len(p) == 1 and p.isalnum() for p in space_parts):
                return space_parts
    return tokens


def _map_user_selections(user: str, options: list[str]) -> list[str]:
    tokens = _split_user_tokens(user)
    if not tokens:
        return []
    letters = {chr(ord("A") + i): options[i] for i in range(len(options))}
    cmp_to_option = _normalize_option_map(options)

    mapped: list[str | None] = []
    for token in tokens:
        t0 = token.strip()
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
        return []

    dedup: list[str] = []
    seen: set[str] = set()
    for v in valid:
        if v not in seen:
            dedup.append(v)
            seen.add(v)
    return dedup


def grade_option_item(
    user: str,
    canonical: str,
    accepted_variants: List[str],
    options: list[str],
    *,
    selection_policy: str,
    correct_options: list[str],
    order_sensitive: bool,
    explicit_correct_options: bool,
) -> GradeResult:
    options = [str(x) for x in (options or [])]
    canonical_display = _format_correct_answer(correct_options, selection_policy, canonical)
    selections = _map_user_selections(user, options)
    if not selections:
        if selection_policy == "any" and not explicit_correct_options:
            return _legacy_mcq_match(user, canonical, accepted_variants, options)
        return GradeResult("wrong", "—", canonical_display)

    user_norm = norm_answer_text(", ".join(selections))
    correct_cmp = [norm_cmp_text(x) for x in correct_options]
    user_cmp = [norm_cmp_text(x) for x in selections]

    if selection_policy == "any":
        if len(user_cmp) == 1 and user_cmp[0] in correct_cmp:
            return GradeResult("correct", user_norm, canonical_display)
        if len(user_cmp) > 1 and set(user_cmp).intersection(correct_cmp):
            return GradeResult("almost", user_norm, canonical_display, note="Choose ONE option")
        return GradeResult("wrong", user_norm, canonical_display)

    if order_sensitive:
        if len(user_cmp) == len(correct_cmp) and all(a == b for a, b in zip(user_cmp, correct_cmp)):
            return GradeResult("correct", user_norm, canonical_display)
        return GradeResult("wrong", user_norm, canonical_display)

    if set(user_cmp) == set(correct_cmp):
        return GradeResult("correct", user_norm, canonical_display)
    overlap = set(user_cmp).intersection(correct_cmp)
    if overlap:
        missing = [opt for opt in correct_options if norm_cmp_text(opt) not in set(user_cmp)]
        extra = [opt for opt in selections if norm_cmp_text(opt) not in set(correct_cmp)]
        return GradeResult("almost", user_norm, canonical_display, missing=missing, extra=extra)
    return GradeResult("wrong", user_norm, canonical_display)


def grade_mcq(user: str, canonical: str, accepted_variants: List[str], options: List[str], mode: str) -> GradeResult:
    return _legacy_mcq_match(user, canonical, accepted_variants, options)

def grade_multiselect(
    user: str,
    canonical: str,
    option_order: List[str],
    accepted_variants: List[str],
    *,
    order_sensitive: bool,
) -> GradeResult:
    item = {
        "canonical": canonical,
        "options": option_order,
    }
    config = resolve_option_item_config(item_type="multiselect", item=item)
    return grade_option_item(
        user,
        canonical,
        accepted_variants,
        option_order,
        selection_policy=config.selection_policy,
        correct_options=config.correct_options,
        order_sensitive=order_sensitive,
        explicit_correct_options=config.explicit_correct_options,
    )

def maybe_llm_regrade(
    *,
    llm: LLMClient | None,
    prompt: str,
    canonical: str,
    user_answer_norm: str,
    flow_mode: str,
    difficulty: str,
    ui_lang: str,
) -> tuple[bool, str]:
    if llm is None:
        return (False, "")
    out = llm.explain_and_regrade(
        prompt=prompt,
        canonical=canonical,
        user_answer=user_answer_norm,
        flow_mode=flow_mode,
        difficulty=difficulty,
        ui_lang=ui_lang,
    )
    return (True, out or "")
