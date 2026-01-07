from __future__ import annotations

import re
from typing import Iterable

from ..normalize import norm_cmp_text
from .types import Issue, QuestionContext


def check_item_structure(ctx: QuestionContext) -> Iterable[Issue]:
    item = ctx.item
    if not isinstance(item, dict):
        yield Issue("MALFORMED_ITEM", "error", "item is not an object")
        return
    if not item.get("prompt"):
        yield Issue("MALFORMED_ITEM", "error", "missing prompt")
    if "canonical" not in item:
        yield Issue("MALFORMED_ITEM", "error", "missing canonical")
    options = item.get("options")
    if ctx.exercise_type in ("mcq", "multiselect") and not options:
        yield Issue("MALFORMED_ITEM", "error", "options required for mcq/multiselect")


def check_options_canonical_mismatch(ctx: QuestionContext) -> Iterable[Issue]:
    item = ctx.item
    options = [str(x) for x in (item.get("options") or [])]
    if not options:
        return
    canonical = str(item.get("canonical") or "")
    parts = [p.strip() for p in canonical.split(",") if p.strip()]
    if not parts:
        return
    option_set = {norm_cmp_text(opt) for opt in options if norm_cmp_text(opt)}
    missing = [part for part in parts if norm_cmp_text(part) not in option_set]
    if missing:
        yield Issue(
            "OPTIONS_CANONICAL_MISMATCH",
            "error",
            f"canonical parts not in options: {', '.join(missing)}",
        )


def check_instruction_format(ctx: QuestionContext) -> Iterable[Issue]:
    instruction = (ctx.instruction or "").lower()
    options = ctx.item.get("options") or []
    if any(k in instruction for k in ("reply with letters", "letters only", "choose", "select")):
        if not options:
            yield Issue(
                "INSTRUCTION_FORMAT_MISMATCH",
                "error",
                "instruction mentions choosing letters but item has no options",
            )
    if any(k in instruction for k in ("write", "type", "fill in")):
        if ctx.exercise_type in ("mcq", "multiselect"):
            yield Issue(
                "INSTRUCTION_FORMAT_MISMATCH",
                "warning",
                "instruction implies free text but exercise type is choice-based",
            )


def _example_signature(text: str) -> dict[str, bool]:
    return {
        "has_blank": bool(re.search(r"_{2,}|\.{3,}", text)),
        "has_options": bool(re.search(r"\bA[\).:]\b", text)) and bool(re.search(r"\bB[\).:]\b", text)),
    }


def check_example_task_mismatch(ctx: QuestionContext) -> Iterable[Issue]:
    instruction = ctx.instruction or ""
    match = re.search(r"Example\s*:(.*)", instruction, re.IGNORECASE)
    if not match:
        return
    example_text = match.group(1).strip()
    if not example_text:
        return
    prompt = str(ctx.item.get("prompt") or "")
    example_sig = _example_signature(example_text)
    prompt_sig = _example_signature(prompt)
    if example_sig != prompt_sig:
        yield Issue(
            "EXAMPLE_TASK_MISMATCH",
            "warning",
            "instruction example format does not match prompt format",
        )


def gather_issues(ctx: QuestionContext) -> list[Issue]:
    issues: list[Issue] = []
    for check in (
        check_item_structure,
        check_options_canonical_mismatch,
        check_instruction_format,
        check_example_task_mismatch,
    ):
        issues.extend(list(check(ctx)))
    return issues
