from __future__ import annotations

import re
from typing import Iterable

from ..normalize import norm_cmp_text
from .types import Issue, QuestionContext

TASK_KIND_CHOICE_EXPLICIT = "choice_explicit"
TASK_KIND_CHOICE_IMPLICIT = "choice_implicit"
TASK_KIND_GAP_FILL = "gap_fill"
TASK_KIND_SENTENCE_BUILD = "sentence_build_tokens"
TASK_KIND_REWRITE = "rewrite"
TASK_KIND_QUESTION_FORM = "question_form"
TASK_KIND_WORD_ORDER = "word_order"
TASK_KIND_SHORT_ANSWER = "short_answer"
TASK_KIND_UNKNOWN = "unknown"

PLACEHOLDER_EXAMPLES = [
    "Complete: (work) My neighbour ____ every weekend.",
    "Complete: (paint) My neighbour ____ every weekend.",
    "Complete: (call) My neighbour ____ every weekend.",
    "Complete: (run) My neighbour ____ every weekend.",
]

_CHOICE_LETTER_REQUIREMENT = re.compile(
    r"(reply with the letters|letters only|\bA\s*,\s*C\b|choose the correct option)",
    re.IGNORECASE,
)
_CHOICE_LABEL = re.compile(r"\bA\)", re.IGNORECASE)
_CHOICE_LABEL_B = re.compile(r"\bB\)", re.IGNORECASE)
_CHOOSE_OR = re.compile(r"\bchoose\b.+\bor\b.+", re.IGNORECASE)
_TOKEN_SLASH = re.compile(r"\b\w+\s*/\s*\w+")
_REWRITE_START = re.compile(r"^\s*rewrite\b", re.IGNORECASE)
_QUESTION_START = re.compile(r"^\s*(ask:|write the question:)", re.IGNORECASE)
_WORD_ORDER = re.compile(r"\bput\b.*\bcorrect (order|place)\b", re.IGNORECASE)
_SHORT_ANSWER = re.compile(r"\bshort answer\b", re.IGNORECASE)
_YES_NO_END = re.compile(r"\((yes|no)\)\s*$", re.IGNORECASE)
_GAP_FILL = re.compile(r"_{2,}|\bcomplete:", re.IGNORECASE)
_ANSWER_LINE = re.compile(r"answer\s*:\s*(.+)", re.IGNORECASE)
_SUSPICIOUS_ANSWER = re.compile(r"\b(have ate|has ate)\b", re.IGNORECASE)


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
    instruction = ctx.instruction or ""
    options = ctx.item.get("options") or []
    task_kind = infer_task_kind(ctx)
    if _instruction_requires_letters(instruction) and not options and task_kind != TASK_KIND_CHOICE_IMPLICIT:
        yield Issue(
            "INSTRUCTION_FORMAT_MISMATCH",
            "error",
            "instruction requires letter-format answers but item has no options",
            metadata={
                "task_kind": task_kind,
                "trigger": "letters_required_without_options",
            },
        )
    if any(k in instruction.lower() for k in ("write", "type", "fill in")):
        if ctx.exercise_type in ("mcq", "multiselect"):
            yield Issue(
                "INSTRUCTION_FORMAT_MISMATCH",
                "warning",
                "instruction implies free text but exercise type is choice-based",
            )


def _instruction_requires_letters(instruction: str) -> bool:
    if _CHOICE_LETTER_REQUIREMENT.search(instruction):
        return True
    if re.search(r"\bA\s*,\s*C\b", instruction, re.IGNORECASE):
        return True
    if re.search(r"separated by commas", instruction, re.IGNORECASE):
        return True
    if re.search(r"choose option\s*(A\s*/\s*B\s*/\s*C|A/B/C)", instruction, re.IGNORECASE):
        return True
    return False


def infer_task_kind(ctx: QuestionContext) -> str:
    instruction = ctx.instruction or ""
    prompt = str(ctx.item.get("prompt") or "")
    options = ctx.item.get("options") or []
    if options:
        return TASK_KIND_CHOICE_EXPLICIT
    if _has_explicit_choice_block(instruction) or _has_explicit_choice_block(prompt):
        return TASK_KIND_CHOICE_EXPLICIT
    if ctx.item.get("options") is None and _CHOOSE_OR.search(f"{instruction}\n{prompt}"):
        return TASK_KIND_CHOICE_IMPLICIT
    if _TOKEN_SLASH.search(prompt):
        return TASK_KIND_SENTENCE_BUILD
    if _REWRITE_START.match(instruction) or _REWRITE_START.match(prompt):
        return TASK_KIND_REWRITE
    if _QUESTION_START.match(prompt) or _QUESTION_START.match(instruction):
        return TASK_KIND_QUESTION_FORM
    if _WORD_ORDER.search(instruction) or _WORD_ORDER.search(prompt):
        return TASK_KIND_WORD_ORDER
    if _SHORT_ANSWER.search(instruction) or _SHORT_ANSWER.search(prompt) or _YES_NO_END.search(prompt):
        return TASK_KIND_SHORT_ANSWER
    if _GAP_FILL.search(prompt) or _GAP_FILL.search(instruction):
        return TASK_KIND_GAP_FILL
    return TASK_KIND_UNKNOWN


def infer_example_kind(example_text: str) -> str:
    if _CHOICE_LABEL.search(example_text) and _CHOICE_LABEL_B.search(example_text):
        return TASK_KIND_CHOICE_EXPLICIT
    if _TOKEN_SLASH.search(example_text):
        return TASK_KIND_SENTENCE_BUILD
    if _REWRITE_START.search(example_text):
        return TASK_KIND_REWRITE
    if _QUESTION_START.search(example_text) or (
        re.search(r"write the question", example_text, re.IGNORECASE) and example_text.strip().endswith("?")
    ):
        return TASK_KIND_QUESTION_FORM
    if _WORD_ORDER.search(example_text):
        return TASK_KIND_WORD_ORDER
    if _SHORT_ANSWER.search(example_text) or _YES_NO_END.search(example_text):
        return TASK_KIND_SHORT_ANSWER
    if _GAP_FILL.search(example_text):
        return TASK_KIND_GAP_FILL
    return TASK_KIND_UNKNOWN


def is_placeholder_example(example_text: str) -> bool:
    normalized = example_text.strip().lower()
    return any(normalized.startswith(text.lower()) for text in PLACEHOLDER_EXAMPLES)


def is_compatible(example_kind: str, task_kind: str, *, placeholder: bool) -> bool:
    if placeholder:
        return True
    if example_kind == TASK_KIND_UNKNOWN or task_kind == TASK_KIND_UNKNOWN:
        return True
    if example_kind == TASK_KIND_CHOICE_EXPLICIT:
        return task_kind == TASK_KIND_CHOICE_EXPLICIT
    if example_kind == TASK_KIND_CHOICE_IMPLICIT:
        return task_kind == TASK_KIND_CHOICE_IMPLICIT
    if example_kind == TASK_KIND_SENTENCE_BUILD:
        return task_kind in (TASK_KIND_SENTENCE_BUILD, TASK_KIND_QUESTION_FORM)
    if example_kind == TASK_KIND_REWRITE:
        return task_kind == TASK_KIND_REWRITE
    if example_kind == TASK_KIND_WORD_ORDER:
        return task_kind == TASK_KIND_WORD_ORDER
    if example_kind == TASK_KIND_SHORT_ANSWER:
        return task_kind == TASK_KIND_SHORT_ANSWER
    if example_kind == TASK_KIND_GAP_FILL:
        return task_kind in (TASK_KIND_GAP_FILL, TASK_KIND_SHORT_ANSWER, TASK_KIND_CHOICE_IMPLICIT)
    return False


def _has_explicit_choice_block(text: str) -> bool:
    return _CHOICE_LETTER_REQUIREMENT.search(text) and _CHOICE_LABEL.search(text) and _CHOICE_LABEL_B.search(text)


def check_example_task_mismatch(ctx: QuestionContext) -> Iterable[Issue]:
    instruction = ctx.instruction or ""
    match = re.search(r"Example\s*:(.*)", instruction, re.IGNORECASE | re.DOTALL)
    if not match:
        return
    example_text = match.group(1).strip()
    if not example_text:
        return
    placeholder = is_placeholder_example(example_text)
    if placeholder:
        yield Issue(
            "EXAMPLE_PLACEHOLDER",
            "info",
            "instruction example matches placeholder template",
        )
        return
    task_kind = infer_task_kind(ctx)
    example_kind = infer_example_kind(example_text)
    if not is_compatible(example_kind, task_kind, placeholder=placeholder):
        example_excerpt = example_text.replace("\n", " ").strip()
        if len(example_excerpt) > 120:
            example_excerpt = example_excerpt[:120] + "..."
        trigger = f"kind_incompatible({example_kind} vs {task_kind})"
        yield Issue(
            "EXAMPLE_TASK_MISMATCH",
            "warning",
            "instruction example format does not match prompt format",
            metadata={
                "task_kind": task_kind,
                "example_kind": example_kind,
                "example_excerpt": example_excerpt,
                "trigger": trigger,
            },
        )


def check_example_answer_suspicious(ctx: QuestionContext) -> Iterable[Issue]:
    instruction = ctx.instruction or ""
    match = re.search(r"Example\s*:(.*)", instruction, re.IGNORECASE | re.DOTALL)
    if not match:
        return
    example_text = match.group(1)
    for answer_match in _ANSWER_LINE.finditer(example_text):
        answer_line = answer_match.group(1).strip()
        if not answer_line:
            continue
        if _SUSPICIOUS_ANSWER.search(answer_line):
            yield Issue(
                "EXAMPLE_ANSWER_SUSPICIOUS",
                "warning",
                "example answer contains suspicious form",
                metadata={"answer_line_excerpt": answer_line[:120]},
            )


def gather_issues(ctx: QuestionContext) -> list[Issue]:
    issues: list[Issue] = []
    for check in (
        check_item_structure,
        check_options_canonical_mismatch,
        check_instruction_format,
        check_example_task_mismatch,
        check_example_answer_suspicious,
    ):
        issues.extend(list(check(ctx)))
    return issues
