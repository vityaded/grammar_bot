from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .llm import LLMClient
from .models import RuleI18n, RuleI18nV2, UnitExercise

_DEFAULT_FORBIDDEN_MARKERS = [
    "yesterday",
    "last week",
    "last month",
    "last year",
    "ago",
    "did ",
    "was ",
    "were ",
]

_FORBIDDEN_MARKERS_BY_UNIT: dict[str, list[str]] = {}


def _section_sort_key(section_path: str | None) -> tuple[int, str, int]:
    if not section_path:
        return (1, "", 0)
    text = section_path.strip()
    prefix = ""
    num = 0
    for i, ch in enumerate(text):
        if ch.isdigit():
            prefix = text[:i]
            try:
                num = int(text[i:] or 0)
            except ValueError:
                num = 0
            break
    else:
        prefix = text
    return (0, prefix.upper(), num)


def _contains_forbidden_markers(payload: dict, markers: list[str]) -> bool:
    if not markers:
        return False
    collected: list[str] = []
    instruction = payload.get("instruction")
    if isinstance(instruction, str):
        collected.append(instruction)
    for item in payload.get("items", []):
        if not isinstance(item, dict):
            continue
        for key in ("prompt", "canonical"):
            val = item.get(key)
            if isinstance(val, str):
                collected.append(val)
        options = item.get("options")
        if isinstance(options, list):
            collected.extend(str(o) for o in options)
    blob = " ".join(collected).lower()
    return any(marker.lower() in blob for marker in markers)


def _forbidden_markers_for_unit(unit_key: str, rule_text: str) -> list[str]:
    custom = _FORBIDDEN_MARKERS_BY_UNIT.get(unit_key)
    if custom is not None:
        return custom
    lower_rule = (rule_text or "").lower()
    if "present" in lower_rule and "past" not in lower_rule:
        return _DEFAULT_FORBIDDEN_MARKERS
    return []


def _rule_text_from_v2(rule: RuleI18nV2) -> str:
    text = rule.rule_text_en or rule.rule_text_uk or ""
    if not text:
        text = rule.rule_short_en or rule.rule_short_uk or ""
    if not text and rule.title_en:
        text = rule.title_en
    if not text and rule.title_uk:
        text = rule.title_uk
    if rule.section_path and text:
        return f"{rule.section_path}. {text}"
    return text


async def _collect_unit_rule_context(
    s: AsyncSession,
    unit_key: str,
) -> tuple[str, list[str], str]:
    rules_v2 = (
        await s.execute(select(RuleI18nV2).where(RuleI18nV2.unit_key == unit_key))
    ).scalars().all()
    rules_v2 = sorted(rules_v2, key=lambda r: _section_sort_key(r.section_path))
    rule_texts: list[str] = []
    examples: list[str] = []
    for rule in rules_v2:
        text = _rule_text_from_v2(rule)
        if text:
            rule_texts.append(text)
        if rule.examples_json:
            try:
                ex = json.loads(rule.examples_json)
                if isinstance(ex, list):
                    examples.extend(str(x) for x in ex)
            except Exception:
                pass
    if not rule_texts:
        legacy = (
            await s.execute(select(RuleI18n).where(RuleI18n.unit_key == unit_key))
        ).scalar_one_or_none()
        if legacy:
            legacy_text = legacy.rule_text_en or legacy.rule_text_uk or ""
            if not legacy_text:
                legacy_text = legacy.rule_short_en or legacy.rule_short_uk or ""
            if legacy_text:
                rule_texts.append(legacy_text)
            if legacy.examples_json:
                try:
                    ex = json.loads(legacy.examples_json)
                    if isinstance(ex, list):
                        examples.extend(str(x) for x in ex)
                except Exception:
                    pass
    rule_text = "\n".join(rule_texts).strip()
    unit_topic_hint = ""
    if not rule_text:
        unit_topic_hint = f"Unit topic: {unit_key}. Generate only tasks for this unit topic."
    return rule_text, examples[:6], unit_topic_hint

def _validate_exercise(payload: Any) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("exercise must be an object")
    exercise_type = payload.get("exercise_type")
    if exercise_type == "free_text":
        exercise_type = "freetext"
    if exercise_type not in {"freetext", "mcq", "multiselect"}:
        raise ValueError("invalid exercise_type")
    instruction = payload.get("instruction")
    if not isinstance(instruction, str) or not instruction.strip():
        raise ValueError("instruction required")
    items = payload.get("items")
    if not isinstance(items, list) or len(items) < 2:
        raise ValueError("items list required")
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("item must be object")
        if not item.get("prompt") or not item.get("canonical"):
            raise ValueError("prompt/canonical required")
        acc = item.get("accepted_variants")
        if not isinstance(acc, list):
            raise ValueError("accepted_variants list required")
        if item.get("options") is not None:
            if not isinstance(item.get("options"), list) or not item.get("options"):
                raise ValueError("options list required when present")
        if exercise_type in {"mcq", "multiselect"} and not item.get("options"):
            raise ValueError("options required for mcq/multiselect")
    payload["exercise_type"] = exercise_type
    return payload

async def ensure_unit_exercise(
    s: AsyncSession,
    *,
    unit_key: str,
    exercise_index: int,
    llm_client: LLMClient | None,
    allow_generate: bool = True,
) -> UnitExercise | None:
    existing = (await s.execute(
        select(UnitExercise).where(UnitExercise.unit_key == unit_key, UnitExercise.exercise_index == exercise_index)
    )).scalar_one_or_none()
    if existing:
        return existing
    if llm_client is None or not allow_generate:
        return None

    rule_text, examples, unit_topic_hint = await _collect_unit_rule_context(s, unit_key)
    topic_lock = (
        "The generated exercise MUST practice ONLY the grammar point(s) from this unit. "
        "Do NOT introduce other tenses (e.g., past simple), modals, or unrelated structures."
    )
    raw = llm_client.generate_unit_exercise(
        unit_key=unit_key,
        exercise_index=exercise_index,
        rule_text=rule_text,
        examples=examples,
        topic_lock=topic_lock,
        unit_topic_hint=unit_topic_hint,
        extra_constraints=None,
    )
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception as exc:
        raise ValueError(f"Invalid JSON from LLM: {exc}") from exc
    payload = _validate_exercise(payload)
    forbidden_markers = _forbidden_markers_for_unit(unit_key, rule_text)
    if _contains_forbidden_markers(payload, forbidden_markers):
        raw = llm_client.generate_unit_exercise(
            unit_key=unit_key,
            exercise_index=exercise_index,
            rule_text=rule_text,
            examples=examples,
            topic_lock=topic_lock,
            unit_topic_hint=unit_topic_hint,
            extra_constraints=(
                "The previous output introduced forbidden tense markers. "
                f"Avoid using these words or phrases: {', '.join(forbidden_markers)}. "
                "Stay strictly within the unit grammar point."
            ),
        )
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except Exception as exc:
            raise ValueError(f"Invalid JSON from LLM: {exc}") from exc
        payload = _validate_exercise(payload)
        if _contains_forbidden_markers(payload, forbidden_markers):
            return None
    ex = UnitExercise(
        unit_key=unit_key,
        exercise_index=exercise_index,
        exercise_type=payload["exercise_type"],
        instruction=payload["instruction"],
        items_json=json.dumps(payload["items"], ensure_ascii=False),
    )
    s.add(ex)
    await s.commit()
    return ex
