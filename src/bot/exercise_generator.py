from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .llm import LLMClient
from .models import RuleI18n, UnitExercise

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
) -> UnitExercise | None:
    existing = (await s.execute(
        select(UnitExercise).where(UnitExercise.unit_key == unit_key, UnitExercise.exercise_index == exercise_index)
    )).scalar_one_or_none()
    if existing:
        return existing
    if llm_client is None:
        return None

    rule = (await s.execute(select(RuleI18n).where(RuleI18n.unit_key == unit_key))).scalar_one_or_none()
    rule_text = ""
    examples: list[str] = []
    if rule:
        rule_text = rule.rule_text_en or rule.rule_text_uk or ""
        try:
            if rule.examples_json:
                ex = json.loads(rule.examples_json)
                if isinstance(ex, list):
                    examples = [str(x) for x in ex]
        except Exception:
            examples = []

    raw = llm_client.generate_unit_exercise(
        unit_key=unit_key,
        exercise_index=exercise_index,
        rule_text=rule_text,
        examples=examples,
    )
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception as exc:
        raise ValueError(f"Invalid JSON from LLM: {exc}") from exc
    payload = _validate_exercise(payload)
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
