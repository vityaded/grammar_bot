from __future__ import annotations

import datetime as dt
import json
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import DueItem, utcnow

def _parse_rule_keys(raw: object | None) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if x]
    if isinstance(raw, str):
        try:
            val = json.loads(raw)
            if isinstance(val, list):
                return [str(x) for x in val if x]
        except Exception:
            return []
    return []

def _rule_keys_json(rule_keys: list[str]) -> str | None:
    if not rule_keys:
        return None
    return json.dumps(rule_keys, ensure_ascii=False)

def _merge_rule_keys(existing_json: str | None, new_keys: list[str]) -> str | None:
    existing = _parse_rule_keys(existing_json)
    if not existing and not new_keys:
        return None
    seen: set[str] = set()
    merged: list[str] = []
    for key in existing + new_keys:
        if key and key not in seen:
            seen.add(key)
            merged.append(key)
    return _rule_keys_json(merged)

async def ensure_detours_for_units(
    s: AsyncSession,
    *,
    tg_user_id: int,
    unit_keys: list[str],
    cause_rule_keys_json: str | None = None,
) -> list[DueItem]:
    if not unit_keys:
        return []
    unique_units = sorted({u for u in unit_keys if u})
    existing_rows = (await s.execute(
        select(DueItem).where(
            DueItem.tg_user_id == tg_user_id,
            DueItem.is_active == True,
            DueItem.kind.in_(["detour", "revisit", "check"]),
            DueItem.unit_key.in_(unique_units),
        )
    )).scalars().all()
    existing_by_unit: dict[str, DueItem] = {}
    for row in existing_rows:
        existing_by_unit.setdefault(row.unit_key, row)
    created: list[DueItem] = []
    changed = False
    incoming_keys = _parse_rule_keys(cause_rule_keys_json)
    now = utcnow()
    for unit_key in unique_units:
        unit_cause_keys = [k for k in incoming_keys if k.startswith(f"{unit_key}_")]
        if not unit_cause_keys:
            unit_cause_keys = list(incoming_keys)
        unit_cause_json = _rule_keys_json(unit_cause_keys)
        existing = existing_by_unit.get(unit_key)
        if not existing:
            di = DueItem(
                tg_user_id=tg_user_id,
                kind="detour",
                unit_key=unit_key,
                due_at=now,
                exercise_index=1,
                item_in_exercise=1,
                correct_in_exercise=0,
                batch_num=1,
                is_active=True,
                cause_rule_keys_json=unit_cause_json,
            )
            s.add(di)
            created.append(di)
            changed = True
            continue

        merged_json = _merge_rule_keys(existing.cause_rule_keys_json, unit_cause_keys)
        if existing.kind == "detour":
            if existing.due_at > now:
                existing.due_at = now
                changed = True
            if merged_json != existing.cause_rule_keys_json:
                existing.cause_rule_keys_json = merged_json
                changed = True
            continue

        existing.kind = "detour"
        existing.due_at = now
        existing.exercise_index = 1
        existing.item_in_exercise = 1
        existing.correct_in_exercise = 0
        existing.batch_num = 1
        existing.is_active = True
        existing.cause_rule_keys_json = merged_json
        changed = True
    if changed:
        await s.commit()
    return created

async def complete_due_without_exercise(
    s: AsyncSession,
    *,
    due: DueItem,
    now: dt.datetime | None = None,
) -> DueItem | None:
    now = now or utcnow()
    due.is_active = False
    follow: DueItem | None = None
    if due.kind == "detour":
        follow = DueItem(
            tg_user_id=due.tg_user_id,
            kind="revisit",
            unit_key=due.unit_key,
            due_at=now + dt.timedelta(days=2),
            exercise_index=1,
            item_in_exercise=1,
            correct_in_exercise=0,
            batch_num=1,
            is_active=True,
            cause_rule_keys_json=due.cause_rule_keys_json,
        )
        s.add(follow)
    elif due.kind == "revisit":
        follow = DueItem(
            tg_user_id=due.tg_user_id,
            kind="check",
            unit_key=due.unit_key,
            due_at=now + dt.timedelta(days=7),
            exercise_index=1,
            item_in_exercise=1,
            correct_in_exercise=0,
            batch_num=1,
            is_active=True,
            cause_rule_keys_json=due.cause_rule_keys_json,
        )
        s.add(follow)
    await s.commit()
    return follow
