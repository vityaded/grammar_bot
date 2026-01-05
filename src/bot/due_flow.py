from __future__ import annotations

import datetime as dt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import DueItem, utcnow

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
        select(DueItem.unit_key).where(
            DueItem.tg_user_id == tg_user_id,
            DueItem.is_active == True,
            DueItem.kind.in_(["detour", "revisit", "check"]),
            DueItem.unit_key.in_(unique_units),
        )
    )).scalars().all()
    existing_units = set(existing_rows)
    created: list[DueItem] = []
    for unit_key in unique_units:
        if unit_key in existing_units:
            continue
        di = DueItem(
            tg_user_id=tg_user_id,
            kind="detour",
            unit_key=unit_key,
            due_at=utcnow(),
            exercise_index=1,
            item_in_exercise=1,
            correct_in_exercise=0,
            batch_num=1,
            is_active=True,
            cause_rule_keys_json=cause_rule_keys_json,
        )
        s.add(di)
        created.append(di)
    if created:
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
