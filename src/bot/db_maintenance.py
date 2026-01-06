from __future__ import annotations

import logging
from collections import defaultdict

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from .exercise_inventory import unit_max_real_exercise_index
from .models import UnitExercise

logger = logging.getLogger(__name__)


async def find_out_of_range_exercises(s: AsyncSession) -> dict[str, list[int]]:
    rows = (
        await s.execute(
            select(UnitExercise.unit_key, UnitExercise.exercise_index)
        )
    ).all()
    indices_by_unit: dict[str, list[int]] = defaultdict(list)
    for unit_key, idx in rows:
        if not unit_key or idx is None:
            continue
        indices_by_unit[unit_key].append(int(idx))

    out_of_range: dict[str, list[int]] = {}
    for unit_key, indices in indices_by_unit.items():
        max_real = await unit_max_real_exercise_index(s, unit_key)
        if max_real <= 0:
            continue
        over = sorted({idx for idx in indices if idx > max_real})
        if over:
            out_of_range[unit_key] = over
    return out_of_range


async def log_out_of_range_exercises(s: AsyncSession) -> None:
    out_of_range = await find_out_of_range_exercises(s)
    for unit_key, indices in sorted(out_of_range.items()):
        logger.warning(
            "unit_exercises_out_of_range unit_key=%s count=%s indices=%s",
            unit_key,
            len(indices),
            indices,
        )


async def purge_generated_exercises(s: AsyncSession) -> dict[str, int]:
    out_of_range = await find_out_of_range_exercises(s)
    deleted_by_unit: dict[str, int] = {}
    for unit_key, indices in out_of_range.items():
        if not indices:
            continue
        result = await s.execute(
            delete(UnitExercise).where(
                UnitExercise.unit_key == unit_key,
                UnitExercise.exercise_index.in_(indices),
            )
        )
        deleted_by_unit[unit_key] = result.rowcount or 0
    if deleted_by_unit:
        await s.commit()
    return deleted_by_unit
