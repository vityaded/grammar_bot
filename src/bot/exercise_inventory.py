from __future__ import annotations

import json
import logging
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import UnitExercise

logger = logging.getLogger(__name__)

_DATASET_MAX_EXERCISES_BY_UNIT: dict[str, int] | None = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_dataset_max_exercises() -> dict[str, int]:
    global _DATASET_MAX_EXERCISES_BY_UNIT
    if _DATASET_MAX_EXERCISES_BY_UNIT is not None:
        return _DATASET_MAX_EXERCISES_BY_UNIT
    mapping: dict[str, int] = {}
    for name in ("unit_exercises_v2.json", "unit_exercises.json"):
        path = _repo_root() / "data" / name
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            exercises = data.get("exercises", [])
            for ex in exercises:
                unit_key = ex.get("unit_key")
                idx = ex.get("exercise_index")
                if not unit_key or not isinstance(idx, int):
                    continue
                mapping[unit_key] = max(mapping.get(unit_key, 0), idx)
        except Exception as exc:
            logger.warning("failed_to_load_dataset_exercises path=%s err=%s", path, exc)
        if mapping:
            break
    _DATASET_MAX_EXERCISES_BY_UNIT = mapping
    return mapping


def dataset_max_exercises_for_unit(unit_key: str) -> int | None:
    if not unit_key:
        return None
    return _load_dataset_max_exercises().get(unit_key)


async def unit_max_real_exercise_index(
    s: AsyncSession,
    unit_key: str,
    *,
    clamp_max: int = 6,
) -> int:
    if not unit_key:
        return 0
    rows = (
        await s.execute(
            select(UnitExercise.exercise_index).where(UnitExercise.unit_key == unit_key)
        )
    ).scalars().all()
    existing = {int(x) for x in rows if x}
    if not existing:
        return 0
    dataset_max = dataset_max_exercises_for_unit(unit_key)
    if dataset_max:
        return int(dataset_max)
    return min(max(existing), clamp_max)


async def unit_real_exercise_indices(
    s: AsyncSession,
    unit_key: str,
    *,
    clamp_max: int = 6,
) -> list[int]:
    if not unit_key:
        return []
    rows = (
        await s.execute(
            select(UnitExercise.exercise_index).where(UnitExercise.unit_key == unit_key)
        )
    ).scalars().all()
    existing = sorted({int(x) for x in rows if x})
    if not existing:
        return []
    dataset_max = dataset_max_exercises_for_unit(unit_key)
    if dataset_max:
        max_real = int(dataset_max)
    else:
        max_real = min(max(existing), clamp_max)
    return [idx for idx in existing if idx <= max_real]
