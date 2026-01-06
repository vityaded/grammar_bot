import asyncio
import json

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from bot import handlers
from bot.models import Base, DueItem, UnitExercise, utcnow


def _make_items(count: int) -> str:
    items = []
    for idx in range(count):
        items.append(
            {
                "prompt": f"Q{idx + 1}",
                "canonical": "A",
                "accepted": ["A"],
                "options": ["A", "B"],
            }
        )
    return json.dumps(items)


async def _setup_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    return engine, Session


async def _seed_unit(s, unit_key: str, exercises: int):
    items_json = _make_items(5)
    for idx in range(1, exercises + 1):
        s.add(
            UnitExercise(
                unit_key=unit_key,
                exercise_index=idx,
                exercise_type="mcq",
                instruction="Pick the right answer.",
                items_json=items_json,
            )
        )
    await s.commit()


def test_detour_max_length_and_followup():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            await _seed_unit(s, "unit_test_1", 4)
            due = DueItem(
                tg_user_id=1,
                kind="detour",
                unit_key="unit_test_1",
                due_at=utcnow(),
                exercise_index=1,
                item_in_exercise=1,
                correct_in_exercise=0,
                batch_num=1,
                is_active=True,
            )
            s.add(due)
            await s.commit()
            await s.refresh(due)

            total_items = 0
            max_exercise_index = 0
            while due.is_active:
                ex, it, _ = await handlers._due_current_item(s, due, llm=None)
                assert ex is not None and it is not None
                total_items += 1
                max_exercise_index = max(max_exercise_index, ex.exercise_index)
                completed = await handlers._advance_due_detour_revisit(
                    s, due, effective_correct=True, llm=None
                )
                if completed:
                    due.is_active = False
                    follow = handlers._create_follow_due(due)
                    if follow:
                        s.add(follow)
                    await s.commit()

            assert total_items <= 8
            assert max_exercise_index <= 4
            follow = (
                await s.execute(
                    select(DueItem).where(
                        DueItem.tg_user_id == 1, DueItem.kind == "revisit"
                    )
                )
            ).scalar_one_or_none()
            assert follow is not None
        await engine.dispose()

    asyncio.run(_run())


def test_revisit_max_length_and_followup():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            await _seed_unit(s, "unit_test_2", 4)
            due = DueItem(
                tg_user_id=2,
                kind="revisit",
                unit_key="unit_test_2",
                due_at=utcnow(),
                exercise_index=1,
                item_in_exercise=1,
                correct_in_exercise=0,
                batch_num=1,
                is_active=True,
            )
            s.add(due)
            await s.commit()
            await s.refresh(due)

            total_items = 0
            while due.is_active:
                ex, it, _ = await handlers._due_current_item(s, due, llm=None)
                assert ex is not None and it is not None
                total_items += 1
                completed = await handlers._advance_due_detour_revisit(
                    s, due, effective_correct=True, llm=None
                )
                if completed:
                    due.is_active = False
                    follow = handlers._create_follow_due(due)
                    if follow:
                        s.add(follow)
                    await s.commit()

            assert total_items <= 4
            follow = (
                await s.execute(
                    select(DueItem).where(
                        DueItem.tg_user_id == 2, DueItem.kind == "check"
                    )
                )
            ).scalar_one_or_none()
            assert follow is not None
        await engine.dispose()

    asyncio.run(_run())


def test_select_real_exercises_deterministic():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            await _seed_unit(s, "unit_test_3", 4)
            due_a = DueItem(
                tg_user_id=3,
                kind="detour",
                unit_key="unit_test_3",
                due_at=utcnow(),
                exercise_index=1,
                item_in_exercise=1,
                correct_in_exercise=0,
                batch_num=1,
                is_active=True,
            )
            due_b = DueItem(
                tg_user_id=3,
                kind="detour",
                unit_key="unit_test_3",
                due_at=utcnow(),
                exercise_index=1,
                item_in_exercise=1,
                correct_in_exercise=0,
                batch_num=1,
                is_active=True,
            )
            s.add_all([due_a, due_b])
            await s.commit()
            await s.refresh(due_a)
            await s.refresh(due_b)

            first = await handlers._select_real_exercises_for_due(s, due_a, 4)
            second = await handlers._select_real_exercises_for_due(s, due_a, 4)
            third = await handlers._select_real_exercises_for_due(s, due_b, 4)

            assert first == second
            assert first != third
        await engine.dispose()

    asyncio.run(_run())
