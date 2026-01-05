import asyncio
import datetime as dt
import json
import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from bot.models import Base, DueItem
from bot.due_flow import complete_due_without_exercise, ensure_detours_for_units
from bot.models import utcnow

async def _setup_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    return engine, Session

def test_due_completion_fallback():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            due = DueItem(
                tg_user_id=1,
                kind="detour",
                unit_key="unit_1",
                due_at=utcnow(),
                exercise_index=2,
                item_in_exercise=1,
                correct_in_exercise=0,
                batch_num=1,
                is_active=True,
            )
            s.add(due)
            await s.commit()
            follow = await complete_due_without_exercise(s, due=due)
            assert due.is_active is False
            assert follow is not None
            assert follow.kind == "revisit"
        await engine.dispose()
    asyncio.run(_run())

def test_detour_creation_from_study_units():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            created = await ensure_detours_for_units(
                s,
                tg_user_id=2,
                unit_keys=["unit_1", "unit_3"],
            )
            assert len(created) == 2
            created_again = await ensure_detours_for_units(
                s,
                tg_user_id=2,
                unit_keys=["unit_1", "unit_3"],
            )
            assert len(created_again) == 0
        await engine.dispose()
    asyncio.run(_run())

def test_detour_overrides_revisit_and_merges_keys():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            due = DueItem(
                tg_user_id=3,
                kind="revisit",
                unit_key="unit_1",
                due_at=utcnow() + dt.timedelta(days=3),
                exercise_index=4,
                item_in_exercise=2,
                correct_in_exercise=1,
                batch_num=2,
                is_active=True,
                cause_rule_keys_json=json.dumps(["unit_1_X"]),
            )
            s.add(due)
            await s.commit()
            now = utcnow().replace(tzinfo=None)
            await ensure_detours_for_units(
                s,
                tg_user_id=3,
                unit_keys=["unit_1"],
                cause_rule_keys_json=json.dumps(["unit_1_A"]),
            )
            await s.refresh(due)
            assert due.kind == "detour"
            assert due.due_at.replace(tzinfo=None) <= now + dt.timedelta(seconds=1)
            assert due.exercise_index == 1
            assert due.item_in_exercise == 1
            assert due.correct_in_exercise == 0
            assert due.batch_num == 1
            merged = json.loads(due.cause_rule_keys_json)
            assert set(merged) == {"unit_1_X", "unit_1_A"}
        await engine.dispose()
    asyncio.run(_run())

def test_detour_merges_cause_rule_keys():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            due = DueItem(
                tg_user_id=4,
                kind="detour",
                unit_key="unit_1",
                due_at=utcnow() + dt.timedelta(days=1),
                exercise_index=1,
                item_in_exercise=1,
                correct_in_exercise=0,
                batch_num=1,
                is_active=True,
                cause_rule_keys_json=json.dumps(["unit_1_A"]),
            )
            s.add(due)
            await s.commit()
            now = utcnow().replace(tzinfo=None)
            await ensure_detours_for_units(
                s,
                tg_user_id=4,
                unit_keys=["unit_1"],
                cause_rule_keys_json=json.dumps(["unit_1_A", "unit_1_B"]),
            )
            await s.refresh(due)
            assert due.kind == "detour"
            assert due.due_at.replace(tzinfo=None) <= now + dt.timedelta(seconds=1)
            merged = json.loads(due.cause_rule_keys_json)
            assert set(merged) == {"unit_1_A", "unit_1_B"}
            assert len(merged) == len(set(merged))
        await engine.dispose()
    asyncio.run(_run())
