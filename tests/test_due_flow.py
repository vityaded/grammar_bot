import asyncio
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
