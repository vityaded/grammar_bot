import asyncio
import json

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from bot import handlers
from bot.models import Base, DueItem, RuleI18nV2, UnitExercise, utcnow


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


async def _seed_unit(s, unit_key: str, exercises: list[int]):
    items_json = _make_items(5)
    for idx in exercises:
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


def test_detour_never_requests_out_of_range(monkeypatch):
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            await _seed_unit(s, "unit_test_detour", [1, 2, 3, 4])
            due = DueItem(
                tg_user_id=1,
                kind="detour",
                unit_key="unit_test_detour",
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

            seen: list[int] = []

            async def _ensure_wrapper(*args, **kwargs):
                exercise_index = kwargs["exercise_index"]
                seen.append(exercise_index)
                return (
                    await s.execute(
                        select(UnitExercise).where(
                            UnitExercise.unit_key == kwargs["unit_key"],
                            UnitExercise.exercise_index == exercise_index,
                        )
                    )
                ).scalar_one_or_none()

            monkeypatch.setattr(handlers, "ensure_unit_exercise", _ensure_wrapper)

            while due.is_active:
                ex, it, _ = await handlers._due_current_item(s, due, llm=None)
                assert ex is not None and it is not None
                completed = await handlers._advance_due_detour_revisit(
                    s, due, effective_correct=True, llm=None
                )
                if completed:
                    due.is_active = False
                    await s.commit()

            assert set(seen).issubset({1, 2, 3, 4})
        await engine.dispose()

    asyncio.run(_run())


def test_detour_ignores_out_of_range_exercises():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            await _seed_unit(s, "unit_test_ignore", [1, 2, 3, 4, 11])
            due = DueItem(
                tg_user_id=2,
                kind="detour",
                unit_key="unit_test_ignore",
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

            selected = await handlers._select_real_exercises_for_due(s, due, 4)

            assert 11 not in selected
            assert set(selected).issubset({1, 2, 3, 4})
        await engine.dispose()

    asyncio.run(_run())


def test_llm_generation_only_when_no_real_exercises():
    class FakeLLM:
        def __init__(self):
            self.calls = []

        def generate_unit_exercise(
            self,
            *,
            unit_key: str,
            exercise_index: int,
            rule_text: str,
            examples: list[str],
            topic_lock: str,
            unit_topic_hint: str,
            extra_constraints: str | None = None,
        ) -> str:
            self.calls.append(
                {
                    "unit_key": unit_key,
                    "exercise_index": exercise_index,
                    "rule_text": rule_text,
                    "examples": examples,
                    "topic_lock": topic_lock,
                    "unit_topic_hint": unit_topic_hint,
                    "extra_constraints": extra_constraints,
                }
            )
            payload = {
                "exercise_type": "freetext",
                "instruction": "Fill in the blanks.",
                "items": [
                    {
                        "prompt": "I ____ to school every day.",
                        "canonical": "go",
                        "accepted_variants": ["go"],
                    },
                    {
                        "prompt": "She ____ coffee in the morning.",
                        "canonical": "drinks",
                        "accepted_variants": ["drinks"],
                    },
                ],
            }
            return json.dumps(payload)

    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            rule = RuleI18nV2(
                unit_key="unit_test_llm",
                rule_key="unit_test_llm_A",
                section_path="A1",
                title_en="Present Simple",
                rule_text_en="Use present simple for habits and routines.",
                examples_json=json.dumps(["I go to school.", "She works late."]),
            )
            s.add(rule)
            await s.commit()

            due = DueItem(
                tg_user_id=3,
                kind="detour",
                unit_key="unit_test_llm",
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

            llm = FakeLLM()
            ex, it, _ = await handlers._due_current_item(s, due, llm=llm)

            assert ex is not None and it is not None
            assert llm.calls
            call = llm.calls[0]
            assert call["exercise_index"] in {1, 2}
            assert "habits and routines" in call["rule_text"]
            assert "MUST practice ONLY" in call["topic_lock"]
        await engine.dispose()

    asyncio.run(_run())
