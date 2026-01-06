import asyncio

from bot import handlers
from bot.models import DueItem, utcnow


def test_due_current_item_returns_three_tuple_on_value_error(monkeypatch):
    async def raise_value_error(*args, **kwargs):
        raise ValueError("boom")

    async def select_exercises(*args, **kwargs):
        return [1]

    monkeypatch.setattr(handlers, "_select_real_exercises_for_due", select_exercises)
    monkeypatch.setattr(handlers, "ensure_unit_exercise", raise_value_error)
    due = DueItem(
        tg_user_id=1,
        kind="detour",
        unit_key="unit_1",
        due_at=utcnow(),
        exercise_index=1,
        item_in_exercise=1,
    )

    result = asyncio.run(handlers._due_current_item(None, due, llm=None))

    assert result == (None, None, None)
    assert len(result) == 3


def test_due_current_item_returns_three_tuple_when_exercise_missing(monkeypatch):
    async def return_none(*args, **kwargs):
        return None

    async def select_exercises(*args, **kwargs):
        return [1]

    monkeypatch.setattr(handlers, "_select_real_exercises_for_due", select_exercises)
    monkeypatch.setattr(handlers, "ensure_unit_exercise", return_none)
    due = DueItem(
        tg_user_id=1,
        kind="revisit",
        unit_key="unit_2",
        due_at=utcnow(),
        exercise_index=1,
        item_in_exercise=1,
    )

    result = asyncio.run(handlers._due_current_item(None, due, llm=None))

    assert result == (None, None, None)
    assert len(result) == 3
