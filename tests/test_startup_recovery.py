import asyncio
import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from bot.config import Settings
from bot.handlers import resume_stuck_users_on_startup
from bot.models import Base, Attempt, PlacementItem, User, UserState

class FakeBot:
    def __init__(self, fail_ids: set[int] | None = None):
        self.sent: list[tuple[int, str, dict]] = []
        self.fail_ids = fail_ids or set()

    async def send_message(self, chat_id: int, text: str, **kwargs):
        if chat_id in self.fail_ids:
            raise RuntimeError("send failed")
        self.sent.append((chat_id, text, kwargs))
        return None

async def _setup_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    return engine, Session

def _settings() -> Settings:
    return Settings(
        bot_token="token",
        admin_ids=[1],
        database_url="sqlite+aiosqlite:///:memory:",
        gemini_api_key=None,
        llm_model="noop",
        ui_default_lang="en",
        acceptance_mode="normal",
    )

def test_startup_recovery_await_next_advances():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            user = User(id=1, is_approved=True, ui_lang="en")
            st = UserState(
                tg_user_id=1,
                mode="await_next",
                acceptance_mode="normal",
                last_attempt_id=10,
                last_placement_order=0,
            )
            item = PlacementItem(
                id=1,
                order_index=1,
                unit_key="unit_1",
                prompt="Prompt?",
                item_type="freetext",
                canonical="answer",
                accepted_variants_json="[]",
            )
            attempt = Attempt(
                id=10,
                tg_user_id=1,
                mode="placement",
                placement_item_id=1,
                due_item_id=None,
                unit_key="unit_1",
                prompt="Prompt?",
                canonical="answer",
                user_answer_norm="answer",
                verdict="correct",
                rule_keys_json=None,
            )
            s.add_all([user, st, item, attempt])
            await s.commit()

        bot = FakeBot()
        await resume_stuck_users_on_startup(bot, settings=_settings(), sessionmaker=Session, throttle_s=0)

        async with Session() as s:
            refreshed = await s.get(UserState, 1)
            assert refreshed.mode == "placement"
            assert refreshed.pending_placement_item_id == 1
            assert refreshed.startup_recovered_at is not None
        assert len(bot.sent) == 1
        await engine.dispose()

    asyncio.run(_run())

def test_startup_recovery_missing_pending_item():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            user = User(id=2, is_approved=True, ui_lang="en")
            st = UserState(
                tg_user_id=2,
                mode="detour",
                acceptance_mode="normal",
                pending_due_item_id=999,
                last_placement_order=0,
            )
            item = PlacementItem(
                id=2,
                order_index=1,
                unit_key="unit_1",
                prompt="Prompt?",
                item_type="freetext",
                canonical="answer",
                accepted_variants_json="[]",
            )
            s.add_all([user, st, item])
            await s.commit()

        bot = FakeBot()
        await resume_stuck_users_on_startup(bot, settings=_settings(), sessionmaker=Session, throttle_s=0)

        async with Session() as s:
            refreshed = await s.get(UserState, 2)
            assert refreshed.mode == "placement"
            assert refreshed.pending_placement_item_id == 2
        assert len(bot.sent) == 1
        await engine.dispose()

    asyncio.run(_run())

def test_startup_recovery_idempotent():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            user = User(id=3, is_approved=True, ui_lang="en")
            st = UserState(
                tg_user_id=3,
                mode="await_next",
                acceptance_mode="normal",
                last_attempt_id=11,
                last_placement_order=0,
            )
            item = PlacementItem(
                id=3,
                order_index=1,
                unit_key="unit_1",
                prompt="Prompt?",
                item_type="freetext",
                canonical="answer",
                accepted_variants_json="[]",
            )
            attempt = Attempt(
                id=11,
                tg_user_id=3,
                mode="placement",
                placement_item_id=3,
                due_item_id=None,
                unit_key="unit_1",
                prompt="Prompt?",
                canonical="answer",
                user_answer_norm="answer",
                verdict="correct",
                rule_keys_json=None,
            )
            s.add_all([user, st, item, attempt])
            await s.commit()

        bot = FakeBot()
        await resume_stuck_users_on_startup(bot, settings=_settings(), sessionmaker=Session, throttle_s=0)
        await resume_stuck_users_on_startup(bot, settings=_settings(), sessionmaker=Session, throttle_s=0)
        assert len(bot.sent) == 1
        await engine.dispose()

    asyncio.run(_run())

def test_startup_recovery_multiple_users_throttles(monkeypatch):
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            user_ok = User(id=4, is_approved=True, ui_lang="en")
            user_fail = User(id=5, is_approved=True, ui_lang="en")
            st_ok = UserState(
                tg_user_id=4,
                mode="await_next",
                acceptance_mode="normal",
                last_attempt_id=12,
                last_placement_order=0,
            )
            st_fail = UserState(
                tg_user_id=5,
                mode="await_next",
                acceptance_mode="normal",
                last_attempt_id=13,
                last_placement_order=0,
            )
            item_ok = PlacementItem(
                id=4,
                order_index=1,
                unit_key="unit_1",
                prompt="Prompt?",
                item_type="freetext",
                canonical="answer",
                accepted_variants_json="[]",
            )
            attempt_ok = Attempt(
                id=12,
                tg_user_id=4,
                mode="placement",
                placement_item_id=4,
                due_item_id=None,
                unit_key="unit_1",
                prompt="Prompt?",
                canonical="answer",
                user_answer_norm="answer",
                verdict="correct",
                rule_keys_json=None,
            )
            attempt_fail = Attempt(
                id=13,
                tg_user_id=5,
                mode="placement",
                placement_item_id=4,
                due_item_id=None,
                unit_key="unit_1",
                prompt="Prompt?",
                canonical="answer",
                user_answer_norm="answer",
                verdict="correct",
                rule_keys_json=None,
            )
            s.add_all([user_ok, user_fail, st_ok, st_fail, item_ok, attempt_ok, attempt_fail])
            await s.commit()

        sleep_calls: list[float] = []

        async def fake_sleep(seconds: float):
            sleep_calls.append(seconds)

        monkeypatch.setattr("bot.handlers.asyncio.sleep", fake_sleep)

        bot = FakeBot(fail_ids={5})
        await resume_stuck_users_on_startup(
            bot,
            settings=_settings(),
            sessionmaker=Session,
            throttle_s=0.1,
        )

        async with Session() as s:
            ok_state = await s.get(UserState, 4)
            fail_state = await s.get(UserState, 5)
            assert ok_state.startup_recovered_at is not None
            assert fail_state.startup_recovered_at is None

        assert len(bot.sent) == 1
        assert len(sleep_calls) == 2
        await engine.dispose()

    asyncio.run(_run())
