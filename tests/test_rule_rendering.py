import asyncio
import json

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from bot.handlers import _render_rules_for_keys
from bot.models import Base, RuleI18nV2


async def _setup_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    return engine, Session


def test_rule_rendering_short_vs_full_and_examples_per_rule():
    async def _run():
        engine, Session = await _setup_session()
        async with Session() as s:
            rule_a = RuleI18nV2(
                rule_key="unit_1_A",
                unit_key="unit_1",
                section_path="A1",
                rule_text_en="Full A",
                rule_short_en="Short A",
                examples_json=json.dumps(["A ex1", "A ex2"]),
            )
            rule_b = RuleI18nV2(
                rule_key="unit_1_B",
                unit_key="unit_1",
                section_path="B2",
                rule_text_en="Full B",
                rule_short_en="Short B",
                examples_json=json.dumps(["B ex1", "B ex2"]),
            )
            s.add_all([rule_b, rule_a])
            await s.commit()

            short_msg = await _render_rules_for_keys(
                s,
                ["unit_1_B", "unit_1_A"],
                "en",
                prefer_short=True,
                max_examples_total=2,
            )
            assert "Short A" in short_msg
            assert "Short B" in short_msg
            assert "Full A" not in short_msg

            full_msg = await _render_rules_for_keys(
                s,
                ["unit_1_B", "unit_1_A"],
                "en",
                prefer_short=False,
                max_examples_total=2,
            )
            assert "Full A" in full_msg
            assert "Short A" not in full_msg

            per_rule_msg = await _render_rules_for_keys(
                s,
                ["unit_1_B", "unit_1_A"],
                "en",
                prefer_short=False,
                max_examples_total=4,
                examples_per_rule=1,
            )
            assert "A ex1" in per_rule_msg
            assert "B ex1" in per_rule_msg

        await engine.dispose()

    asyncio.run(_run())
