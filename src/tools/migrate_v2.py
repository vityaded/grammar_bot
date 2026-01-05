import asyncio
from sqlalchemy import text
from src.bot.config import load_settings
from src.bot.db import make_engine


async def _ensure_sqlite(engine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS rules_i18n_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_key VARCHAR(64) UNIQUE,
                unit_key VARCHAR(64),
                section_path VARCHAR(32),
                title_en TEXT,
                rule_text_en TEXT,
                rule_text_uk TEXT,
                rule_short_en TEXT,
                rule_short_uk TEXT,
                examples_json TEXT
            )
            """
        ))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_rules_i18n_v2_unit_key ON rules_i18n_v2 (unit_key)"))

        res = await conn.execute(text("PRAGMA table_info(attempts)"))
        cols = {row[1] for row in res.fetchall()}
        if "rule_keys_json" not in cols:
            await conn.execute(text("ALTER TABLE attempts ADD COLUMN rule_keys_json TEXT"))

        res = await conn.execute(text("PRAGMA table_info(due_items)"))
        cols = {row[1] for row in res.fetchall()}
        if "cause_rule_keys_json" not in cols:
            await conn.execute(text("ALTER TABLE due_items ADD COLUMN cause_rule_keys_json TEXT"))


async def _ensure_postgres(engine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS rules_i18n_v2 (
                id SERIAL PRIMARY KEY,
                rule_key VARCHAR(64) UNIQUE,
                unit_key VARCHAR(64),
                section_path VARCHAR(32),
                title_en TEXT,
                rule_text_en TEXT,
                rule_text_uk TEXT,
                rule_short_en TEXT,
                rule_short_uk TEXT,
                examples_json TEXT
            )
            """
        ))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_rules_i18n_v2_unit_key ON rules_i18n_v2 (unit_key)"))
        await conn.execute(text("ALTER TABLE attempts ADD COLUMN IF NOT EXISTS rule_keys_json TEXT"))
        await conn.execute(text("ALTER TABLE due_items ADD COLUMN IF NOT EXISTS cause_rule_keys_json TEXT"))


async def main() -> None:
    settings = load_settings()
    engine = make_engine(settings)
    if engine.url.get_backend_name().startswith("sqlite"):
        await _ensure_sqlite(engine)
    else:
        await _ensure_postgres(engine)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
