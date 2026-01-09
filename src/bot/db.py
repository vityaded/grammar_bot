from __future__ import annotations
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from .config import Settings

class Base(DeclarativeBase):
    pass

def make_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(settings.database_url, echo=False, future=True)

def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def ensure_sqlite_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        result = await conn.execute(text("PRAGMA table_info(placement_items);"))
        columns = {row[1] for row in result.fetchall()}
        if "study_units_json" not in columns:
            await conn.execute(
                text("ALTER TABLE placement_items ADD COLUMN study_units_json TEXT;")
            )
        result = await conn.execute(text("PRAGMA table_info(user_state);"))
        columns = {row[1] for row in result.fetchall()}
        if "acceptance_mode" not in columns:
            await conn.execute(
                text("ALTER TABLE user_state ADD COLUMN acceptance_mode TEXT DEFAULT 'normal';")
            )
        if "startup_recovered_at" not in columns:
            await conn.execute(
                text("ALTER TABLE user_state ADD COLUMN startup_recovered_at DATETIME;")
            )
        result = await conn.execute(text("PRAGMA table_info(due_items);"))
        columns = {row[1] for row in result.fetchall()}
        if "exercise_attempts" not in columns:
            await conn.execute(
                text("ALTER TABLE due_items ADD COLUMN exercise_attempts INTEGER DEFAULT 0;")
            )
        if "exercise_hard_mode" not in columns:
            await conn.execute(
                text("ALTER TABLE due_items ADD COLUMN exercise_hard_mode BOOLEAN DEFAULT 0;")
            )
        await conn.execute(
            text(
                "UPDATE user_state SET acceptance_mode='normal' "
                "WHERE acceptance_mode IS NULL OR acceptance_mode='';"
            )
        )
        await conn.execute(
            text(
                "UPDATE due_items SET exercise_attempts=0 "
                "WHERE exercise_attempts IS NULL;"
            )
        )
        await conn.execute(
            text(
                "UPDATE due_items SET exercise_hard_mode=0 "
                "WHERE exercise_hard_mode IS NULL;"
            )
        )
