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
        await conn.execute(
            text(
                "UPDATE user_state SET acceptance_mode='normal' "
                "WHERE acceptance_mode IS NULL OR acceptance_mode='';"
            )
        )
