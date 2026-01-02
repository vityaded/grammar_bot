from __future__ import annotations
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from .config import Settings

class Base(DeclarativeBase):
    pass

def make_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(settings.database_url, echo=False, future=True)

def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
