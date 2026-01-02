import asyncio
from pathlib import Path
from sqlalchemy import text
from .config import load_settings
from .db import make_engine
from .models import Base

async def main():
    settings = load_settings()
    if settings.database_url.startswith("sqlite"):
        Path("./data").mkdir(parents=True, exist_ok=True)

    engine = make_engine(settings)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        if settings.database_url.startswith("sqlite"):
            await conn.execute(text("PRAGMA journal_mode=WAL;"))
            await conn.execute(text("PRAGMA synchronous=NORMAL;"))
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
