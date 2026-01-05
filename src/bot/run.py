import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from .config import load_settings
from .db import ensure_sqlite_schema, make_engine, make_sessionmaker
from .handlers import register_handlers

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = load_settings()
    engine = make_engine(settings)
    if settings.database_url.startswith("sqlite"):
        await ensure_sqlite_schema(engine)
    sessionmaker = make_sessionmaker(engine)

    bot = Bot(
        settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2),
    )
    dp = Dispatcher()

    register_handlers(dp, settings=settings, sessionmaker=sessionmaker)

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
