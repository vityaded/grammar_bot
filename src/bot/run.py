import asyncio
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from .config import load_settings
from .db import make_engine, make_sessionmaker
from .handlers import register_handlers

async def main():
    settings = load_settings()
    engine = make_engine(settings)
    sessionmaker = make_sessionmaker(engine)

    bot = Bot(settings.bot_token, parse_mode=ParseMode.MARKDOWN_V2)
    dp = Dispatcher()

    register_handlers(dp, settings=settings, sessionmaker=sessionmaker)

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
