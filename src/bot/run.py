import asyncio
import logging
import traceback
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from .config import load_settings
from .db import ensure_sqlite_schema, make_engine, make_sessionmaker
from .db_maintenance import log_out_of_range_exercises
from .handlers import register_handlers

async def _notify_admins(bot: Bot, admin_ids: list[int], message: str) -> None:
    chunk_size = 4000
    chunks = [message[i : i + chunk_size] for i in range(0, len(message), chunk_size)] or [message]
    for admin_id in admin_ids:
        for chunk in chunks:
            await bot.send_message(admin_id, chunk, parse_mode=None)

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = load_settings()
    bot = Bot(
        settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2),
    )
    try:
        engine = make_engine(settings)
        if settings.database_url.startswith("sqlite"):
            await ensure_sqlite_schema(engine)
        sessionmaker = make_sessionmaker(engine)
        async with sessionmaker() as s:
            await log_out_of_range_exercises(s)

        dp = Dispatcher()
        register_handlers(dp, settings=settings, sessionmaker=sessionmaker)

        await dp.start_polling(bot)
    except Exception:
        error_text = traceback.format_exc()
        logging.getLogger(__name__).exception("bot_run_failed")
        try:
            await _notify_admins(
                bot,
                settings.admin_ids,
                f"Bot error detected:\n\n{error_text}",
            )
        except Exception:
            logging.getLogger(__name__).exception("failed_to_notify_admins")
        raise
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
