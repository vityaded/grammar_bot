import asyncio
from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from tests.telegram_harness.session import RecordingSession


def test_recording_session_preserves_reply_markup() -> None:
    async def _run() -> None:
        session = RecordingSession()
        bot = Bot(token="123456:TEST", session=session)
        markup = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Approve", callback_data="admin_approve:1")]]
        )

        message = await bot.send_message(chat_id=999, text="Access request", reply_markup=markup)

        assert message.reply_markup is not None
        assert message.reply_markup.inline_keyboard[0][0].callback_data == "admin_approve:1"
        assert session.messages_by_chat[999][-1].reply_markup is not None
        assert (
            session.messages_by_chat[999][-1].reply_markup.inline_keyboard[0][0].callback_data
            == "admin_approve:1"
        )

    asyncio.run(_run())
