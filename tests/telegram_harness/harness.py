from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Callable

from aiogram import Bot, Dispatcher
from aiogram.types import InlineKeyboardMarkup, Message
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.bot.config import Settings
from src.bot.db import Base
from src.bot.handlers import register_handlers

from .session import RecordingSession
from .updates import raw_callback_update, raw_message_update


class BotHarness:
    def __init__(
        self,
        *,
        bot: Bot,
        dispatcher: Dispatcher,
        engine,
        sessionmaker: async_sessionmaker,
        settings: Settings,
    ) -> None:
        self.bot = bot
        self.dispatcher = dispatcher
        self.engine = engine
        self.sessionmaker = sessionmaker
        self.settings = settings
        self._update_id = 0
        self._incoming_message_id: dict[int, int] = {}

    @classmethod
    async def create(
        cls,
        tmp_path: Path | None = None,
        *,
        sessionmaker: async_sessionmaker | None = None,
        settings: Settings | None = None,
        settings_overrides: dict[str, object] | None = None,
    ) -> "BotHarness":
        engine = None
        if sessionmaker is None:
            if tmp_path is None:
                raise ValueError("tmp_path is required when sessionmaker is not provided")
            database_url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
            settings = Settings(
                bot_token="123456:TEST",
                admin_ids=[999],
                database_url=database_url,
                gemini_api_key=None,
                llm_model="gemini-3-flash-preview",
                ui_default_lang="uk",
                acceptance_mode="normal",
            )
            if settings_overrides:
                settings = dataclasses.replace(settings, **settings_overrides)

            engine = create_async_engine(settings.database_url, echo=False)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        else:
            if settings is None:
                raise ValueError("settings are required when sessionmaker is provided")
            if settings_overrides:
                settings = dataclasses.replace(settings, **settings_overrides)
        dispatcher = Dispatcher()
        register_handlers(dispatcher, settings=settings, sessionmaker=sessionmaker)

        bot = Bot(token=settings.bot_token, session=RecordingSession())
        return cls(
            bot=bot,
            dispatcher=dispatcher,
            engine=engine,
            sessionmaker=sessionmaker,
            settings=settings,
        )

    async def close(self) -> None:
        await self.bot.session.close()
        if self.engine is not None:
            await self.engine.dispose()

    def _next_update_id(self) -> int:
        self._update_id += 1
        return self._update_id

    def _next_incoming_message_id(self, chat_id: int) -> int:
        current = self._incoming_message_id.get(chat_id, 0) + 1
        self._incoming_message_id[chat_id] = current
        return current

    async def send_text(self, *, user_id: int, chat_id: int | None = None, text: str) -> None:
        chat_id = chat_id or user_id
        update = raw_message_update(
            update_id=self._next_update_id(),
            user_id=user_id,
            chat_id=chat_id,
            text=text,
            message_id=self._next_incoming_message_id(chat_id),
        )
        await self.dispatcher.feed_raw_update(self.bot, update)

    async def click(self, *, from_user_id: int, chat_id: int, message: Message, data: str) -> None:
        message_dict = message.model_dump(by_alias=True, exclude_none=True)
        update = raw_callback_update(
            update_id=self._next_update_id(),
            from_user_id=from_user_id,
            chat_id=chat_id,
            message_dict=message_dict,
            data=data,
        )
        await self.dispatcher.feed_raw_update(self.bot, update)

    def last_bot_message(self, chat_id: int) -> Message | None:
        session = self.bot.session
        if isinstance(session, RecordingSession):
            messages = session.messages_by_chat.get(chat_id, [])
            if messages:
                return messages[-1]
        return None

    def find_callback_data(
        self,
        chat_id: int,
        predicate: Callable[[str], bool] | None = None,
    ) -> list[str]:
        session = self.bot.session
        if not isinstance(session, RecordingSession):
            return []
        messages = session.messages_by_chat.get(chat_id, [])
        for message in reversed(messages):
            if not message.reply_markup:
                continue
            if not isinstance(message.reply_markup, InlineKeyboardMarkup):
                continue
            results: list[str] = []
            for row in message.reply_markup.inline_keyboard:
                for button in row:
                    if button.callback_data is None:
                        continue
                    if predicate and not predicate(button.callback_data):
                        continue
                    results.append(button.callback_data)
            if results:
                return results
        return []

    def find_callbacks_matching(self, chat_id: int, prefix: str) -> list[str]:
        session = self.bot.session
        if not isinstance(session, RecordingSession):
            return []
        messages = session.messages_by_chat.get(chat_id, [])
        results: list[str] = []
        for message in reversed(messages):
            if not message.reply_markup:
                continue
            if not isinstance(message.reply_markup, InlineKeyboardMarkup):
                continue
            for row in message.reply_markup.inline_keyboard:
                for button in row:
                    if button.callback_data and button.callback_data.startswith(prefix):
                        results.append(button.callback_data)
        return results

    def find_message_with_callback(
        self,
        chat_id: int,
        predicate: Callable[[str], bool],
    ) -> Message | None:
        session = self.bot.session
        if not isinstance(session, RecordingSession):
            return None
        messages = session.messages_by_chat.get(chat_id, [])
        for message in reversed(messages):
            if not message.reply_markup:
                continue
            if not isinstance(message.reply_markup, InlineKeyboardMarkup):
                continue
            for row in message.reply_markup.inline_keyboard:
                for button in row:
                    if button.callback_data and predicate(button.callback_data):
                        return message
        return None
