from __future__ import annotations

import datetime as dt
from typing import Any, AsyncGenerator

from aiogram import Bot
from aiogram.client.session.base import BaseSession
from aiogram.methods import (
    AnswerCallbackQuery,
    EditMessageReplyMarkup,
    EditMessageText,
    SendMessage,
)
from aiogram.types import Chat, InlineKeyboardMarkup, Message, User


class RecordingSession(BaseSession):
    def __init__(self) -> None:
        super().__init__()
        self.calls: list[dict[str, Any]] = []
        self.messages_by_chat: dict[int, list[Message]] = {}
        self._message_id_counter: dict[int, int] = {}

    async def close(self) -> None:
        return None

    async def stream_content(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        timeout: int = 30,
        chunk_size: int = 65536,
        raise_for_status: bool = True,
    ) -> AsyncGenerator[bytes, None]:
        if False:
            yield b""
        return

    def _record_call(self, method_name: str, payload: dict[str, Any], chat_id: int | None) -> None:
        self.calls.append(
            {
                "method": method_name,
                "payload": payload,
                "chat_id": chat_id,
                "timestamp": dt.datetime.now(tz=dt.timezone.utc),
            }
        )

    def _bot_user(self, bot: Bot) -> User:
        return User.model_validate(
            {
                "id": 0,
                "is_bot": True,
                "first_name": "Test Bot",
                "username": "test_bot",
            }
        )

    def _next_message_id(self, chat_id: int) -> int:
        current = self._message_id_counter.get(chat_id, 0) + 1
        self._message_id_counter[chat_id] = current
        return current

    def _build_message(self, bot: Bot, chat_id: int, text: str | None, reply_markup: Any) -> Message:
        reply_markup = self._normalize_reply_markup(reply_markup)
        message = Message.model_validate(
            {
                "message_id": self._next_message_id(chat_id),
                "date": dt.datetime.now(tz=dt.timezone.utc),
                "chat": Chat.model_validate({"id": chat_id, "type": "private"}),
                "from": self._bot_user(bot).model_dump(by_alias=True),
                "text": text,
                "reply_markup": reply_markup,
            }
        )
        self.messages_by_chat.setdefault(chat_id, []).append(message)
        return message

    def _normalize_reply_markup(self, reply_markup: Any) -> Any:
        if reply_markup is None:
            return None
        if isinstance(reply_markup, InlineKeyboardMarkup):
            return reply_markup
        if isinstance(reply_markup, dict):
            return InlineKeyboardMarkup.model_validate(reply_markup)
        return reply_markup

    def _find_message(self, chat_id: int, message_id: int | None = None) -> Message | None:
        messages = self.messages_by_chat.get(chat_id, [])
        if not messages:
            return None
        if message_id is None:
            return messages[-1]
        for message in messages:
            if message.message_id == message_id:
                return message
        return messages[-1]

    async def make_request(self, bot: Bot, method: Any, timeout: int | None = None) -> Any:
        payload = method.model_dump(exclude_none=True)
        chat_id = payload.get("chat_id")
        method_name = method.__class__.__name__
        self._record_call(method_name, payload, chat_id)

        if isinstance(method, SendMessage):
            if chat_id is None:
                return True
            return self._build_message(bot, int(chat_id), payload.get("text"), payload.get("reply_markup"))

        if isinstance(method, EditMessageText):
            if chat_id is None:
                return True
            message = self._find_message(int(chat_id), payload.get("message_id"))
            if not message:
                return True
            message.text = payload.get("text") or message.text
            if payload.get("reply_markup") is not None:
                message.reply_markup = self._normalize_reply_markup(payload.get("reply_markup"))
            return message

        if isinstance(method, EditMessageReplyMarkup):
            if chat_id is None:
                return True
            message = self._find_message(int(chat_id), payload.get("message_id"))
            if not message:
                return True
            if payload.get("reply_markup") is not None:
                message.reply_markup = self._normalize_reply_markup(payload.get("reply_markup"))
            return message

        if isinstance(method, AnswerCallbackQuery):
            return True

        return True
