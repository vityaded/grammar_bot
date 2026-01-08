from __future__ import annotations

import time
from typing import Any


def raw_message_update(
    *,
    update_id: int,
    user_id: int,
    chat_id: int,
    text: str,
    message_id: int,
) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": message_id,
            "date": int(time.time()),
            "chat": {"id": chat_id, "type": "private"},
            "from": {
                "id": user_id,
                "is_bot": False,
                "first_name": f"User {user_id}",
                "username": f"user{user_id}",
            },
            "text": text,
        },
    }


def raw_callback_update(
    *,
    update_id: int,
    from_user_id: int,
    chat_id: int,
    message_dict: dict[str, Any],
    data: str,
    callback_id: str = "1",
) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "callback_query": {
            "id": callback_id,
            "from": {
                "id": from_user_id,
                "is_bot": False,
                "first_name": f"User {from_user_id}",
                "username": f"user{from_user_id}",
            },
            "message": message_dict,
            "chat_instance": str(chat_id),
            "data": data,
        },
    }
