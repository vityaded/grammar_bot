import re

import pytest

from src.bot.models import PlacementItem
from tests.telegram_harness.harness import BotHarness


@pytest.mark.asyncio
async def test_telegram_like_smoke(tmp_path):
    harness = await BotHarness.create(
        tmp_path,
        settings_overrides={"admin_ids": [999], "ui_default_lang": "uk"},
    )
    try:
        admin_id = 999
        user_id = 111

        await harness.send_text(user_id=admin_id, text="/admin")
        admin_message = harness.last_bot_message(admin_id)
        assert admin_message
        assert "Admin actions:" in (admin_message.text or "")

        admin_callbacks = harness.find_callback_data(admin_id)
        assert "admin_invite" in admin_callbacks

        await harness.click(
            from_user_id=admin_id,
            chat_id=admin_id,
            message=admin_message,
            data="admin_invite",
        )
        invite_message = harness.last_bot_message(admin_id)
        assert invite_message
        invite_text = (invite_message.text or "").replace("\\", "")
        token_match = re.search(r"INV_[A-Za-z0-9_\-]+", invite_text)
        assert token_match
        token = token_match.group(0)

        await harness.send_text(user_id=user_id, text=f"/start {token}")
        user_message = harness.last_bot_message(user_id)
        assert user_message
        lower_text = (user_message.text or "").lower()
        assert "access" in lower_text or "доступ" in lower_text

        admin_request = harness.last_bot_message(admin_id)
        assert admin_request
        approve_callbacks = harness.find_callback_data(
            admin_id,
            predicate=lambda value: value.startswith("admin_approve:"),
        )
        assert approve_callbacks
        approve_callback = approve_callbacks[0]

        await harness.click(
            from_user_id=admin_id,
            chat_id=admin_id,
            message=admin_request,
            data=approve_callback,
        )
        approved_message = harness.last_bot_message(user_id)
        assert approved_message

        await harness.click(
            from_user_id=user_id,
            chat_id=user_id,
            message=approved_message,
            data="lang:uk",
        )
        lang_message = harness.last_bot_message(user_id)
        assert lang_message
        assert "OK" in (lang_message.text or "")
        assert harness.find_callback_data(user_id, predicate=lambda v: v == "start_placement")

        async with harness.sessionmaker() as session:
            session.add(
                PlacementItem(
                    order_index=1,
                    unit_key="unit_1",
                    prompt="Type: I am here.",
                    item_type="freetext",
                    canonical="I am here.",
                    accepted_variants_json="[]",
                )
            )
            await session.commit()

        await harness.click(
            from_user_id=user_id,
            chat_id=user_id,
            message=lang_message,
            data="start_placement",
        )
        placement_message = harness.last_bot_message(user_id)
        assert placement_message
        assert (placement_message.text or "").strip()
    finally:
        await harness.close()
