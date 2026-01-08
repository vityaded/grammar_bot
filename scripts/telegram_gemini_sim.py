from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select

from src.bot.autotest.solver import GeminiSolver
from src.bot.autotest.types import QuestionContext
from src.bot.models import PlacementItem, UserState
from tests.telegram_harness.harness import BotHarness


def _api_key() -> str | None:
    return os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")


def _parse_options(options_json: str | None) -> list[str]:
    if not options_json:
        return []
    try:
        return json.loads(options_json) or []
    except json.JSONDecodeError:
        return []


async def _seed_placement_item(harness: BotHarness) -> None:
    async with harness.sessionmaker() as session:
        existing = (await session.execute(select(PlacementItem).limit(1))).scalar_one_or_none()
        if existing:
            return
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


async def _ensure_access_flow(harness: BotHarness, *, admin_id: int, user_id: int) -> None:
    await harness.send_text(user_id=admin_id, text="/admin")
    admin_message = harness.last_bot_message(admin_id)
    if not admin_message:
        raise RuntimeError("Admin panel not opened")

    await harness.click(
        from_user_id=admin_id,
        chat_id=admin_id,
        message=admin_message,
        data="admin_invite",
    )
    invite_message = harness.last_bot_message(admin_id)
    if not invite_message or not invite_message.text:
        raise RuntimeError("Invite message missing")
    invite_text = invite_message.text.replace("\\", "")
    match = re.search(r"INV_[A-Za-z0-9_\-]+", invite_text)
    if not match:
        raise RuntimeError("Invite token missing")
    token = match.group(0)

    await harness.send_text(user_id=user_id, text=f"/start {token}")
    admin_request = harness.last_bot_message(admin_id)
    if not admin_request:
        raise RuntimeError("Admin access request missing")
    callbacks = harness.find_callback_data(
        admin_id,
        predicate=lambda value: value.startswith("admin_approve:"),
    )
    if not callbacks:
        raise RuntimeError("Approval callback missing")

    await harness.click(
        from_user_id=admin_id,
        chat_id=admin_id,
        message=admin_request,
        data=callbacks[0],
    )
    approved_message = harness.last_bot_message(user_id)
    if not approved_message:
        raise RuntimeError("User approval message missing")
    await harness.click(
        from_user_id=user_id,
        chat_id=user_id,
        message=approved_message,
        data="lang:uk",
    )


async def _get_pending_placement(harness: BotHarness, user_id: int) -> PlacementItem | None:
    async with harness.sessionmaker() as session:
        state = await session.get(UserState, user_id)
        if not state or state.mode != "placement" or not state.pending_placement_item_id:
            return None
        return await session.get(PlacementItem, state.pending_placement_item_id)


async def _get_acceptance_mode(harness: BotHarness, user_id: int) -> str:
    async with harness.sessionmaker() as session:
        state = await session.get(UserState, user_id)
        if state and state.acceptance_mode:
            return state.acceptance_mode
    return harness.settings.acceptance_mode


async def _send_solver_answer(
    harness: BotHarness,
    solver: GeminiSolver,
    user_id: int,
    item: PlacementItem,
    acceptance_mode: str,
) -> str:
    ctx = QuestionContext(
        unit_key=item.unit_key,
        exercise_index=0,
        item_idx=0,
        exercise_type=item.item_type,
        instruction=item.instruction or "",
        item={"prompt": item.prompt, "options": _parse_options(item.options_json)},
        question_key=f"placement:{item.id}",
        acceptance_mode=acceptance_mode,
    )
    answer = await solver.solve(ctx)
    await harness.send_text(user_id=user_id, text=answer.raw)
    return answer.raw


async def run_simulation(turns: int) -> None:
    api_key = _api_key()
    if not api_key:
        print("Missing GOOGLE_API_KEY or GEMINI_API_KEY. Exiting.")
        raise SystemExit(1)

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    log_path = reports_dir / f"telegram_gemini_sim_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"

    harness = await BotHarness.create(Path("/tmp"))
    try:
        admin_id = 999
        user_id = 111
        await _seed_placement_item(harness)
        await _ensure_access_flow(harness, admin_id=admin_id, user_id=user_id)

        solver = GeminiSolver(api_key=api_key, model=harness.settings.llm_model, timeout_sec=30)

        for turn in range(1, turns + 1):
            message = harness.last_bot_message(user_id)
            buttons = harness.find_callback_data(user_id)
            log_entry: dict[str, Any] = {
                "turn": turn,
                "bot_message": message.text if message else None,
                "buttons": buttons,
            }

            if buttons:
                if "lang:uk" in buttons:
                    choice = "lang:uk"
                elif "start_placement" in buttons:
                    choice = "start_placement"
                else:
                    choice = buttons[0]
                await harness.click(
                    from_user_id=user_id,
                    chat_id=user_id,
                    message=message,
                    data=choice,
                )
                log_entry["user_action"] = {"type": "click", "value": choice}
            else:
                pending_item = await _get_pending_placement(harness, user_id)
                if pending_item:
                    acceptance_mode = await _get_acceptance_mode(harness, user_id)
                    answer = await _send_solver_answer(
                        harness,
                        solver,
                        user_id,
                        pending_item,
                        acceptance_mode,
                    )
                    log_entry["user_action"] = {"type": "text", "value": answer}
                else:
                    log_entry["user_action"] = {"type": "noop"}

            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    finally:
        await harness.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--turns", type=int, default=20)
    args = parser.parse_args()
    asyncio.run(run_simulation(args.turns))


if __name__ == "__main__":
    main()
