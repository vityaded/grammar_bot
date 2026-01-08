from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

warnings.filterwarnings(
    "ignore",
    message=r'.*Field "model_custom_emoji_id" has conflict with protected namespace.*',
    category=UserWarning,
)

from dotenv import load_dotenv
from sqlalchemy import select

from src.bot.autotest.solver import GeminiSolver
from src.bot.autotest.types import QuestionContext
from aiogram.types import Chat, Message, User

from src.bot.models import AccessRequest, PlacementItem, UserState
from tests.telegram_harness.harness import BotHarness
from tests.telegram_harness.session import RecordingSession


def _get_gemini_key() -> str | None:
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
    admin_request = harness.find_message_with_callback(
        admin_id,
        predicate=lambda value: value.startswith("admin_approve:"),
    )
    callbacks = harness.find_callbacks_matching(admin_id, "admin_approve:")
    approval_cb = callbacks[0] if callbacks else None
    if approval_cb and admin_request:
        await harness.click(
            from_user_id=admin_id,
            chat_id=admin_id,
            message=admin_request,
            data=approval_cb,
        )
    else:
        print("No approval button found in recorded admin messages", flush=True)
        callbacks_found = harness.find_callbacks_matching(admin_id, "")
        print(f"Found callbacks: {callbacks_found}", flush=True)
        async with harness.sessionmaker() as session:
            req = (
                await session.execute(
                    select(AccessRequest)
                    .where(AccessRequest.tg_user_id == user_id)
                    .order_by(AccessRequest.id.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
        if not req:
            session = harness.bot.session
            messages = []
            if isinstance(session, RecordingSession):
                messages = session.messages_by_chat.get(admin_id, [])[-5:]
            print(
                "Last admin messages:\n"
                + "\n".join([m.text or "<no text>" for m in messages]),
                flush=True,
            )
            print(f"All callback_data: {callbacks_found}", flush=True)
            raise RuntimeError("AccessRequest not created; cannot approve")
        approval_cb = f"admin_approve:{req.id}"
        print(f"Falling back to DB approval: {approval_cb}", flush=True)
        admin_request = harness.last_bot_message(admin_id)
        if not admin_request:
            admin_request = Message.model_validate(
                {
                    "message_id": 1,
                    "date": datetime.now(timezone.utc),
                    "chat": Chat.model_validate({"id": admin_id, "type": "private"}),
                    "from": User.model_validate(
                        {
                            "id": 0,
                            "is_bot": True,
                            "first_name": "Test Bot",
                            "username": "test_bot",
                        }
                    ).model_dump(by_alias=True),
                    "text": "Admin",
                }
            )
        await harness.click(
            from_user_id=admin_id,
            chat_id=admin_id,
            message=admin_request,
            data=approval_cb,
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


def _truncate(text: str | None, limit: int = 160) -> str:
    if not text:
        return ""
    clean = " ".join(text.split())
    if len(clean) <= limit:
        return clean
    return f"{clean[:limit - 3]}..."


def _log(message: str, *, enabled: bool) -> None:
    if not enabled:
        return
    print(message, flush=True)


async def run_simulation(*, turns: int, use_gemini: bool, verbose: bool, env_file: Path) -> None:
    api_key = _get_gemini_key() if use_gemini else None
    if use_gemini and not api_key:
        print(
            "Gemini is enabled but no API key found. Set GOOGLE_API_KEY in environment or .env",
            flush=True,
        )
        raise SystemExit(2)

    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    log_path = reports_dir / f"telegram_gemini_sim_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl"

    gemini_status = "enabled" if use_gemini else "disabled"
    _log(
        "\n".join(
            [
                "Telegram Gemini Simulation",
                f"Turns: {turns}",
                f"Gemini: {gemini_status}",
                f"Env file: {env_file}",
                f"Report: {log_path}",
            ]
        ),
        enabled=True,
    )

    harness = await BotHarness.create(Path("/tmp"))
    try:
        admin_id = 999
        user_id = 111
        await _seed_placement_item(harness)
        await _ensure_access_flow(harness, admin_id=admin_id, user_id=user_id)

        solver = None
        if use_gemini and api_key:
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
                _log(f"Turn {turn}: USER -> CLICK: callback_data={choice}", enabled=verbose)
            else:
                pending_item = await _get_pending_placement(harness, user_id)
                if pending_item:
                    if solver:
                        acceptance_mode = await _get_acceptance_mode(harness, user_id)
                        answer = await _send_solver_answer(
                            harness,
                            solver,
                            user_id,
                            pending_item,
                            acceptance_mode,
                        )
                    else:
                        answer = pending_item.canonical or pending_item.prompt or ""
                        await harness.send_text(user_id=user_id, text=answer)
                    log_entry["user_action"] = {"type": "text", "value": answer}
                    _log(f"Turn {turn}: USER -> TEXT: {_truncate(answer)}", enabled=verbose)
                else:
                    log_entry["user_action"] = {"type": "noop"}
                    _log(f"Turn {turn}: USER -> NOOP", enabled=verbose)

            reply_message = harness.last_bot_message(user_id)
            reply_text = reply_message.text if reply_message else None
            reply_buttons = harness.find_callback_data(user_id)
            log_entry["bot_reply"] = reply_text
            log_entry["reply_buttons"] = reply_buttons

            if reply_message:
                _log(f"Turn {turn}: BOT <- TEXT: {_truncate(reply_text)}", enabled=verbose)
                _log(f"Turn {turn}: BOT <- inline buttons: {len(reply_buttons)}", enabled=verbose)

            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    finally:
        await harness.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--turns", type=int, default=20)
    parser.add_argument("--no-gemini", action="store_true", help="Run without Gemini (no API key needed)")
    parser.add_argument("--env-file", default=str(ROOT / ".env"), help="Path to .env file")
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument("--verbose", action="store_true", help="Enable verbose logging (default)")
    verbosity.add_argument("--quiet", action="store_true", help="Disable per-turn logging")
    args = parser.parse_args()

    env_file = Path(args.env_file)
    load_dotenv(dotenv_path=env_file, override=False)
    verbose = False if args.quiet else True
    if args.verbose:
        verbose = True

    asyncio.run(
        run_simulation(
            turns=args.turns,
            use_gemini=not args.no_gemini,
            verbose=verbose,
            env_file=env_file,
        )
    )


if __name__ == "__main__":
    main()
