from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
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

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.bot.autotest.solver import GeminiSolver
from src.bot.autotest.types import QuestionContext
from aiogram.types import Chat, InlineKeyboardMarkup, Message, User

from src.bot.config import load_settings
from src.bot.db import ensure_sqlite_schema, make_engine
from src.bot.models import AccessRequest, PlacementItem, UserState
from tests.telegram_harness.harness import BotHarness
from tests.telegram_harness.session import RecordingSession


def _parse_options(options_json: str | None) -> list[str]:
    if not options_json:
        return []
    try:
        return json.loads(options_json) or []
    except json.JSONDecodeError:
        return []


def _buttons_from_message(message: Message | None) -> list[str]:
    if not message or not message.reply_markup:
        return []
    if not isinstance(message.reply_markup, InlineKeyboardMarkup):
        return []
    buttons: list[str] = []
    for row in message.reply_markup.inline_keyboard:
        for button in row:
            if button.callback_data is not None:
                buttons.append(button.callback_data)
    return buttons


def _load_sample_placement_items(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload["items"] if isinstance(payload, dict) else payload


async def _seed_sample_placement_items(
    sessionmaker: async_sessionmaker,
    sample_path: Path,
) -> int:
    items = _load_sample_placement_items(sample_path)
    async with sessionmaker() as session:
        for index, item in enumerate(items, start=1):
            options_payload = item.get("options")
            selection_policy = item.get("selection_policy")
            correct_options = item.get("correct_options")
            if selection_policy or correct_options:
                options_payload = {
                    "options": item.get("options") or [],
                    "selection_policy": selection_policy,
                    "correct_options": correct_options,
                }
            session.add(
                PlacementItem(
                    order_index=item.get("order_index", index),
                    unit_key=item["unit_key"],
                    prompt=item["prompt"],
                    item_type=item["item_type"],
                    canonical=item.get("canonical"),
                    accepted_variants_json=json.dumps(item.get("accepted_variants", []), ensure_ascii=False),
                    options_json=json.dumps(options_payload, ensure_ascii=False)
                    if options_payload is not None
                    else None,
                    instruction=item.get("instruction"),
                    study_units_json=json.dumps(item.get("meta", {}).get("study_units"), ensure_ascii=False)
                    if item.get("meta", {}).get("study_units") is not None
                    else None,
                )
            )
        await session.flush()
    return len(items)


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
    elif approval_cb:
        admin_request = harness.last_bot_message(admin_id)
        if not admin_request:
            raise RuntimeError("Admin message missing for approval callback")
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


async def run_simulation(
    *,
    turns: int,
    use_gemini: bool,
    verbose: bool,
    commit: bool,
    admin_id: int | None,
    seed_sample_placement: bool,
) -> None:
    settings = load_settings()
    database_url = settings.database_url
    api_key = settings.gemini_api_key if use_gemini else None
    if admin_id is None:
        admin_id = settings.admin_ids[0]
    elif admin_id not in settings.admin_ids:
        settings = dataclasses.replace(settings, admin_ids=[*settings.admin_ids, admin_id])

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
                f"Database: {database_url}",
                f"Transaction: {'commit' if commit else 'rollback'}",
                f"Report: {log_path}",
            ]
        ),
        enabled=True,
    )

    engine = make_engine(settings)
    await ensure_sqlite_schema(engine)
    conn = await engine.connect()
    trans = await conn.begin()
    SessionLocal = async_sessionmaker(bind=conn, expire_on_commit=False)

    harness = await BotHarness.create(
        sessionmaker=SessionLocal,
        settings=settings,
    )
    try:
        user_id = 111
        async with SessionLocal() as session:
            placement_count = await session.scalar(select(func.count(PlacementItem.id)))
        if placement_count == 0:
            print("No placement items in DB; start_placement will immediately finish.", flush=True)
            if seed_sample_placement:
                inserted = await _seed_sample_placement_items(
                    SessionLocal,
                    ROOT / "data" / "placement.json",
                )
                print(f"Seeded {inserted} sample placement items.", flush=True)

        await _ensure_access_flow(harness, admin_id=admin_id, user_id=user_id)

        solver = None
        if use_gemini and api_key:
            solver = GeminiSolver(api_key=api_key, model=harness.settings.llm_model, timeout_sec=30)

        for turn in range(1, turns + 1):
            message = harness.last_bot_message(user_id)
            buttons = _buttons_from_message(message)
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
                    answer = "OK"
                    await harness.send_text(user_id=user_id, text=answer)
                    log_entry["user_action"] = {"type": "text", "value": answer}
                    _log(f"Turn {turn}: USER -> TEXT: {_truncate(answer)}", enabled=verbose)

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
        if commit:
            await trans.commit()
        else:
            await trans.rollback()
    except Exception:
        if trans.is_active:
            await trans.rollback()
        raise
    finally:
        await harness.close()
        await conn.close()
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--turns", type=int, default=20)
    parser.add_argument("--no-gemini", action="store_true", help="Run without Gemini (no API key needed)")
    parser.add_argument("--admin-id", type=int, default=None, help="Override admin id (must be in ADMIN_IDS)")
    parser.add_argument(
        "--seed-sample-placement",
        action="store_true",
        help="Seed sample placement items if none exist (uses data/placement.json)",
    )
    tx_mode = parser.add_mutually_exclusive_group()
    tx_mode.add_argument("--commit", action="store_true", help="Persist changes to the real database")
    tx_mode.add_argument("--rollback", action="store_true", help="Rollback changes (default)")
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument("--verbose", action="store_true", help="Enable verbose logging (default)")
    verbosity.add_argument("--quiet", action="store_true", help="Disable per-turn logging")
    args = parser.parse_args()

    verbose = False if args.quiet else True
    if args.verbose:
        verbose = True

    asyncio.run(
        run_simulation(
            turns=args.turns,
            use_gemini=not args.no_gemini,
            verbose=verbose,
            commit=args.commit,
            admin_id=args.admin_id,
            seed_sample_placement=args.seed_sample_placement,
        )
    )


if __name__ == "__main__":
    main()
