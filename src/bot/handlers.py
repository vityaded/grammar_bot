from __future__ import annotations
import json
import datetime as dt
import secrets

from aiogram import Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command, CommandStart
from sqlalchemy import select, and_, delete
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from .config import Settings
from .models import (
    User, UserState, AccessRequest, PlacementItem, Attempt, WhyCache, DueItem,
    RuleI18n, UnitExercise, utcnow
)
from .keyboards import kb_admin_actions, kb_admin_approve, kb_lang, kb_start_placement, kb_why_next
from .normalize import norm_text, norm_multiselect_raw
from .grader import grade_freetext, grade_mcq, grade_multiselect, maybe_llm_regrade
from .i18n import t
from .due_flow import ensure_detours_for_units, complete_due_without_exercise
from .exercise_generator import ensure_unit_exercise
from .llm import LLMClient

# ---------------- MarkdownV2 escape ----------------
def esc_md2(text: str) -> str:
    if text is None:
        return ""
    for ch in r"_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, "\\" + ch)
    return text

# ---------------- helpers ----------------
async def _get_or_create_user(s: AsyncSession, m: Message, default_lang: str) -> User:
    u = await s.get(User, m.from_user.id)
    if u:
        return u
    u = User(
        id=m.from_user.id,
        username=m.from_user.username,
        first_name=m.from_user.first_name,
        ui_lang=default_lang,
        is_approved=False,
    )
    s.add(u)
    await s.commit()
    return u

async def _get_or_create_state(s: AsyncSession, tg_user_id: int) -> UserState:
    st = await s.get(UserState, tg_user_id)
    if st:
        return st
    st = UserState(tg_user_id=tg_user_id, mode="idle", last_placement_order=0)
    s.add(st)
    await s.commit()
    return st

async def _send_admin_requests(s: AsyncSession, settings: Settings, m: Message, req: AccessRequest):
    for admin_id in settings.admin_ids:
        try:
            await m.bot.send_message(
                admin_id,
                f"Access request from {esc_md2(m.from_user.full_name)} \\({m.from_user.id}\\)\nToken: `{esc_md2(req.invite_token)}`",
                reply_markup=kb_admin_approve(req.id),
            )
        except Exception:
            pass

async def _next_due_item(s: AsyncSession, tg_user_id: int) -> DueItem | None:
    now = utcnow()
    for kind in ("revisit", "check", "detour"):
        q = (
            select(DueItem)
            .where(
                DueItem.tg_user_id==tg_user_id,
                DueItem.is_active==True,
                DueItem.kind==kind,
                DueItem.due_at <= now,
            )
            .order_by(DueItem.due_at.asc(), DueItem.id.asc())
            .limit(1)
        )
        di = (await s.execute(q)).scalar_one_or_none()
        if di:
            return di
    return None

async def _placement_next_item(s: AsyncSession, after_order: int) -> PlacementItem | None:
    q = (
        select(PlacementItem)
        .where(PlacementItem.order_index > after_order)
        .order_by(PlacementItem.order_index.asc())
        .limit(1)
    )
    return (await s.execute(q)).scalar_one_or_none()

async def _render_rule_message(s: AsyncSession, unit_key: str, ui_lang: str) -> str:
    r = (await s.execute(select(RuleI18n).where(RuleI18n.unit_key==unit_key))).scalar_one_or_none()
    if not r:
        return ""
    rule_text = r.rule_text_uk if ui_lang=="uk" else r.rule_text_en
    if not rule_text:
        rule_text = r.rule_text_en or ""
    msg = ""
    if rule_text:
        msg += f"*{esc_md2(t('rule_header', ui_lang))}* {esc_md2(rule_text)}\n"
    if r.examples_json:
        try:
            ex = json.loads(r.examples_json)
            if isinstance(ex, list):
                for line in ex:
                    msg += esc_md2(str(line)) + "\n"
        except Exception:
            pass
    return msg.strip()

async def _render_rule_short(s: AsyncSession, unit_key: str, ui_lang: str) -> str:
    r = (await s.execute(select(RuleI18n).where(RuleI18n.unit_key==unit_key))).scalar_one_or_none()
    if not r:
        return ""
    rs = r.rule_short_uk if ui_lang=="uk" else r.rule_short_en
    if not rs:
        rs = r.rule_short_en or r.rule_text_en or ""
    if not rs:
        return ""
    return f"*{esc_md2(t('rule_header', ui_lang))}* {esc_md2(rs)}"

def _feedback_text(verdict: str, user_answer_norm: str, canonical: str, ui_lang: str) -> str:
    # labels bold; verdict emoji + one word
    if verdict == "correct":
        v = "✅ Correct"
    elif verdict == "almost":
        v = "⚠️ Almost"
    else:
        v = "❌ Wrong"
    # show user answer normalized and correct answer as inline code
    ua = esc_md2(user_answer_norm) if user_answer_norm else "—"
    ca = esc_md2(canonical)
    return f"{v}\n*{esc_md2(t('your_answer', ui_lang))}* `{ua}`\n*{esc_md2(t('correct_answer', ui_lang))}* `{ca}`\n\n{esc_md2(t('press_next', ui_lang))}"

def _parse_options(options_json: str | None) -> list[str]:
    if not options_json:
        return []
    try:
        v = json.loads(options_json)
        if isinstance(v, list):
            return [str(x) for x in v]
    except Exception:
        pass
    return []

def _parse_accepted(accepted_json: str) -> list[str]:
    try:
        v = json.loads(accepted_json or "[]")
        if isinstance(v, list):
            return [str(x) for x in v]
    except Exception:
        pass
    return []

def _parse_study_units(study_units_json: str | None, fallback_unit: str | None) -> list[str]:
    if study_units_json:
        try:
            v = json.loads(study_units_json)
            if isinstance(v, list) and v:
                units: list[str] = []
                for raw in v:
                    if raw is None:
                        continue
                    if isinstance(raw, int):
                        units.append(f"unit_{raw}")
                    else:
                        s = str(raw).strip()
                        if not s:
                            continue
                        if s.isdigit():
                            units.append(f"unit_{s}")
                        else:
                            units.append(s)
                return units
        except Exception:
            pass
    return [fallback_unit] if fallback_unit else []

async def _ask_placement_item(m: Message, user: User, st: UserState, item: PlacementItem):
    instr = item.instruction or ""
    text = ""
    if instr:
        text += esc_md2(instr) + "\n\n"
    text += esc_md2(item.prompt)
    opts = _parse_options(item.options_json)
    if opts:
        for i, o in enumerate(opts):
            label = chr(ord("A")+i)
            text += f"\n{esc_md2(f'{label})')} {esc_md2(str(o))}"
    await m.answer(text)

    st.mode = "placement"
    st.pending_placement_item_id = item.id
    st.pending_due_item_id = None
    st.last_placement_order = item.order_index
    st.updated_at = utcnow()

async def _due_current_item(
    s: AsyncSession,
    due: DueItem,
    *,
    llm: LLMClient | None,
) -> tuple[UnitExercise | None, dict | None]:
    try:
        ex = await ensure_unit_exercise(
            s,
            unit_key=due.unit_key,
            exercise_index=due.exercise_index,
            llm_client=llm,
        )
    except ValueError:
        return (None, None)
    if not ex:
        return (None, None)
    try:
        items = json.loads(ex.items_json)
        if not isinstance(items, list) or not items:
            return (ex, None)
    except Exception:
        return (ex, None)
    # take first two items in order
    idx = 0 if due.item_in_exercise <= 1 else 1
    if idx >= len(items):
        idx = len(items)-1
    return (ex, items[idx])

async def _handle_missing_due_content(
    m: Message,
    s: AsyncSession,
    user: User,
    st: UserState,
    due: DueItem,
    *,
    llm: LLMClient | None,
):
    await complete_due_without_exercise(s, due=due)
    next_due = await _next_due_item(s, user.id)
    if next_due:
        show_rule = next_due.kind in ("detour", "revisit")
        await _ask_due_item(m, s, user, st, next_due, show_rule_first=show_rule, llm=llm)
        await s.commit()
        return
    item = await _placement_next_item(s, st.last_placement_order)
    if item:
        await _ask_placement_item(m, user, st, item)
        await s.commit()
        return
    st.mode = "idle"
    st.pending_due_item_id = None
    st.pending_placement_item_id = None
    st.updated_at = utcnow()
    await s.commit()
    await m.answer("OK")

async def _ask_due_item(
    m: Message,
    s: AsyncSession,
    user: User,
    st: UserState,
    due: DueItem,
    *,
    show_rule_first: bool,
    llm: LLMClient | None,
):
    # If show_rule_first: send one message with rule+examples, then immediately ask first item (separate message).
    if show_rule_first:
        rule_msg = await _render_rule_message(s, due.unit_key, user.ui_lang)
        if rule_msg:
            await m.answer(rule_msg)

    ex, it = await _due_current_item(s, due, llm=llm)
    if not ex or not it:
        await _handle_missing_due_content(m, s, user, st, due, llm=llm)
        return

    instr = ex.instruction or ""
    text = ""
    if instr:
        text += esc_md2(instr) + "\n\n"
    text += esc_md2(str(it.get("prompt","")))
    opts = it.get("options") or []
    if isinstance(opts, list) and opts:
        for i, o in enumerate(opts):
            label = chr(ord("A")+i)
            text += f"\n{esc_md2(f'{label})')} {esc_md2(str(o))}"
    await m.answer(text)

    st.mode = due.kind
    st.pending_due_item_id = due.id
    st.pending_placement_item_id = None
    st.updated_at = utcnow()

def _grade_item(item_type: str, user_answer: str, canonical: str, accepted: list[str], options: list[str]) -> tuple[str, str]:
    if item_type == "multiselect":
        gr = grade_multiselect(user_answer, canonical, options, accepted)
    elif item_type == "mcq":
        gr = grade_mcq(user_answer, canonical, accepted, options)
    else:
        gr = grade_freetext(user_answer, canonical, accepted)
    return (gr.verdict, gr.user_answer_norm)

def _due_item_type_from_ex(ex: UnitExercise) -> str:
    return ex.exercise_type

# ---------------- main registration ----------------
def register_handlers(dp: Dispatcher, *, settings: Settings, sessionmaker: async_sessionmaker[AsyncSession]):
    llm = LLMClient(settings.gemini_api_key) if settings.gemini_api_key else None

    @dp.message(Command("admin"))
    async def on_admin(m: Message):
        if m.from_user.id not in settings.admin_ids:
            await m.answer("Forbidden")
            return
        await m.answer("Admin actions:", reply_markup=kb_admin_actions())

    @dp.message(Command("reset_progress"))
    async def on_reset_progress(m: Message):
        async with sessionmaker() as s:
            user = await s.get(User, m.from_user.id)
            if not user or not user.is_approved:
                return
            st = await _get_or_create_state(s, user.id)
            await s.execute(delete(WhyCache).where(WhyCache.tg_user_id == user.id))
            await s.execute(delete(Attempt).where(Attempt.tg_user_id == user.id))
            await s.execute(delete(DueItem).where(DueItem.tg_user_id == user.id))
            st.mode = "idle"
            st.pending_placement_item_id = None
            st.pending_due_item_id = None
            st.last_placement_order = 0
            st.last_attempt_id = None
            st.updated_at = utcnow()
            await s.commit()
        await m.answer(
            esc_md2(t("progress_reset", user.ui_lang)),
            reply_markup=kb_start_placement(user.ui_lang),
        )

    @dp.message(CommandStart())
    async def on_start(m: Message):
        async with sessionmaker() as s:
            user = await _get_or_create_user(s, m, settings.ui_default_lang)
            await _get_or_create_state(s, user.id)

            token = None
            if m.text and len(m.text.split()) > 1:
                arg = m.text.split(maxsplit=1)[1].strip()
                if arg.startswith("INV_"):
                    token = arg[4:]

            if not user.is_approved:
                if token:
                    req = AccessRequest(tg_user_id=user.id, invite_token=token)
                    s.add(req)
                    try:
                        await s.commit()
                    except Exception:
                        await s.rollback()
                        req = (await s.execute(
                            select(AccessRequest).where(and_(AccessRequest.tg_user_id==user.id, AccessRequest.invite_token==token))
                        )).scalar_one()
                    await _send_admin_requests(s, settings, m, req)
                await m.answer(esc_md2(t("access_required", settings.ui_default_lang)), reply_markup=kb_lang())
                return

            await m.answer(esc_md2(t("choose_lang", user.ui_lang)), reply_markup=kb_lang())

    @dp.callback_query(F.data == "admin_invite")
    async def admin_invite(c: CallbackQuery):
        if c.from_user.id not in settings.admin_ids:
            await c.answer("Forbidden", show_alert=True)
            return
        token = secrets.token_urlsafe(12)
        start_token = f"INV_{token}"
        try:
            me = await c.bot.get_me()
            username = me.username
        except Exception:
            username = None
        if username:
            link = f"https://t.me/{username}?start={start_token}"
            msg = (
                "Invite link:\n"
                f"{esc_md2(link)}\n\n"
                f"Token: `{esc_md2(start_token)}`"
            )
        else:
            msg = (
                "Invite token:\n"
                f"`{esc_md2(start_token)}`\n\n"
                f"Use /start `{esc_md2(start_token)}`"
            )
        await c.message.answer(msg)
        await c.answer("Invite created")

    @dp.callback_query(F.data.startswith("lang:"))
    async def on_lang(c: CallbackQuery):
        lang = c.data.split(":",1)[1]
        async with sessionmaker() as s:
            user = await s.get(User, c.from_user.id)
            if not user:
                await c.answer()
                return
            user.ui_lang = lang
            await s.commit()
        await c.message.answer("OK", reply_markup=kb_start_placement(lang))
        await c.answer()

    @dp.callback_query(F.data.startswith("admin_approve:"))
    async def admin_approve(c: CallbackQuery):
        if c.from_user.id not in settings.admin_ids:
            await c.answer("Forbidden", show_alert=True)
            return
        req_id = int(c.data.split(":")[1])
        async with sessionmaker() as s:
            req = await s.get(AccessRequest, req_id)
            if not req:
                await c.answer()
                return
            req.approved = True
            user = await s.get(User, req.tg_user_id)
            if user:
                user.is_approved = True
            await s.commit()
            try:
                await c.bot.send_message(req.tg_user_id, esc_md2(t("approved_choose_lang", settings.ui_default_lang)), reply_markup=kb_lang())
            except Exception:
                pass
        await c.answer("Approved")
        try:
            await c.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

    # Gate placement by due revisits/checks
    @dp.callback_query(F.data == "start_placement")
    async def start_placement(c: CallbackQuery):
        async with sessionmaker() as s:
            user = await s.get(User, c.from_user.id)
            if not user or not user.is_approved:
                await c.answer("No access", show_alert=True)
                return
            st = await _get_or_create_state(s, user.id)

            due = await _next_due_item(s, user.id)
            if due:
                st.mode = due.kind
                st.pending_due_item_id = due.id
                st.pending_placement_item_id = None
                st.updated_at = utcnow()
                await s.commit()
                # do not send any message before the exercise; show the exercise now (rule may appear for detour/revisit start when created; here due already exists)
                await _ask_due_item(c.message, s, user, st, due, show_rule_first=(due.kind in ("detour","revisit") and due.item_in_exercise==1 and due.correct_in_exercise==0 and due.batch_num==1 and due.exercise_index==1), llm=llm)
                await s.commit()
                await c.answer()
                return

            item = await _placement_next_item(s, st.last_placement_order)
            if not item:
                # placement finished
                await c.message.answer("OK")
                await c.answer()
                return
            await _ask_placement_item(c.message, user, st, item)
            await s.commit()
        await c.answer()

    # ---------- answer messages (user text) ----------
    @dp.message(F.text)
    async def on_answer(m: Message):
        async with sessionmaker() as s:
            user = await s.get(User, m.from_user.id)
            if not user or not user.is_approved:
                return
            st = await _get_or_create_state(s, user.id)

            if st.mode == "await_next":
                await m.answer(esc_md2(t("use_buttons", user.ui_lang)))
                return

            if st.mode == "placement" and st.pending_placement_item_id:
                item = await s.get(PlacementItem, st.pending_placement_item_id)
                if not item:
                    return
                accepted = _parse_accepted(item.accepted_variants_json)
                options = _parse_options(item.options_json)
                verdict, user_norm = _grade_item(item.item_type, m.text, item.canonical, accepted, options)

                att = Attempt(
                    tg_user_id=user.id,
                    mode="placement",
                    placement_item_id=item.id,
                    due_item_id=None,
                    unit_key=item.unit_key,
                    prompt=item.prompt,
                    canonical=item.canonical,
                    user_answer_norm=(norm_multiselect_raw(user_norm) if item.item_type=="multiselect" else norm_text(user_norm)),
                    verdict=verdict,
                )
                s.add(att)
                await s.commit()
                st.last_attempt_id = att.id
                st.pending_placement_item_id = None
                st.mode = "await_next"
                st.updated_at = utcnow()
                await s.commit()

                # show feedback + buttons only
                fb = _feedback_text(verdict, att.user_answer_norm, att.canonical, user.ui_lang)
                await m.answer(fb, reply_markup=kb_why_next(att.id, "placement_next", user.ui_lang))
                return

            if st.mode in ("detour","revisit","check") and st.pending_due_item_id:
                due = await s.get(DueItem, st.pending_due_item_id)
                if not due or not due.is_active:
                    return
                ex, it = await _due_current_item(s, due, llm=llm)
                if not ex or not it:
                    return
                item_type = ex.exercise_type
                canonical = str(it.get("canonical","")).strip()
                accepted = [str(x) for x in (it.get("accepted_variants") or [])]
                options = it.get("options") or []
                if not isinstance(options, list):
                    options = []
                verdict, user_norm = _grade_item(item_type, m.text, canonical, accepted, options)

                att = Attempt(
                    tg_user_id=user.id,
                    mode=due.kind,
                    placement_item_id=None,
                    due_item_id=due.id,
                    unit_key=due.unit_key,
                    prompt=str(it.get("prompt","")),
                    canonical=canonical,
                    user_answer_norm=(norm_multiselect_raw(user_norm) if item_type=="multiselect" else norm_text(user_norm)),
                    verdict=verdict,
                )
                s.add(att)
                await s.commit()
                st.last_attempt_id = att.id
                st.pending_due_item_id = None
                st.mode = "await_next"
                st.updated_at = utcnow()
                await s.commit()

                fb = _feedback_text(verdict, att.user_answer_norm, att.canonical, user.ui_lang)
                await m.answer(fb, reply_markup=kb_why_next(att.id, f"{due.kind}_next", user.ui_lang))
                return

    # ---------- WHY button ----------
    @dp.callback_query(F.data.startswith("why:"))
    async def on_why(c: CallbackQuery):
        attempt_id = int(c.data.split(":")[1])
        async with sessionmaker() as s:
            user = await s.get(User, c.from_user.id)
            if not user:
                await c.answer()
                return
            att = await s.get(Attempt, attempt_id)
            if not att or att.tg_user_id != user.id:
                await c.answer()
                return

            # cache by attempt + answer_norm (invalidate if changed)
            wc = (await s.execute(select(WhyCache).where(WhyCache.attempt_id==attempt_id))).scalar_one_or_none()
            if wc and wc.answer_norm == att.user_answer_norm:
                await c.message.answer(wc.message_text)
                await c.answer()
                return

            # compute
            flipped = False
            explanation = ""
            if llm:
                ok, out = maybe_llm_regrade(
                    llm=llm,
                    prompt=att.prompt,
                    canonical=att.canonical,
                    user_answer_norm=att.user_answer_norm,
                    mode=att.mode,
                    ui_lang=user.ui_lang,
                )
                if ok:
                    # parse: first line CORRECT/WRONG, rest explanation
                    lines = [x.strip() for x in out.splitlines() if x.strip()]
                    verdict_line = lines[0].upper() if lines else "WRONG"
                    explanation = "\n".join(lines[1:]) if len(lines) > 1 else ""
                    if verdict_line.startswith("CORRECT") and att.verdict != "correct":
                        flipped = True

            if not explanation:
                explanation = "Пояснення недоступне без LLM ключа\\." if user.ui_lang=="uk" else "Explanation unavailable without an LLM key\\."

            rule_short = await _render_rule_short(s, att.unit_key or "", user.ui_lang) if att.unit_key else ""
            msg = esc_md2(explanation).strip()
            if rule_short:
                msg = (msg + "\n\n" + rule_short).strip()

            # persist cache
            if wc:
                wc.answer_norm = att.user_answer_norm
                wc.message_text = msg
                wc.flipped_to_correct = flipped
            else:
                wc = WhyCache(
                    tg_user_id=user.id,
                    attempt_id=att.id,
                    answer_norm=att.user_answer_norm,
                    message_text=msg,
                    flipped_to_correct=flipped,
                )
                s.add(wc)
            await s.commit()

            await c.message.answer(msg)
        await c.answer()

    # ---------- NEXT button ----------
    @dp.callback_query(F.data.startswith("next:"))
    async def on_next(c: CallbackQuery):
        _, next_kind, attempt_id_str = c.data.split(":", 2)
        attempt_id = int(attempt_id_str)
        async with sessionmaker() as s:
            user = await s.get(User, c.from_user.id)
            st = await _get_or_create_state(s, c.from_user.id)
            if not user or not user.is_approved:
                await c.answer()
                return

            att = await s.get(Attempt, attempt_id)
            if not att or att.tg_user_id != user.id:
                await c.answer()
                return

            wc = (await s.execute(select(WhyCache).where(WhyCache.attempt_id==attempt_id))).scalar_one_or_none()
            effective_correct = (att.verdict == "correct") or (wc is not None and wc.flipped_to_correct)

            # ---- placement next ----
            if next_kind == "placement_next":
                if effective_correct:
                    # show next placement item immediately
                    item = await _placement_next_item(s, st.last_placement_order)
                    if not item:
                        await c.message.answer("OK")
                        await c.answer()
                        return
                    await _ask_placement_item(c.message, user, st, item)
                    await s.commit()
                    await c.answer()
                    return

                # wrong/almost -> schedule detour, but start detour only AFTER Next (this click).
                placement_item = await s.get(PlacementItem, att.placement_item_id or 0) if att.placement_item_id else None
                unit_keys = _parse_study_units(
                    placement_item.study_units_json if placement_item else None,
                    att.unit_key,
                )
                await ensure_detours_for_units(s, tg_user_id=user.id, unit_keys=unit_keys)
                next_due = await _next_due_item(s, user.id)
                if not next_due:
                    await c.message.answer("OK")
                    await c.answer()
                    return
                # start detour: show rule then first item immediately
                await _ask_due_item(c.message, s, user, st, next_due, show_rule_first=True, llm=llm)
                await s.commit()
                await c.answer()
                return

            # ---- due modes ----
            if next_kind in ("detour_next","revisit_next","check_next"):
                due = await s.get(DueItem, att.due_item_id or 0) if att.due_item_id else None
                if not due or not due.is_active:
                    # go to next due/placement
                    di = await _next_due_item(s, user.id)
                    if di:
                        await _ask_due_item(c.message, s, user, st, di, show_rule_first=(di.kind in ("detour","revisit") and di.exercise_index==1 and di.item_in_exercise==1 and di.batch_num==1), llm=llm)
                        await s.commit()
                        await c.answer()
                        return
                    item = await _placement_next_item(s, st.last_placement_order)
                    if item:
                        await _ask_placement_item(c.message, user, st, item)
                        await s.commit()
                    await c.answer()
                    return

                # check special: if wrong and why did NOT flip -> detour on next
                if due.kind == "check" and (not effective_correct):
                    await ensure_detours_for_units(s, tg_user_id=user.id, unit_keys=[due.unit_key])
                    next_due = await _next_due_item(s, user.id)
                    if not next_due:
                        await c.message.answer("OK")
                        await c.answer()
                        return
                    await _ask_due_item(c.message, s, user, st, next_due, show_rule_first=True, llm=llm)
                    await s.commit()
                    await c.answer()
                    return

                # update progress based on effective_correct, but "almost" counts wrong
                if effective_correct:
                    due.correct_in_exercise += 1
                    if due.correct_in_exercise >= 2:
                        # exercise completed -> advance to next exercise
                        due.exercise_index += 1
                        due.item_in_exercise = 1
                        due.correct_in_exercise = 0
                    else:
                        # ask next item in this exercise
                        due.item_in_exercise = 2
                else:
                    # wrong or almost => reset counters
                    due.item_in_exercise = 1
                    due.correct_in_exercise = 0

                    # regen batch for detour/revisit if under limit; check doesn't regen here
                    max_batches = 5
                    if due.kind in ("detour","revisit") and due.batch_num < max_batches:
                        # move to next batch start immediately
                        due.batch_num += 1
                        due.exercise_index = (due.batch_num - 1) * 4 + 1
                        due.item_in_exercise = 1
                        due.correct_in_exercise = 0
                        await s.commit()
                        # show rule again + ask first item immediately
                        await _ask_due_item(c.message, s, user, st, due, show_rule_first=True, llm=llm)
                        await s.commit()
                        await c.answer()
                        return

                # check completion conditions
                if due.kind in ("detour","revisit"):
                    # each batch is 4 exercises; stop after 5 batches -> schedule revisit (if detour) or check (if revisit)
                    batch_start = (due.batch_num - 1) * 4 + 1
                    batch_end_exclusive = batch_start + 4
                    if due.exercise_index >= batch_end_exclusive:
                        if due.batch_num >= 5:
                            due.is_active = False
                            # schedule follow-up
                            if due.kind == "detour":
                                follow = DueItem(
                                    tg_user_id=user.id,
                                    kind="revisit",
                                    unit_key=due.unit_key,
                                    due_at=utcnow() + dt.timedelta(days=2),
                                    exercise_index=1,
                                    item_in_exercise=1,
                                    correct_in_exercise=0,
                                    batch_num=1,
                                    is_active=True,
                                )
                                s.add(follow)
                            else:
                                follow = DueItem(
                                    tg_user_id=user.id,
                                    kind="check",
                                    unit_key=due.unit_key,
                                    due_at=utcnow() + dt.timedelta(days=7),
                                    exercise_index=1,
                                    item_in_exercise=1,
                                    correct_in_exercise=0,
                                    batch_num=1,
                                    is_active=True,
                                )
                                s.add(follow)
                            await s.commit()
                            # stop message only for detour/revisit
                            stop_msg = "OK\n\n" + esc_md2(t("press_next_only", user.ui_lang))
                            await c.message.answer(stop_msg, reply_markup=kb_why_next(att.id, f"{due.kind}_next", user.ui_lang))
                            await c.answer()
                            return
                        else:
                            # next batch
                            due.batch_num += 1
                            due.exercise_index = (due.batch_num - 1) * 4 + 1
                            due.item_in_exercise = 1
                            due.correct_in_exercise = 0

                if due.kind == "check":
                    # one question only: if reached here, effective_correct == True, mark done and schedule detour only on wrong (handled above)
                    due.is_active = False
                    await s.commit()
                    # no header/message; just move to next due/placement by showing exercise immediately
                    di = await _next_due_item(s, user.id)
                    if di:
                        await _ask_due_item(c.message, s, user, st, di, show_rule_first=(di.kind in ("detour","revisit") and di.exercise_index==1 and di.item_in_exercise==1 and di.batch_num==1), llm=llm)
                        await s.commit()
                        await c.answer()
                        return
                    item = await _placement_next_item(s, st.last_placement_order)
                    if item:
                        await _ask_placement_item(c.message, user, st, item)
                        await s.commit()
                    await c.answer()
                    return

                await s.commit()
                # ask next due item immediately
                await _ask_due_item(c.message, s, user, st, due, show_rule_first=False, llm=llm)
                await s.commit()
                await c.answer()
                return

        await c.answer()
