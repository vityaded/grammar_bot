from __future__ import annotations
import asyncio
import datetime as dt
import json
import logging
import secrets
import hashlib
import random

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command, CommandStart
from aiogram.utils.formatting import Text, Bold, Code
from sqlalchemy import select, and_, delete, func
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from .config import Settings
from .db_maintenance import purge_generated_exercises
from .exercise_inventory import unit_real_exercise_indices
from .models import (
    User, UserState, AccessRequest, PlacementItem, Attempt, WhyCache, DueItem,
    RuleI18nV2, UnitExercise, utcnow
)
from .keyboards import kb_admin_actions, kb_admin_approve, kb_lang, kb_start_placement, kb_why_next, kb_why_only
from .grader import (
    grade_freetext,
    grade_mcq,
    grade_multiselect,
    grade_option_item,
    maybe_llm_regrade,
    resolve_option_item_config,
)
from .i18n import t
from .due_flow import ensure_detours_for_units, complete_due_without_exercise
from .exercise_generator import ensure_unit_exercise
from .llm import LLMClient

logger = logging.getLogger(__name__)

class _BotMessenger:
    def __init__(self, bot: Bot, chat_id: int):
        self._bot = bot
        self.chat_id = chat_id

    async def answer(self, text: str, **kwargs):
        return await self._bot.send_message(self.chat_id, text, **kwargs)

# ---------------- MarkdownV2 escape ----------------
def esc_md2(text: str) -> str:
    if text is None:
        return ""
    for ch in r"_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, "\\" + ch)
    return text

# ---------------- helpers ----------------
def _get_user_acceptance_mode(st: UserState, settings: Settings) -> str:
    m = (st.acceptance_mode or "").strip().lower()
    if m in ("easy", "normal", "strict"):
        return m
    return settings.acceptance_mode

def _get_user_acceptance_mode_from_state(st: UserState) -> str:
    m = (st.acceptance_mode or "").strip().lower()
    if m in ("easy", "normal", "strict"):
        return m
    return "normal"

async def _set_user_acceptance_mode(s: AsyncSession, st: UserState, mode: str) -> None:
    st.acceptance_mode = mode
    st.updated_at = utcnow()
    await s.commit()

def _effective_correct(verdict: str, flipped_to_correct: bool, mode: str) -> bool:
    if flipped_to_correct:
        return True
    if mode in ("easy", "normal"):
        return verdict in ("correct", "almost")
    return verdict == "correct"

def _should_attach_remediation(verdict: str, acceptance_mode: str, flipped: bool) -> bool:
    return not _effective_correct(verdict, flipped, acceptance_mode)

def _build_llm(settings: Settings) -> LLMClient | None:
    if not settings.gemini_api_key:
        return None
    return LLMClient(settings.gemini_api_key, model=settings.llm_model)

def _next_kind_from_attempt(att: Attempt) -> str | None:
    if att.mode == "placement":
        return "placement_next"
    if att.mode in ("detour", "revisit", "check"):
        return f"{att.mode}_next"
    return None

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

async def _get_or_create_state(s: AsyncSession, tg_user_id: int, settings: Settings) -> UserState:
    st = await s.get(UserState, tg_user_id)
    if st:
        current = (st.acceptance_mode or "").strip().lower()
        if not current:
            st.acceptance_mode = settings.acceptance_mode
            st.updated_at = utcnow()
            await s.commit()
        return st
    st = UserState(
        tg_user_id=tg_user_id,
        mode="idle",
        last_placement_order=0,
        acceptance_mode=settings.acceptance_mode,
    )
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
                parse_mode=ParseMode.MARKDOWN_V2,
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

def _parse_rule_keys(raw: object | None) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if x]
    if isinstance(raw, str):
        try:
            val = json.loads(raw)
            if isinstance(val, list):
                return [str(x) for x in val if x]
        except Exception:
            return []
    return []

def _rule_keys_json(rule_keys: list[str]) -> str | None:
    if not rule_keys:
        return None
    return json.dumps(rule_keys, ensure_ascii=False)

class _RuleDisplayPlan:
    __slots__ = ("show_rule_before", "prefer_short", "examples_per_rule", "max_examples_total")

    def __init__(
        self,
        *,
        show_rule_before: bool,
        prefer_short: bool,
        examples_per_rule: int,
        max_examples_total: int,
    ) -> None:
        self.show_rule_before = show_rule_before
        self.prefer_short = prefer_short
        self.examples_per_rule = examples_per_rule
        self.max_examples_total = max_examples_total

def _section_sort_key(section_path: str | None) -> tuple[int, str, int]:
    if not section_path:
        return (1, "", 0)
    text = section_path.strip()
    prefix = ""
    num = 0
    for i, ch in enumerate(text):
        if ch.isdigit():
            prefix = text[:i]
            try:
                num = int(text[i:] or 0)
            except ValueError:
                num = 0
            break
    else:
        prefix = text
    return (0, prefix.upper(), num)

async def _fetch_rule_v2(s: AsyncSession, rule_key: str) -> RuleI18nV2 | None:
    if not rule_key:
        return None
    return (await s.execute(select(RuleI18nV2).where(RuleI18nV2.rule_key==rule_key))).scalar_one_or_none()

async def _fetch_unit_rules_v2(s: AsyncSession, unit_key: str) -> list[RuleI18nV2]:
    if not unit_key:
        return []
    rows = (await s.execute(select(RuleI18nV2).where(RuleI18nV2.unit_key==unit_key))).scalars().all()
    return sorted(rows, key=lambda r: _section_sort_key(r.section_path))

def _pick_rule_text(rule: RuleI18nV2, ui_lang: str, prefer_short: bool) -> str:
    if ui_lang == "uk":
        candidates = [
            rule.rule_short_uk if prefer_short else None,
            rule.rule_short_en if prefer_short else None,
            rule.rule_text_uk,
            rule.rule_text_en,
            rule.rule_short_uk,
            rule.rule_short_en,
        ]
    else:
        candidates = [
            rule.rule_short_en if prefer_short else None,
            rule.rule_short_uk if prefer_short else None,
            rule.rule_text_en,
            rule.rule_text_uk,
            rule.rule_short_en,
            rule.rule_short_uk,
        ]
    for c in candidates:
        if c:
            return c
    return ""

def _preface_rule_plan(due_kind: str, is_due_start: bool, stuck: bool) -> _RuleDisplayPlan | None:
    if due_kind == "detour":
        if is_due_start or stuck:
            return _RuleDisplayPlan(
                show_rule_before=True,
                prefer_short=False,
                examples_per_rule=1,
                max_examples_total=6,
            )
        return None
    if due_kind == "revisit":
        if is_due_start:
            return _RuleDisplayPlan(
                show_rule_before=True,
                prefer_short=True,
                examples_per_rule=0,
                max_examples_total=2,
            )
        if stuck:
            return _RuleDisplayPlan(
                show_rule_before=True,
                prefer_short=False,
                examples_per_rule=1,
                max_examples_total=6,
            )
        return None
    if due_kind == "check":
        if stuck:
            return _RuleDisplayPlan(
                show_rule_before=True,
                prefer_short=False,
                examples_per_rule=1,
                max_examples_total=6,
            )
        return None
    return None

def _remediation_rule_plan(due_kind: str) -> _RuleDisplayPlan:
    if due_kind == "detour":
        return _RuleDisplayPlan(
            show_rule_before=False,
            prefer_short=True,
            examples_per_rule=0,
            max_examples_total=2,
        )
    return _RuleDisplayPlan(
        show_rule_before=False,
        prefer_short=False,
        examples_per_rule=1,
        max_examples_total=4,
    )

async def _due_attempt_info(
    s: AsyncSession,
    due: DueItem,
    *,
    acceptance_mode: str,
    item_rule_keys: list[str],
) -> tuple[bool, bool]:
    count_q = select(func.count(Attempt.id)).where(Attempt.due_item_id == due.id)
    attempt_count = (await s.execute(count_q)).scalar_one()
    is_due_start = attempt_count == 0

    last_two = (
        await s.execute(
            select(Attempt)
            .where(Attempt.due_item_id == due.id)
            .order_by(Attempt.created_at.desc(), Attempt.id.desc())
            .limit(2)
        )
    ).scalars().all()
    if len(last_two) < 2:
        return is_due_start, False

    cause_keys = _parse_rule_keys(due.cause_rule_keys_json)
    target_keys = cause_keys if cause_keys else item_rule_keys
    if not target_keys:
        return is_due_start, False

    def _overlaps(att: Attempt) -> bool:
        keys = _parse_rule_keys(att.rule_keys_json)
        return any(k in target_keys for k in keys)

    wrong_flags = [not _effective_correct(att.verdict, False, acceptance_mode) for att in last_two]
    stuck = all(wrong_flags) and all(_overlaps(att) for att in last_two)
    return is_due_start, stuck

async def _render_rules_for_keys(
    s: AsyncSession,
    rule_keys: list[str],
    ui_lang: str,
    *,
    max_examples_total: int = 2,
    prefer_short: bool = True,
    examples_per_rule: int = 0,
) -> str:
    if not rule_keys:
        return ""
    rules: list[RuleI18nV2] = []
    for key in rule_keys:
        r = await _fetch_rule_v2(s, key)
        if r:
            rules.append(r)
    if not rules:
        return ""
    rules.sort(key=lambda r: _section_sort_key(r.section_path))

    header = f"*{esc_md2(t('rule_header', ui_lang))}*"
    lines: list[str] = []
    for r in rules:
        text = _pick_rule_text(r, ui_lang, prefer_short)
        if not text:
            continue
        if r.section_path:
            line = f"{esc_md2(r.section_path)}\\. {esc_md2(text)}"
        else:
            line = esc_md2(text)
        lines.append(line)

    msg = header
    if lines:
        msg += " " + lines[0]
        if len(lines) > 1:
            msg += "\n" + "\n".join(lines[1:])

    if max_examples_total > 0:
        examples: list[str] = []
        if examples_per_rule > 0:
            for r in rules:
                if not r.examples_json:
                    continue
                try:
                    ex = json.loads(r.examples_json)
                except Exception:
                    continue
                if isinstance(ex, list):
                    for line in ex[:examples_per_rule]:
                        examples.append(str(line))
                if len(examples) >= max_examples_total:
                    break
        else:
            for r in rules:
                if not r.examples_json:
                    continue
                try:
                    ex = json.loads(r.examples_json)
                    if isinstance(ex, list):
                        for line in ex:
                            examples.append(str(line))
                except Exception:
                    continue
                if len(examples) >= max_examples_total:
                    break
        if examples:
            examples = examples[:max_examples_total]
            msg += "\n" + "\n".join(esc_md2(e) for e in examples)
    return msg.strip()

async def _render_rule_fallback_for_unit(
    s: AsyncSession,
    unit_key: str,
    ui_lang: str,
    *,
    max_sections: int = 3,
    prefer_short: bool = True,
) -> str:
    rules = await _fetch_unit_rules_v2(s, unit_key)
    if not rules:
        return ""
    header = f"*{esc_md2(t('rule_header', ui_lang))}*"
    lines: list[str] = []
    for r in rules[:max_sections]:
        text = _pick_rule_text(r, ui_lang, prefer_short=prefer_short)
        if not text:
            continue
        if r.section_path:
            lines.append(f"{esc_md2(r.section_path)}\\. {esc_md2(text)}")
        else:
            lines.append(esc_md2(text))
    if not lines:
        return ""
    return (header + " " + lines[0] + ("\n" + "\n".join(lines[1:]) if len(lines) > 1 else "")).strip()

def _build_rules_text(
    rules: list[RuleI18nV2],
    ui_lang: str,
    *,
    max_examples_total: int,
    prefer_short: bool,
    examples_per_rule: int,
) -> Text | None:
    if not rules:
        return None
    rules.sort(key=lambda r: _section_sort_key(r.section_path))

    lines: list[str] = []
    for r in rules:
        text = _pick_rule_text(r, ui_lang, prefer_short)
        if not text:
            continue
        if r.section_path:
            line = f"{r.section_path}. {text}"
        else:
            line = text
        lines.append(line)
    if not lines:
        return None

    parts: list[object] = [Bold(t("rule_header", ui_lang)), " ", lines[0]]
    if len(lines) > 1:
        parts.extend(["\n", "\n".join(lines[1:])])

    if max_examples_total > 0:
        examples: list[str] = []
        if examples_per_rule > 0:
            for r in rules:
                if not r.examples_json:
                    continue
                try:
                    ex = json.loads(r.examples_json)
                except Exception:
                    continue
                if isinstance(ex, list):
                    for line in ex[:examples_per_rule]:
                        examples.append(str(line))
                if len(examples) >= max_examples_total:
                    break
        else:
            for r in rules:
                if not r.examples_json:
                    continue
                try:
                    ex = json.loads(r.examples_json)
                    if isinstance(ex, list):
                        for line in ex:
                            examples.append(str(line))
                except Exception:
                    continue
                if len(examples) >= max_examples_total:
                    break
        if examples:
            examples = examples[:max_examples_total]
            parts.extend(["\n", "\n".join(examples)])

    return Text(*parts)

async def _render_rules_for_keys_entities(
    s: AsyncSession,
    rule_keys: list[str],
    ui_lang: str,
    *,
    max_examples_total: int = 2,
    prefer_short: bool = True,
    examples_per_rule: int = 0,
) -> Text | None:
    if not rule_keys:
        return None
    rules: list[RuleI18nV2] = []
    for key in rule_keys:
        r = await _fetch_rule_v2(s, key)
        if r:
            rules.append(r)
    if not rules:
        return None
    return _build_rules_text(
        rules,
        ui_lang,
        max_examples_total=max_examples_total,
        prefer_short=prefer_short,
        examples_per_rule=examples_per_rule,
    )

async def _render_rule_fallback_for_unit_entities(
    s: AsyncSession,
    unit_key: str,
    ui_lang: str,
    *,
    max_sections: int = 3,
    prefer_short: bool = True,
) -> Text | None:
    rules = await _fetch_unit_rules_v2(s, unit_key)
    if not rules:
        return None
    return _build_rules_text(
        rules[:max_sections],
        ui_lang,
        max_examples_total=0,
        prefer_short=prefer_short,
        examples_per_rule=0,
    )

def _build_feedback_text(
    verdict: str,
    user_answer_norm: str,
    canonical: str,
    ui_lang: str,
    acceptance_mode: str,
    note: str = "",
    *,
    show_next_prompt: bool = True,
) -> Text:
    # labels bold; verdict emoji + one word
    if verdict == "correct":
        v = "✅ Correct"
    elif verdict == "almost":
        if acceptance_mode in ("easy", "normal"):
            v = "⚠️ Almost (accepted)"
        else:
            v = "⚠️ Almost (counts as wrong)"
    else:
        v = "❌ Wrong"
    parts: list[object] = [v]
    if note:
        parts.extend(["\n", note])

    # show user answer normalized and correct answer as inline code
    ua = user_answer_norm or "—"
    ca = canonical
    parts.extend(
        [
            "\n",
            Bold(t("your_answer", ui_lang)),
            " ",
            Code(ua),
            "\n",
            Bold(t("correct_answer", ui_lang)),
            " ",
            Code(ca),
        ]
    )
    if show_next_prompt:
        parts.extend(["\n\n", t("press_next", ui_lang)])
    return Text(*parts)

def build_feedback_message(
    verdict: str,
    user_answer_norm: str,
    canonical: str,
    ui_lang: str,
    acceptance_mode: str,
    note: str = "",
    *,
    show_next_prompt: bool = True,
    rule_message: Text | None = None,
) -> dict[str, object]:
    content = _build_feedback_text(
        verdict,
        user_answer_norm,
        canonical,
        ui_lang,
        acceptance_mode,
        note,
        show_next_prompt=show_next_prompt,
    )
    if rule_message:
        content = Text(content, "\n\n", rule_message)
    return content.as_kwargs()

async def _auto_next_after_correct_placement(m: Message, s: AsyncSession, user: User, st: UserState) -> None:
    item = await _placement_next_item(s, st.last_placement_order)
    if not item:
        await m.answer("OK")
        return
    logger.info(
        "next_item: placement_selected user_id=%s placement_item_id=%s reason=%s",
        user.id,
        item.id,
        "placement_correct",
    )
    await _ask_placement_item(m, user, st, item)
    await s.commit()

async def _auto_next_after_correct_due(
    m: Message,
    s: AsyncSession,
    user: User,
    st: UserState,
    due: DueItem,
    *,
    llm: LLMClient | None,
) -> None:
    if due.kind in ("detour", "revisit"):
        completed = await _advance_due_detour_revisit(
            s,
            due,
            effective_correct=True,
            llm=llm,
        )
        if completed:
            due.is_active = False
            follow = _create_follow_due(due)
            if follow:
                follow.tg_user_id = user.id
                s.add(follow)
            await s.commit()
            di = await _next_due_item(s, user.id)
            if di:
                await _log_due_selected(
                    s,
                    di,
                    user_id=user.id,
                    reason="due_completed_next_due",
                )
                await _ask_due_item(
                    m,
                    s,
                    user,
                    st,
                    di,
                    acceptance_mode=_get_user_acceptance_mode_from_state(st),
                    llm=llm,
                )
                await s.commit()
                return
            item = await _placement_next_item(s, st.last_placement_order)
            if item:
                logger.info(
                    "next_item: placement_selected user_id=%s placement_item_id=%s reason=%s",
                    user.id,
                    item.id,
                    "due_completed_no_due",
                )
                await _ask_placement_item(m, user, st, item)
                await s.commit()
            return
    else:
        due.correct_in_exercise += 1
        items_length = await _due_items_length(s, due, llm=llm)
        if items_length is not None and items_length > 0:
            required_correct = min(2, items_length)
        else:
            required_correct = 2
        if due.correct_in_exercise >= required_correct:
            due.exercise_index += 1
            due.item_in_exercise = 1
            due.correct_in_exercise = 0
        else:
            due.item_in_exercise = (due.item_in_exercise or 1) + 1
            items_length = await _due_items_length(s, due, llm=llm)
            if items_length and due.item_in_exercise > items_length:
                due.exercise_index += 1
                due.item_in_exercise = 1
                due.correct_in_exercise = 0

    if due.kind == "check":
        due.is_active = False
        await s.commit()
        di = await _next_due_item(s, user.id)
        if di:
            await _log_due_selected(
                s,
                di,
                user_id=user.id,
                reason="check_completed_next_due",
            )
            await _ask_due_item(
                m,
                s,
                user,
                st,
                di,
                acceptance_mode=_get_user_acceptance_mode_from_state(st),
                llm=llm,
            )
            await s.commit()
            return
        item = await _placement_next_item(s, st.last_placement_order)
        if item:
            logger.info(
                "next_item: placement_selected user_id=%s placement_item_id=%s reason=%s",
                user.id,
                item.id,
                "check_completed_no_due",
            )
            await _ask_placement_item(m, user, st, item)
            await s.commit()
        return

    await s.commit()
    await _log_due_selected(
        s,
        due,
        user_id=user.id,
        reason="continue_due_exercise",
    )
    await _ask_due_item(
        m,
        s,
        user,
        st,
        due,
        acceptance_mode=_get_user_acceptance_mode_from_state(st),
        llm=llm,
    )
    await s.commit()

def _parse_option_payload(options_json: str | None) -> tuple[list[str], str | None, list[str] | None]:
    if not options_json:
        return ([], None, None)
    try:
        v = json.loads(options_json)
        if isinstance(v, list):
            return ([str(x) for x in v], None, None)
        if isinstance(v, dict):
            options = v.get("options")
            if not isinstance(options, list):
                options = []
            selection_policy = v.get("selection_policy")
            correct_options = v.get("correct_options")
            if not isinstance(correct_options, list):
                correct_options = None
            return ([str(x) for x in options], selection_policy, correct_options)
    except Exception:
        pass
    return ([], None, None)

def _parse_options(options_json: str | None) -> list[str]:
    options, _selection_policy, _correct_options = _parse_option_payload(options_json)
    return options

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

def _due_cause_keys(due: DueItem) -> list[str]:
    return _parse_rule_keys(due.cause_rule_keys_json)

def _filter_items_by_cause(items: list[dict], cause_keys: list[str]) -> list[dict]:
    if not cause_keys:
        return items
    cause_set = set(cause_keys)
    filtered: list[dict] = []
    for it in items:
        item_keys = _parse_rule_keys(it.get("rule_keys"))
        if item_keys and cause_set.intersection(item_keys):
            filtered.append(it)
    return filtered or items

def _due_max_exercises(due: DueItem) -> int | None:
    if due.kind == "detour":
        return 4
    if due.kind == "revisit":
        return 2
    if due.kind == "check":
        return 1
    return None

async def _select_real_exercises_for_due(
    s: AsyncSession,
    due: DueItem,
    max_exercises: int,
) -> list[int]:
    if max_exercises <= 0:
        return []
    candidates = await unit_real_exercise_indices(s, due.unit_key)
    if not candidates:
        return []
    seed_input = f"{due.id}:{due.unit_key}:{due.kind}".encode("utf-8")
    seed = int.from_bytes(hashlib.sha256(seed_input).digest()[:8], "big")
    rng = random.Random(seed)
    rng.shuffle(candidates)
    return candidates[: min(max_exercises, len(candidates))]

async def _due_real_exercise_index(
    s: AsyncSession,
    due: DueItem,
) -> int | None:
    if due.kind in ("detour", "revisit"):
        max_exercises = _due_max_exercises(due) or 0
        selected = await _select_real_exercises_for_due(s, due, max_exercises)
        if not selected:
            return None
        position = due.exercise_index or 1
        if position < 1:
            position = 1
        if position > len(selected):
            return None
        return selected[position - 1]
    real_indices = await unit_real_exercise_indices(s, due.unit_key)
    if not real_indices:
        return None
    if due.exercise_index in real_indices:
        return due.exercise_index
    return real_indices[0]


async def _due_selected_exercises(
    s: AsyncSession,
    due: DueItem,
) -> list[int]:
    max_exercises = _due_max_exercises(due)
    if max_exercises is None:
        return await unit_real_exercise_indices(s, due.unit_key)
    return await _select_real_exercises_for_due(s, due, max_exercises)


async def _log_due_selected(
    s: AsyncSession,
    due: DueItem,
    *,
    user_id: int,
    reason: str,
) -> None:
    selected = await _due_selected_exercises(s, due)
    real_exercise_index = await _due_real_exercise_index(s, due)
    selected_preview = selected[:6]
    logger.info(
        "next_item: due_selected user_id=%s due_id=%s kind=%s reason=%s unit_key=%s real_exercise_index=%s exercise_pos=%s selected_exercises=%s item_in_exercise=%s",
        user_id,
        due.id,
        due.kind,
        reason,
        due.unit_key,
        real_exercise_index,
        due.exercise_index,
        selected_preview,
        due.item_in_exercise,
    )

async def _advance_due_detour_revisit(
    s: AsyncSession,
    due: DueItem,
    *,
    effective_correct: bool,
    llm: LLMClient | None,
) -> bool:
    if effective_correct:
        due.correct_in_exercise += 1
        items_length = await _due_items_length(s, due, llm=llm)
        if items_length is not None and items_length > 0:
            required_correct = min(2, items_length)
        else:
            required_correct = 2
        if due.correct_in_exercise >= required_correct:
            due.exercise_index += 1
            due.item_in_exercise = 1
            due.correct_in_exercise = 0
        else:
            due.item_in_exercise = (due.item_in_exercise or 1) + 1
            if items_length and due.item_in_exercise > items_length:
                due.exercise_index += 1
                due.item_in_exercise = 1
                due.correct_in_exercise = 0
    else:
        due.item_in_exercise = 1
        due.correct_in_exercise = 0

    max_exercises = _due_max_exercises(due) or 0
    selected = await _select_real_exercises_for_due(s, due, max_exercises)
    if not selected:
        return True
    if (due.exercise_index or 1) > len(selected):
        return True
    return False

def _create_follow_due(due: DueItem) -> DueItem | None:
    if due.kind == "detour":
        return DueItem(
            tg_user_id=due.tg_user_id,
            kind="revisit",
            unit_key=due.unit_key,
            due_at=utcnow() + dt.timedelta(days=2),
            exercise_index=1,
            item_in_exercise=1,
            correct_in_exercise=0,
            batch_num=1,
            is_active=True,
            cause_rule_keys_json=due.cause_rule_keys_json,
        )
    if due.kind == "revisit":
        return DueItem(
            tg_user_id=due.tg_user_id,
            kind="check",
            unit_key=due.unit_key,
            due_at=utcnow() + dt.timedelta(days=7),
            exercise_index=1,
            item_in_exercise=1,
            correct_in_exercise=0,
            batch_num=1,
            is_active=True,
            cause_rule_keys_json=due.cause_rule_keys_json,
        )
    return None

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
    await m.answer(text, parse_mode=ParseMode.MARKDOWN_V2)

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
) -> tuple[UnitExercise | None, dict | None, int | None]:
    """Returns (exercise, item, item_index); item and item_index may be None."""
    try:
        selected = await _due_selected_exercises(s, due)
        real_exercise_index = await _due_real_exercise_index(s, due)
        if real_exercise_index is None:
            if selected:
                due.exercise_index = 1
                real_exercise_index = selected[0]
                ex = await ensure_unit_exercise(
                    s,
                    unit_key=due.unit_key,
                    exercise_index=real_exercise_index,
                    llm_client=llm,
                    allow_generate=False,
                )
            else:
                bounded_index = max(1, min(due.exercise_index or 1, 2))
                ex = await ensure_unit_exercise(
                    s,
                    unit_key=due.unit_key,
                    exercise_index=bounded_index,
                    llm_client=llm,
                    allow_generate=True,
                )
        else:
            if due.kind in ("detour", "revisit"):
                position = due.exercise_index or 1
                if position < 1 or position > len(selected):
                    due.exercise_index = 1
                    real_exercise_index = selected[0]
            elif due.kind == "check":
                if real_exercise_index != (due.exercise_index or real_exercise_index):
                    due.exercise_index = 1
            ex = await ensure_unit_exercise(
                s,
                unit_key=due.unit_key,
                exercise_index=real_exercise_index,
                llm_client=llm,
                allow_generate=False,
            )
    except ValueError:
        return (None, None, None)
    if not ex:
        if selected:
            refreshed = await _due_selected_exercises(s, due)
            if refreshed:
                due.exercise_index = 1
                ex = await ensure_unit_exercise(
                    s,
                    unit_key=due.unit_key,
                    exercise_index=refreshed[0],
                    llm_client=llm,
                    allow_generate=False,
                )
        if not ex:
            return (None, None, None)
    try:
        items = json.loads(ex.items_json)
        if not isinstance(items, list) or not items:
            return (ex, None, None)
    except Exception:
        return (ex, None, None)
    cause_keys = _due_cause_keys(due)
    filtered_items = _filter_items_by_cause(items, cause_keys)
    if due.kind in ("detour", "revisit"):
        filtered_items = filtered_items[:2]
    item_index = due.item_in_exercise or 1
    if item_index < 1:
        item_index = 1
    if item_index > len(filtered_items):
        item_index = 1
    return (ex, filtered_items[item_index - 1], item_index)

async def _due_items_length(
    s: AsyncSession,
    due: DueItem,
    *,
    llm: LLMClient | None,
) -> int | None:
    try:
        selected = await _due_selected_exercises(s, due)
        real_exercise_index = await _due_real_exercise_index(s, due)
        if real_exercise_index is None:
            if selected:
                due.exercise_index = 1
                real_exercise_index = selected[0]
                ex = await ensure_unit_exercise(
                    s,
                    unit_key=due.unit_key,
                    exercise_index=real_exercise_index,
                    llm_client=llm,
                    allow_generate=False,
                )
            else:
                bounded_index = max(1, min(due.exercise_index or 1, 2))
                ex = await ensure_unit_exercise(
                    s,
                    unit_key=due.unit_key,
                    exercise_index=bounded_index,
                    llm_client=llm,
                    allow_generate=True,
                )
        else:
            ex = await ensure_unit_exercise(
                s,
                unit_key=due.unit_key,
                exercise_index=real_exercise_index,
                llm_client=llm,
                allow_generate=False,
            )
    except ValueError:
        return None
    if not ex:
        if selected:
            refreshed = await _due_selected_exercises(s, due)
            if refreshed:
                due.exercise_index = 1
                ex = await ensure_unit_exercise(
                    s,
                    unit_key=due.unit_key,
                    exercise_index=refreshed[0],
                    llm_client=llm,
                    allow_generate=False,
                )
        if not ex:
            return None
    try:
        items = json.loads(ex.items_json)
    except Exception:
        return None
    if not isinstance(items, list) or not items:
        return None
    cause_keys = _due_cause_keys(due)
    filtered_items = _filter_items_by_cause(items, cause_keys)
    if due.kind in ("detour", "revisit"):
        filtered_items = filtered_items[:2]
    return len(filtered_items)

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
        await _ask_due_item(
            m,
            s,
            user,
            st,
            next_due,
            acceptance_mode=_get_user_acceptance_mode_from_state(st),
            llm=llm,
        )
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
    acceptance_mode: str,
    llm: LLMClient | None,
):
    ex, it, item_index = await _due_current_item(s, due, llm=llm)
    if not ex or not it:
        await _handle_missing_due_content(m, s, user, st, due, llm=llm)
        return
    if item_index and due.item_in_exercise != item_index:
        due.item_in_exercise = item_index

    item_rule_keys = _parse_rule_keys(it.get("rule_keys"))
    is_due_start, stuck = await _due_attempt_info(
        s,
        due,
        acceptance_mode=acceptance_mode,
        item_rule_keys=item_rule_keys,
    )
    plan = _preface_rule_plan(due.kind, is_due_start, stuck)
    if plan and plan.show_rule_before:
        cause_keys = _parse_rule_keys(due.cause_rule_keys_json)
        rule_keys = cause_keys or item_rule_keys
        if rule_keys:
            rule_msg = await _render_rules_for_keys(
                s,
                rule_keys,
                user.ui_lang,
                max_examples_total=plan.max_examples_total,
                prefer_short=plan.prefer_short,
                examples_per_rule=plan.examples_per_rule,
            )
        else:
            rule_msg = await _render_rule_fallback_for_unit(
                s,
                due.unit_key,
                user.ui_lang,
                max_sections=3,
                prefer_short=plan.prefer_short,
            )
        if rule_msg:
            await m.answer(rule_msg, parse_mode=ParseMode.MARKDOWN_V2)

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
    await m.answer(text, parse_mode=ParseMode.MARKDOWN_V2)

    st.mode = due.kind
    st.pending_due_item_id = due.id
    st.pending_placement_item_id = None
    st.updated_at = utcnow()
    await s.commit()

async def _ask_next_due_or_placement(
    m: Message,
    s: AsyncSession,
    user: User,
    st: UserState,
    *,
    llm: LLMClient | None,
    reason: str,
) -> bool:
    di = await _next_due_item(s, user.id)
    if di:
        await _log_due_selected(
            s,
            di,
            user_id=user.id,
            reason=reason,
        )
        await _ask_due_item(
            m,
            s,
            user,
            st,
            di,
            acceptance_mode=_get_user_acceptance_mode_from_state(st),
            llm=llm,
        )
        await s.commit()
        return True
    item = await _placement_next_item(s, st.last_placement_order)
    if item:
        logger.info(
            "next_item: placement_selected user_id=%s placement_item_id=%s reason=%s",
            user.id,
            item.id,
            reason,
        )
        await _ask_placement_item(m, user, st, item)
        await s.commit()
        return True
    st.mode = "idle"
    st.pending_due_item_id = None
    st.pending_placement_item_id = None
    st.updated_at = utcnow()
    await s.commit()
    await m.answer("OK")
    return True

async def _handle_next_action(
    m: Message,
    s: AsyncSession,
    user: User,
    st: UserState,
    att: Attempt,
    next_kind: str,
    *,
    settings: Settings,
    llm: LLMClient | None,
) -> bool:
    wc = (await s.execute(select(WhyCache).where(WhyCache.attempt_id==att.id))).scalar_one_or_none()
    acceptance_mode = _get_user_acceptance_mode(st, settings)
    effective_correct = _effective_correct(
        att.verdict,
        wc is not None and wc.flipped_to_correct,
        acceptance_mode,
    )

    # ---- placement next ----
    if next_kind == "placement_next":
        if effective_correct:
            # show next placement item immediately
            item = await _placement_next_item(s, st.last_placement_order)
            if not item:
                await m.answer("OK")
                return True
            logger.info(
                "next_item: placement_selected user_id=%s placement_item_id=%s reason=%s",
                user.id,
                item.id,
                "placement_correct",
            )
            await _ask_placement_item(m, user, st, item)
            await s.commit()
            return True

        # wrong/almost -> schedule detour, but start detour only AFTER Next (this click).
        placement_item = await s.get(PlacementItem, att.placement_item_id or 0) if att.placement_item_id else None
        unit_keys = _parse_study_units(
            placement_item.study_units_json if placement_item else None,
            att.unit_key,
        )
        await ensure_detours_for_units(
            s,
            tg_user_id=user.id,
            unit_keys=unit_keys,
            cause_rule_keys_json=att.rule_keys_json,
        )
        next_due = await _next_due_item(s, user.id)
        if not next_due:
            await m.answer("OK")
            return True
        await _log_due_selected(
            s,
            next_due,
            user_id=user.id,
            reason="placement_incorrect_detour_scheduled",
        )
        # start detour: show rule then first item immediately
        await _ask_due_item(
            m,
            s,
            user,
            st,
            next_due,
            acceptance_mode=_get_user_acceptance_mode(st, settings),
            llm=llm,
        )
        await s.commit()
        return True

    # ---- due modes ----
    if next_kind in ("detour_next","revisit_next","check_next"):
        due = await s.get(DueItem, att.due_item_id or 0) if att.due_item_id else None
        if not due or not due.is_active:
            # go to next due/placement
            di = await _next_due_item(s, user.id)
            if di:
                await _log_due_selected(
                    s,
                    di,
                    user_id=user.id,
                    reason="previous_due_inactive",
                )
                await _ask_due_item(
                    m,
                    s,
                    user,
                    st,
                    di,
                    acceptance_mode=_get_user_acceptance_mode(st, settings),
                    llm=llm,
                )
                await s.commit()
                return True
            item = await _placement_next_item(s, st.last_placement_order)
            if item:
                logger.info(
                    "next_item: placement_selected user_id=%s placement_item_id=%s reason=%s",
                    user.id,
                    item.id,
                    "no_due_items_after_inactive_due",
                )
                await _ask_placement_item(m, user, st, item)
                await s.commit()
            return True

        # check special: if wrong and why did NOT flip -> detour on next
        if due.kind == "check" and (not effective_correct):
            await ensure_detours_for_units(
                s,
                tg_user_id=user.id,
                unit_keys=[due.unit_key],
                cause_rule_keys_json=att.rule_keys_json,
            )
            next_due = await _next_due_item(s, user.id)
            if not next_due:
                await m.answer("OK")
                return True
            await _log_due_selected(
                s,
                next_due,
                user_id=user.id,
                reason="check_incorrect_detour_scheduled",
            )
            await _ask_due_item(
                m,
                s,
                user,
                st,
                next_due,
                acceptance_mode=_get_user_acceptance_mode(st, settings),
                llm=llm,
            )
            await s.commit()
            return True

        # update progress based on effective_correct, but "almost" counts wrong
        if due.kind in ("detour", "revisit"):
            completed = await _advance_due_detour_revisit(
                s,
                due,
                effective_correct=effective_correct,
                llm=llm,
            )
            if completed:
                due.is_active = False
                follow = _create_follow_due(due)
                if follow:
                    follow.tg_user_id = user.id
                    s.add(follow)
                await s.commit()
                di = await _next_due_item(s, user.id)
                if di:
                    await _log_due_selected(
                        s,
                        di,
                        user_id=user.id,
                        reason="due_completed_next_due",
                    )
                    await _ask_due_item(
                        m,
                        s,
                        user,
                        st,
                        di,
                        acceptance_mode=_get_user_acceptance_mode(st, settings),
                        llm=llm,
                    )
                    await s.commit()
                    return True
                item = await _placement_next_item(s, st.last_placement_order)
                if item:
                    logger.info(
                        "next_item: placement_selected user_id=%s placement_item_id=%s reason=%s",
                        user.id,
                        item.id,
                        "due_completed_no_due",
                    )
                    await _ask_placement_item(m, user, st, item)
                    await s.commit()
                return True
        else:
            if effective_correct:
                due.correct_in_exercise += 1
                items_length = await _due_items_length(s, due, llm=llm)
                if items_length is not None and items_length > 0:
                    required_correct = min(2, items_length)
                else:
                    required_correct = 2
                if due.correct_in_exercise >= required_correct:
                    due.exercise_index += 1
                    due.item_in_exercise = 1
                    due.correct_in_exercise = 0
                else:
                    due.item_in_exercise = (due.item_in_exercise or 1) + 1
                    items_length = await _due_items_length(s, due, llm=llm)
                    if items_length and due.item_in_exercise > items_length:
                        due.exercise_index += 1
                        due.item_in_exercise = 1
                        due.correct_in_exercise = 0
            else:
                due.item_in_exercise = 1
                due.correct_in_exercise = 0

        if due.kind == "check":
            # one question only: if reached here, effective_correct == True, mark done and schedule detour only on wrong (handled above)
            due.is_active = False
            await s.commit()
            # no header/message; just move to next due/placement by showing exercise immediately
            di = await _next_due_item(s, user.id)
            if di:
                await _log_due_selected(
                    s,
                    di,
                    user_id=user.id,
                    reason="check_completed_next_due",
                )
                await _ask_due_item(
                    m,
                    s,
                    user,
                    st,
                    di,
                    acceptance_mode=_get_user_acceptance_mode(st, settings),
                    llm=llm,
                )
                await s.commit()
                return True
            item = await _placement_next_item(s, st.last_placement_order)
            if item:
                logger.info(
                    "next_item: placement_selected user_id=%s placement_item_id=%s reason=%s",
                    user.id,
                    item.id,
                    "check_completed_no_due",
                )
                await _ask_placement_item(m, user, st, item)
                await s.commit()
            return True

        await s.commit()
        await _log_due_selected(
            s,
            due,
            user_id=user.id,
            reason="continue_due_exercise",
        )
        # ask next due item immediately
        await _ask_due_item(
            m,
            s,
            user,
            st,
            due,
            acceptance_mode=_get_user_acceptance_mode(st, settings),
            llm=llm,
        )
        await s.commit()
        return True
    return False

async def _is_stuck_state(
    s: AsyncSession,
    st: UserState,
    *,
    llm: LLMClient | None,
) -> tuple[bool, str]:
    if st.mode == "await_next":
        return True, "await_next"
    if st.mode == "placement":
        if not st.pending_placement_item_id:
            return True, "placement_missing_id"
        item = await s.get(PlacementItem, st.pending_placement_item_id)
        if not item:
            return True, "placement_missing_item"
        return False, ""
    if st.mode in ("detour", "revisit", "check"):
        if not st.pending_due_item_id:
            return True, "due_missing_id"
        due = await s.get(DueItem, st.pending_due_item_id)
        if not due or not due.is_active:
            return True, "due_missing_or_inactive"
        ex, it, _item_index = await _due_current_item(s, due, llm=llm)
        if not ex or not it:
            return True, "due_missing_content"
        return False, ""
    return False, ""

async def resume_stuck_users_on_startup(
    bot: Bot,
    *,
    settings: Settings,
    sessionmaker: async_sessionmaker[AsyncSession],
    throttle_s: float = 0.05,
    recovery_window: dt.timedelta | None = None,
) -> None:
    llm = _build_llm(settings)
    window = recovery_window or dt.timedelta(minutes=30)
    now = utcnow()
    cutoff = now - window
    async with sessionmaker() as s:
        q = (
            select(UserState.tg_user_id)
            .join(User, User.id == UserState.tg_user_id)
            .where(
                User.is_approved.is_(True),
                UserState.mode.in_(("await_next", "placement", "detour", "revisit", "check")),
            )
        )
        user_ids = (await s.execute(q)).scalars().all()
    logger.info("startup_recovery: candidates=%s", len(user_ids))
    if not user_ids:
        return

    stuck_total = 0
    resumed = 0
    failed = 0
    recovered_user_ids: list[int] = []
    for user_id in user_ids:
        async with sessionmaker() as s:
            user = await s.get(User, user_id)
            st = await s.get(UserState, user_id)
            if not user or not st or not user.is_approved:
                continue
            if st.startup_recovered_at:
                recovered_at = st.startup_recovered_at
                if recovered_at.tzinfo is None:
                    recovered_at = recovered_at.replace(tzinfo=dt.timezone.utc)
                if recovered_at >= cutoff:
                    continue
            stuck, reason = await _is_stuck_state(s, st, llm=llm)
            if not stuck:
                continue
            stuck_total += 1
            user_id_value = user.id
            messenger = _BotMessenger(bot, user_id_value)
            try:
                recovered = False
                if st.mode == "await_next":
                    att = await s.get(Attempt, st.last_attempt_id or 0) if st.last_attempt_id else None
                    if att and att.tg_user_id == user_id_value:
                        next_kind = _next_kind_from_attempt(att)
                        if next_kind:
                            recovered = await _handle_next_action(
                                messenger,
                                s,
                                user,
                                st,
                                att,
                                next_kind,
                                settings=settings,
                                llm=llm,
                            )
                    if not recovered:
                        recovered = await _ask_next_due_or_placement(
                            messenger,
                            s,
                            user,
                            st,
                            llm=llm,
                            reason="recovery_no_attempt",
                        )
                elif st.mode in ("placement",):
                    recovered = await _ask_next_due_or_placement(
                        messenger,
                        s,
                        user,
                        st,
                        llm=llm,
                        reason="recovery_missing_placement",
                    )
                elif st.mode in ("detour", "revisit", "check"):
                    recovered = await _ask_next_due_or_placement(
                        messenger,
                        s,
                        user,
                        st,
                        llm=llm,
                        reason="recovery_missing_due",
                    )
                if recovered:
                    st.startup_recovered_at = now
                    st.updated_at = utcnow()
                    await s.commit()
                    resumed += 1
                    recovered_user_ids.append(user_id_value)
                else:
                    failed += 1
            except Exception:
                failed += 1
                await s.rollback()
                logger.exception(
                    "startup_recovery_failed user_id=%s reason=%s",
                    user_id_value,
                    reason,
                )
            if throttle_s:
                await asyncio.sleep(throttle_s)
    logger.info(
        "startup_recovery: stuck_total=%s resumed=%s failed=%s",
        stuck_total,
        resumed,
        failed,
    )
    if recovered_user_ids:
        logger.debug("startup_recovery: resumed_user_ids=%s", recovered_user_ids)

def _grade_item(
    item_type: str,
    user_answer: str,
    canonical: str,
    accepted: list[str],
    options: list[str],
    *,
    mode: str,
    order_sensitive: bool,
    selection_policy: str | None = None,
    correct_options: list[str] | None = None,
    instruction: str | None = None,
    log_context: dict | None = None,
    item_ref: dict | None = None,
) -> tuple[str, str, str, str]:
    if item_type in ("multiselect", "mcq"):
        item_payload = {
            "canonical": canonical,
            "options": options,
            "selection_policy": selection_policy,
            "correct_options": correct_options,
        }
        config = resolve_option_item_config(
            item_type=item_type,
            item=item_payload,
            instruction=instruction,
            logger=logger,
            context=log_context,
        )
        if config.needs_review and item_ref is not None:
            item_ref["needs_review"] = True
        gr = grade_option_item(
            user_answer,
            canonical,
            accepted,
            options,
            selection_policy=config.selection_policy,
            correct_options=config.correct_options,
            order_sensitive=order_sensitive,
            explicit_correct_options=config.explicit_correct_options,
        )
        return (gr.verdict, gr.user_answer_norm, gr.canonical, gr.note)
    gr = grade_freetext(user_answer, canonical, accepted, mode)
    return (gr.verdict, gr.user_answer_norm, gr.canonical, gr.note)

def _due_item_type_from_ex(ex: UnitExercise) -> str:
    return ex.exercise_type

# ---------------- main registration ----------------
def register_handlers(dp: Dispatcher, *, settings: Settings, sessionmaker: async_sessionmaker[AsyncSession]):
    llm = _build_llm(settings)

    @dp.message(Command("admin"))
    async def on_admin(m: Message):
        if m.from_user.id not in settings.admin_ids:
            await m.answer("Forbidden")
            return
        logger.info(
            "admin_action: open_admin_panel admin_id=%s username=%s",
            m.from_user.id,
            m.from_user.username,
        )
        await m.answer("Admin actions:", reply_markup=kb_admin_actions())

    @dp.message(Command("purge_generated_exercises"))
    async def on_purge_generated_exercises(m: Message):
        if m.from_user.id not in settings.admin_ids:
            await m.answer("Forbidden")
            return
        async with sessionmaker() as s:
            deleted = await purge_generated_exercises(s)
        if not deleted:
            await m.answer("No out-of-range exercises to purge.")
            return
        summary = ", ".join(f"{unit_key}={count}" for unit_key, count in sorted(deleted.items()))
        await m.answer(f"Purged out-of-range exercises: {summary}")

    @dp.message(Command(commands=["reset_progress", "reset_all"]))
    async def on_reset_progress(m: Message):
        async with sessionmaker() as s:
            user = await s.get(User, m.from_user.id)
            if not user or not user.is_approved:
                return
            st = await _get_or_create_state(s, user.id, settings)
            await s.execute(delete(WhyCache).where(WhyCache.tg_user_id == user.id))
            await s.execute(delete(Attempt).where(Attempt.tg_user_id == user.id))
            await s.execute(delete(DueItem).where(DueItem.tg_user_id == user.id))
            st.mode = "idle"
            st.pending_placement_item_id = None
            st.pending_due_item_id = None
            st.last_placement_order = 0
            st.last_attempt_id = None
            st.startup_recovered_at = None
            st.updated_at = utcnow()
            await s.commit()
        await m.answer(
            esc_md2(t("progress_reset", user.ui_lang)),
            reply_markup=kb_start_placement(user.ui_lang),
            parse_mode=ParseMode.MARKDOWN_V2,
        )

    @dp.message(Command(commands=["easy", "normal", "strict"]))
    async def on_set_difficulty(m: Message):
        mode = (m.text or "").lstrip("/").strip().lower()
        if mode not in {"easy", "normal", "strict"}:
            return
        async with sessionmaker() as s:
            user = await s.get(User, m.from_user.id)
            if not user or not user.is_approved:
                return
            st = await _get_or_create_state(s, user.id, settings)
            await _set_user_acceptance_mode(s, st, mode)
        await m.answer(f"Difficulty set to {mode.upper()}.")

    @dp.message(CommandStart())
    async def on_start(m: Message):
        async with sessionmaker() as s:
            user = await _get_or_create_user(s, m, settings.ui_default_lang)
            await _get_or_create_state(s, user.id, settings)

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
                await m.answer(
                    esc_md2(t("access_required", settings.ui_default_lang)),
                    reply_markup=kb_lang(),
                    parse_mode=ParseMode.MARKDOWN_V2,
                )
                return

            await m.answer(
                esc_md2(t("choose_lang", user.ui_lang)),
                reply_markup=kb_lang(),
                parse_mode=ParseMode.MARKDOWN_V2,
            )

    @dp.callback_query(F.data == "admin_invite")
    async def admin_invite(c: CallbackQuery):
        if c.from_user.id not in settings.admin_ids:
            await c.answer("Forbidden", show_alert=True)
            return
        token = secrets.token_urlsafe(12)
        start_token = f"INV_{token}"
        logger.info(
            "admin_action: create_invite admin_id=%s username=%s token_prefix=%s",
            c.from_user.id,
            c.from_user.username,
            start_token[:8],
        )
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
        await c.message.answer(msg, parse_mode=ParseMode.MARKDOWN_V2)
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
            logger.info(
                "admin_action: approve_access admin_id=%s username=%s request_id=%s target_user_id=%s",
                c.from_user.id,
                c.from_user.username,
                req_id,
                req.tg_user_id,
            )
            try:
                await c.bot.send_message(
                    req.tg_user_id,
                    esc_md2(t("approved_choose_lang", settings.ui_default_lang)),
                    reply_markup=kb_lang(),
                    parse_mode=ParseMode.MARKDOWN_V2,
                )
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
            st = await _get_or_create_state(s, user.id, settings)

            due = await _next_due_item(s, user.id)
            if due:
                await _log_due_selected(
                    s,
                    due,
                    user_id=user.id,
                    reason="due_item_available_before_placement",
                )
                st.mode = due.kind
                st.pending_due_item_id = due.id
                st.pending_placement_item_id = None
                st.updated_at = utcnow()
                await s.commit()
                # do not send any message before the exercise; show the exercise now (rule may appear for detour/revisit start when created; here due already exists)
                await _ask_due_item(
                    c.message,
                    s,
                    user,
                    st,
                    due,
                    acceptance_mode=_get_user_acceptance_mode(st, settings),
                    llm=llm,
                )
                await s.commit()
                await c.answer()
                return

            item = await _placement_next_item(s, st.last_placement_order)
            if not item:
                # placement finished
                await c.message.answer("OK")
                await c.answer()
                return
            logger.info(
                "next_item: placement_selected user_id=%s placement_item_id=%s reason=%s",
                user.id,
                item.id,
                "no_due_items",
            )
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
            st = await _get_or_create_state(s, user.id, settings)

            if st.mode == "await_next":
                await m.answer(esc_md2(t("use_buttons", user.ui_lang)), parse_mode=ParseMode.MARKDOWN_V2)
                return

            if st.mode == "placement" and st.pending_placement_item_id:
                item = await s.get(PlacementItem, st.pending_placement_item_id)
                if not item:
                    return
                acceptance_mode = _get_user_acceptance_mode(st, settings)
                accepted = _parse_accepted(item.accepted_variants_json)
                options_json = item.options_json
                options, selection_policy, correct_options = _parse_option_payload(options_json)
                order_sensitive = acceptance_mode == "strict"
                verdict, user_norm, canonical_display, note = _grade_item(
                    item.item_type,
                    m.text,
                    item.canonical,
                    accepted,
                    options,
                    mode=acceptance_mode,
                    order_sensitive=order_sensitive,
                    selection_policy=selection_policy,
                    correct_options=correct_options,
                    instruction=item.instruction,
                    log_context={
                        "scope": "placement",
                        "item_id": item.id,
                        "unit_key": item.unit_key,
                    },
                    item_ref=None,
                )
                base_ok = _effective_correct(verdict, False, acceptance_mode)
                rule_keys: list[str] = []
                if not base_ok:
                    unit_keys = _parse_study_units(item.study_units_json, item.unit_key)
                    seen = set()
                    for unit_key in unit_keys:
                        rules = await _fetch_unit_rules_v2(s, unit_key)
                        for rule in rules[:2]:
                            if rule.rule_key and rule.rule_key not in seen:
                                seen.add(rule.rule_key)
                                rule_keys.append(rule.rule_key)

                att = Attempt(
                    tg_user_id=user.id,
                    mode="placement",
                    placement_item_id=item.id,
                    due_item_id=None,
                    unit_key=item.unit_key,
                    prompt=item.prompt,
                    canonical=canonical_display,
                    user_answer_norm=user_norm,
                    verdict=verdict,
                    rule_keys_json=_rule_keys_json(rule_keys),
                )
                s.add(att)
                await s.commit()
                st.last_attempt_id = att.id
                st.pending_placement_item_id = None
                st.mode = "await_next"
                st.updated_at = utcnow()
                await s.commit()

                effective_correct = _effective_correct(verdict, False, acceptance_mode)
                rule_msg = None
                if _should_attach_remediation(verdict, acceptance_mode, False):
                    rule_msg = await _render_rule_fallback_for_unit_entities(
                        s,
                        item.unit_key,
                        user.ui_lang,
                        max_sections=3,
                    )
                fb_kwargs = build_feedback_message(
                    verdict,
                    att.user_answer_norm,
                    att.canonical,
                    user.ui_lang,
                    acceptance_mode,
                    note,
                    show_next_prompt=not effective_correct,
                    rule_message=rule_msg,
                )
                if effective_correct:
                    await m.answer(**fb_kwargs, reply_markup=kb_why_only(att.id, user.ui_lang))
                    await _auto_next_after_correct_placement(m, s, user, st)
                    return
                await m.answer(**fb_kwargs, reply_markup=kb_why_next(att.id, "placement_next", user.ui_lang))
                return

            if st.mode in ("detour","revisit","check") and st.pending_due_item_id:
                due = await s.get(DueItem, st.pending_due_item_id)
                if not due or not due.is_active:
                    return
                ex, it, _item_index = await _due_current_item(s, due, llm=llm)
                if not ex or not it:
                    return
                acceptance_mode = _get_user_acceptance_mode(st, settings)
                item_type = ex.exercise_type
                canonical = str(it.get("canonical","")).strip()
                accepted = [str(x) for x in (it.get("accepted_variants") or [])]
                options = it.get("options") or []
                if not isinstance(options, list):
                    options = []
                selection_policy = it.get("selection_policy")
                correct_options = it.get("correct_options")
                order_sensitive = acceptance_mode == "strict"
                if isinstance(it, dict) and it.get("order_sensitive") is True:
                    order_sensitive = True
                verdict, user_norm, canonical_display, note = _grade_item(
                    item_type,
                    m.text,
                    canonical,
                    accepted,
                    options,
                    mode=acceptance_mode,
                    order_sensitive=order_sensitive,
                    selection_policy=selection_policy,
                    correct_options=correct_options,
                    instruction=ex.instruction,
                    log_context={
                        "scope": "unit_exercise",
                        "unit_key": ex.unit_key,
                        "exercise_index": ex.exercise_index,
                        "item_index": _item_index,
                    },
                    item_ref=it if isinstance(it, dict) else None,
                )
                rule_keys = _parse_rule_keys(it.get("rule_keys"))

                att = Attempt(
                    tg_user_id=user.id,
                    mode=due.kind,
                    placement_item_id=None,
                    due_item_id=due.id,
                    unit_key=due.unit_key,
                    prompt=str(it.get("prompt","")),
                    canonical=canonical_display,
                    user_answer_norm=user_norm,
                    verdict=verdict,
                    rule_keys_json=_rule_keys_json(rule_keys),
                )
                s.add(att)
                await s.commit()
                st.last_attempt_id = att.id
                st.pending_due_item_id = None
                st.mode = "await_next"
                st.updated_at = utcnow()
                await s.commit()

                effective_correct = _effective_correct(verdict, False, acceptance_mode)
                rule_msg = None
                if _should_attach_remediation(verdict, acceptance_mode, False):
                    plan = _remediation_rule_plan(due.kind)
                    if rule_keys:
                        rule_msg = await _render_rules_for_keys_entities(
                            s,
                            rule_keys,
                            user.ui_lang,
                            max_examples_total=plan.max_examples_total,
                            prefer_short=plan.prefer_short,
                            examples_per_rule=plan.examples_per_rule,
                        )
                    else:
                        rule_msg = await _render_rule_fallback_for_unit_entities(
                            s,
                            due.unit_key,
                            user.ui_lang,
                            max_sections=3,
                            prefer_short=plan.prefer_short,
                        )
                fb_kwargs = build_feedback_message(
                    verdict,
                    att.user_answer_norm,
                    att.canonical,
                    user.ui_lang,
                    acceptance_mode,
                    note,
                    show_next_prompt=not effective_correct,
                    rule_message=rule_msg,
                )
                if effective_correct:
                    await m.answer(**fb_kwargs, reply_markup=kb_why_only(att.id, user.ui_lang))
                    await _auto_next_after_correct_due(m, s, user, st, due, llm=llm)
                    return
                await m.answer(**fb_kwargs, reply_markup=kb_why_next(att.id, f"{due.kind}_next", user.ui_lang))
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
            st = await _get_or_create_state(s, user.id, settings)
            att = await s.get(Attempt, attempt_id)
            if not att or att.tg_user_id != user.id:
                await c.answer()
                return

            # cache by attempt + answer_norm (invalidate if changed)
            wc = (await s.execute(select(WhyCache).where(WhyCache.attempt_id==attempt_id))).scalar_one_or_none()
            if wc and wc.answer_norm == att.user_answer_norm:
                await c.message.answer(wc.message_text, parse_mode=ParseMode.MARKDOWN_V2)
                await c.answer()
                return

            # compute
            flipped = False
            explanation = ""
            if llm:
                difficulty = _get_user_acceptance_mode(st, settings)
                ok, out = maybe_llm_regrade(
                    llm=llm,
                    prompt=att.prompt,
                    canonical=att.canonical,
                    user_answer_norm=att.user_answer_norm,
                    flow_mode=att.mode,
                    difficulty=difficulty,
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

            rule_msg = ""
            rule_keys = _parse_rule_keys(att.rule_keys_json)
            if rule_keys:
                rule_msg = await _render_rules_for_keys(s, rule_keys, user.ui_lang, max_examples_total=0, prefer_short=True)
            elif att.unit_key:
                rule_msg = await _render_rule_fallback_for_unit(s, att.unit_key, user.ui_lang, max_sections=3)
            msg = esc_md2(explanation).strip()
            if rule_msg:
                msg = (msg + "\n\n" + rule_msg).strip()

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

            await c.message.answer(msg, parse_mode=ParseMode.MARKDOWN_V2)
        await c.answer()

    # ---------- NEXT button ----------
    @dp.callback_query(F.data.startswith("next:"))
    async def on_next(c: CallbackQuery):
        _, next_kind, attempt_id_str = c.data.split(":", 2)
        attempt_id = int(attempt_id_str)
        async with sessionmaker() as s:
            user = await s.get(User, c.from_user.id)
            st = await _get_or_create_state(s, c.from_user.id, settings)
            if not user or not user.is_approved:
                await c.answer()
                return

            att = await s.get(Attempt, attempt_id)
            if not att or att.tg_user_id != user.id:
                await c.answer()
                return
            await _handle_next_action(
                c.message,
                s,
                user,
                st,
                att,
                next_kind,
                settings=settings,
                llm=llm,
            )

        await c.answer()
