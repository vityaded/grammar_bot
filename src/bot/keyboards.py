from __future__ import annotations
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from .i18n import t

def kb_why_next(attempt_id: int, next_kind: str, ui_lang: str) -> InlineKeyboardMarkup:
    # next_kind: placement_next | detour_next | revisit_next | check_next | finish
    b = InlineKeyboardBuilder()
    b.button(text="❓ " + ("Чому" if ui_lang == "uk" else "Why"), callback_data=f"why:{attempt_id}")
    b.button(text="▶️ " + ("Далі" if ui_lang == "uk" else "Next"), callback_data=f"next:{next_kind}:{attempt_id}")
    b.adjust(2)
    return b.as_markup()

def kb_why_only(attempt_id: int, ui_lang: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="❓ " + ("Чому" if ui_lang == "uk" else "Why"), callback_data=f"why:{attempt_id}")
    b.adjust(1)
    return b.as_markup()

def kb_next_only(attempt_id: int, next_kind: str, ui_lang: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="▶️ " + ("Далі" if ui_lang == "uk" else "Next"), callback_data=f"next:{next_kind}:{attempt_id}")
    b.adjust(1)
    return b.as_markup()

def kb_admin_approve(req_id: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✅ Approve", callback_data=f"admin_approve:{req_id}")
    b.adjust(1)
    return b.as_markup()

def kb_admin_actions() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="➕ Create invite link", callback_data="admin_invite")
    b.adjust(1)
    return b.as_markup()

def kb_lang() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="Українська", callback_data="lang:uk")
    b.button(text="English", callback_data="lang:en")
    b.adjust(2)
    return b.as_markup()

def kb_start_placement(ui_lang: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=t("start_placement", ui_lang), callback_data="start_placement")
    b.adjust(1)
    return b.as_markup()
