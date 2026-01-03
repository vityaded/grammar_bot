from __future__ import annotations
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

def kb_why_next(attempt_id: int, next_kind: str) -> InlineKeyboardMarkup:
    # next_kind: placement_next | detour_next | revisit_next | check_next | finish
    b = InlineKeyboardBuilder()
    b.button(text="❓ Чому", callback_data=f"why:{attempt_id}")
    b.button(text="▶️ Далі", callback_data=f"next:{next_kind}:{attempt_id}")
    b.adjust(2)
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

def kb_start_placement() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="Start placement", callback_data="start_placement")
    b.adjust(1)
    return b.as_markup()

def kb_multiselect_submit(attempt_id: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✅ Submit", callback_data=f"ms_submit:{attempt_id}")
    b.adjust(1)
    return b.as_markup()
