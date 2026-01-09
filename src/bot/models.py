from __future__ import annotations
import datetime as dt
from sqlalchemy import (
    String, Integer, DateTime, Boolean, Text, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .db import Base

UTC = dt.timezone.utc
def utcnow() -> dt.datetime:
    return dt.datetime.now(tz=UTC)

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # tg user id
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    is_approved: Mapped[bool] = mapped_column(Boolean, default=False)
    ui_lang: Mapped[str] = mapped_column(String(8), default="uk")  # uk/en
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

class UserState(Base):
    __tablename__ = "user_state"
    tg_user_id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # placement | detour | revisit | check | idle
    mode: Mapped[str] = mapped_column(String(16), default="idle")
    acceptance_mode: Mapped[str] = mapped_column(String(16), default="normal")

    pending_placement_item_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pending_due_item_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # placement progress
    last_placement_order: Mapped[int] = mapped_column(Integer, default=0)

    # last graded attempt (for buttons)
    last_attempt_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # startup recovery guard
    startup_recovered_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

class AccessRequest(Base):
    __tablename__ = "access_requests"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tg_user_id: Mapped[int] = mapped_column(Integer, index=True)
    invite_token: Mapped[str] = mapped_column(String(128))
    requested_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    approved: Mapped[bool] = mapped_column(Boolean, default=False)
    __table_args__ = (UniqueConstraint("tg_user_id", "invite_token", name="uq_access_user_token"),)

class RuleI18n(Base):
    __tablename__ = "rules_i18n"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    unit_key: Mapped[str] = mapped_column(String(64), unique=True)   # internal key; unit number hidden from user
    rule_text_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_text_uk: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_short_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_short_uk: Mapped[str | None] = mapped_column(Text, nullable=True)
    examples_json: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON list; examples must be English

class RuleI18nV2(Base):
    __tablename__ = "rules_i18n_v2"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_key: Mapped[str] = mapped_column(String(64), unique=True)   # e.g. unit_12_B
    unit_key: Mapped[str] = mapped_column(String(64), index=True)
    section_path: Mapped[str | None] = mapped_column(String(32), nullable=True)
    title_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_text_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_text_uk: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_short_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_short_uk: Mapped[str | None] = mapped_column(Text, nullable=True)
    examples_json: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON list; examples must be English

class PlacementItem(Base):
    __tablename__ = "placement_items"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_index: Mapped[int] = mapped_column(Integer, index=True)
    unit_key: Mapped[str] = mapped_column(String(64), index=True)
    prompt: Mapped[str] = mapped_column(Text)                      # English
    item_type: Mapped[str] = mapped_column(String(32))             # mcq | multiselect | freetext
    canonical: Mapped[str] = mapped_column(Text)                   # shown correct answer
    accepted_variants_json: Mapped[str] = mapped_column(Text, default="[]")  # JSON list
    options_json: Mapped[str | None] = mapped_column(Text, nullable=True)    # JSON list (mcq/multiselect)
    instruction: Mapped[str | None] = mapped_column(Text, nullable=True)     # English instruction shown before each item
    study_units_json: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON list of unit keys

class UnitExercise(Base):
    __tablename__ = "unit_exercises"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    unit_key: Mapped[str] = mapped_column(String(64), index=True)
    exercise_index: Mapped[int] = mapped_column(Integer)           # book order (1..N)
    exercise_type: Mapped[str] = mapped_column(String(32))         # mcq | freetext | multiselect
    instruction: Mapped[str] = mapped_column(Text)                 # English
    items_json: Mapped[str] = mapped_column(Text)                  # JSON list of items
    __table_args__ = (
        UniqueConstraint("unit_key", "exercise_index", name="uq_unit_exercise"),
        Index("ix_unit_exercises_unit_order", "unit_key", "exercise_index"),
    )

class DueItem(Base):
    __tablename__ = "due_items"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tg_user_id: Mapped[int] = mapped_column(Integer, index=True)
    kind: Mapped[str] = mapped_column(String(16))     # detour | revisit | check
    unit_key: Mapped[str] = mapped_column(String(64), index=True)
    due_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), index=True)

    # progress inside current due item:
    # exercise_index is ABSOLUTE (book order). Each batch uses 4 consecutive exercises.
    exercise_index: Mapped[int] = mapped_column(Integer, default=1)
    item_in_exercise: Mapped[int] = mapped_column(Integer, default=1)  # 1..K (K depends on exercise items and filtering)
    correct_in_exercise: Mapped[int] = mapped_column(Integer, default=0)  # 0..required_correct (2 for revisit, up to 3 for detour, capped by items)
    exercise_attempts: Mapped[int] = mapped_column(Integer, default=0)
    exercise_hard_mode: Mapped[bool] = mapped_column(Boolean, default=False)

    batch_num: Mapped[int] = mapped_column(Integer, default=1)  # 1..max_batches
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    cause_rule_keys_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (Index("ix_due_active_order", "tg_user_id", "is_active", "due_at", "id"),)

class Attempt(Base):
    __tablename__ = "attempts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tg_user_id: Mapped[int] = mapped_column(Integer, index=True)
    mode: Mapped[str] = mapped_column(String(16))  # placement | detour | revisit | check
    placement_item_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    due_item_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    unit_key: Mapped[str | None] = mapped_column(String(64), nullable=True)

    prompt: Mapped[str] = mapped_column(Text)
    canonical: Mapped[str] = mapped_column(Text)
    user_answer_norm: Mapped[str] = mapped_column(Text)
    verdict: Mapped[str] = mapped_column(String(16))  # correct | almost | wrong
    rule_keys_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

class WhyCache(Base):
    __tablename__ = "why_cache"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tg_user_id: Mapped[int] = mapped_column(Integer, index=True)
    attempt_id: Mapped[int] = mapped_column(Integer, ForeignKey("attempts.id"), unique=True)
    answer_norm: Mapped[str] = mapped_column(Text)
    message_text: Mapped[str] = mapped_column(Text)
    flipped_to_correct: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    attempt: Mapped[Attempt] = relationship("Attempt")
