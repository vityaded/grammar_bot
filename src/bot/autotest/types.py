from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class QuestionContext:
    unit_key: str
    exercise_index: int
    item_idx: int
    exercise_type: str
    instruction: str
    item: dict[str, Any]
    question_key: str
    acceptance_mode: str


@dataclass(frozen=True)
class AnswerAttempt:
    raw: str
    normalized: str
    forced_wrong: bool = False
    force_reason: str | None = None


@dataclass(frozen=True)
class Issue:
    issue_type: str
    severity: str
    details: str
    metadata: dict[str, Any] | None = None


@dataclass
class Turn:
    turn_id: int
    question_key: str
    bot_message: str
    user_message: str
    bot_feedback: str
    issues: list[dict[str, Any]]
    jsonl_ref: int | str | None = None
