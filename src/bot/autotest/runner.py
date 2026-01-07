from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import random
import traceback
from collections import Counter, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ..db import ensure_sqlite_schema
from ..grader import grade_freetext, grade_option_item, resolve_option_item_config
from ..models import Attempt, UnitExercise, User, UserState, utcnow
from ..normalize import norm_cmp_text, norm_multiselect_raw
from .checks import gather_issues
from .dialogue_logger import DialogueLogger
from .solver import GeminiSolver
from .types import AnswerAttempt, Issue, QuestionContext, Turn

logger = logging.getLogger(__name__)


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    remaining = len(text) - max_chars
    return f"{text[:max_chars]}...[TRUNCATED {remaining} chars]"


def format_bot_message(ctx: QuestionContext, *, max_options: int, max_chars: int) -> str:
    header = f"[{ctx.unit_key} ex{ctx.exercise_index} item{ctx.item_idx}] {ctx.question_key}"
    instruction = _truncate_text(ctx.instruction or "", max_chars)
    prompt = str(ctx.item.get("prompt") or "")
    prompt = _truncate_text(prompt, max_chars) if len(prompt) > max_chars else prompt
    options = [str(x) for x in (ctx.item.get("options") or [])]
    option_lines: list[str] = []
    if options:
        truncated = options[:max_options]
        for idx, opt in enumerate(truncated):
            label = chr(ord("A") + idx)
            opt_text = _truncate_text(str(opt), max_chars) if len(str(opt)) > max_chars else str(opt)
            option_lines.append(f"{label}) {opt_text}")
        remaining = len(options) - len(truncated)
        if remaining > 0:
            option_lines.append(f"...(+{remaining} more)")
    parts = [header, "Instruction:", instruction, "Question:", prompt]
    if option_lines:
        parts.extend(["Options:", "\n".join(option_lines), "Format: A, C"])
    return "\n".join(parts)


@dataclass
class RunnerConfig:
    db_url: str
    total_attempts: int
    user_id: int
    model: str
    mistake_min: int
    mistake_max: int
    seed: int | None
    max_same_item: int
    max_no_progress: int
    timeout_sec: float
    log_dir: Path
    run_id: str
    dialogue_log_path: Path
    problem_dialogue_log_path: Path
    dialogue_context: int
    dialogue_max_options: int
    dialogue_max_chars: int
    dialogue_include_jsonl_ref: bool
    ui_lang: str = "en"
    mode: str = "sweep"


@dataclass
class AttemptStats:
    attempts: int = 0
    correct: int = 0
    almost: int = 0
    wrong: int = 0
    forced_wrong: int = 0
    forced_wrong_accepted: int = 0


class MistakeScheduler:
    def __init__(self, *, mistake_min: int, mistake_max: int, rng: random.Random) -> None:
        if mistake_min <= 0 or mistake_max <= 0:
            raise ValueError("mistake bounds must be positive")
        if mistake_min > mistake_max:
            raise ValueError("mistake_min must be <= mistake_max")
        self._rng = rng
        self._min = mistake_min
        self._max = mistake_max
        self._counter = rng.randint(mistake_min, mistake_max)

    def should_force_wrong(self) -> bool:
        self._counter -= 1
        if self._counter <= 0:
            self._counter = self._rng.randint(self._min, self._max)
            return True
        return False


class StuckDetector:
    def __init__(self, *, max_same_item: int, max_no_progress: int) -> None:
        self._max_same_item = max_same_item
        self._max_no_progress = max_no_progress
        self._same_count = 0
        self._no_progress_count = 0
        self._last_question_key: str | None = None
        self._last_progress_key: str | None = None

    def record(self, *, question_key: str, progress_key: str) -> tuple[str, int] | None:
        if question_key == self._last_question_key:
            self._same_count += 1
        else:
            self._same_count = 1
            self._last_question_key = question_key
        if progress_key == self._last_progress_key:
            self._no_progress_count += 1
        else:
            self._no_progress_count = 1
            self._last_progress_key = progress_key
        if self._same_count > self._max_same_item:
            return ("same_item", self._same_count)
        if self._no_progress_count > self._max_no_progress:
            return ("no_progress", self._no_progress_count)
        return None


class StuckError(RuntimeError):
    pass


class AutotestRunner:
    def __init__(self, config: RunnerConfig, *, api_key: str) -> None:
        self.config = config
        self._engine = create_async_engine(config.db_url, future=True)
        self._sessionmaker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self._engine,
            expire_on_commit=False,
        )
        self._solver = GeminiSolver(
            api_key=api_key,
            model=config.model,
            timeout_sec=config.timeout_sec,
        )
        self._rng = random.Random(config.seed)
        self._mistake_scheduler = MistakeScheduler(
            mistake_min=config.mistake_min,
            mistake_max=config.mistake_max,
            rng=self._rng,
        )
        self._stuck = StuckDetector(
            max_same_item=config.max_same_item,
            max_no_progress=config.max_no_progress,
        )
        self._events: deque[dict[str, Any]] = deque(maxlen=50)
        self._issues: Counter[str] = Counter()
        self._issue_examples: dict[str, list[dict[str, Any]]] = {}
        self._stats = AttemptStats()
        self._timeout_counts: dict[str, int] = {}
        self._last_ctx: dict[str, Any] | None = None
        self._last_verdicts: list[dict[str, Any]] = []
        self._acceptance_mode = "normal"
        self._log_path = config.log_dir / f"autotest_{config.run_id}.jsonl"
        self._summary_path = config.log_dir / f"autotest_{config.run_id}_summary.json"
        self._dialogue_logger = DialogueLogger(
            config.dialogue_log_path,
            config.problem_dialogue_log_path,
            config.dialogue_context,
            config.dialogue_max_options,
            config.dialogue_max_chars,
            config.dialogue_include_jsonl_ref,
        )
        self._event_index = 0
        self._turn_id = 0

    async def run(self) -> int:
        logger.info(
            "Autotest run started (run_id=%s, model=%s, total_attempts=%s)",
            self.config.run_id,
            self.config.model,
            self.config.total_attempts,
        )
        await ensure_sqlite_schema(self._engine)
        async with self._sessionmaker() as session:
            await self._ensure_test_user(session)
            try:
                exit_code = await self._run_sweep(session)
            except StuckError:
                logger.warning("Autotest stopped: stuck condition reached")
                return 2
            except Exception as exc:
                logger.exception("Autotest stopped: unexpected error")
                await self._log_exception("runner_exception", exc)
                await self._write_summary(reason="exception", exception=exc)
                return 3
        await self._write_summary(reason=None)
        logger.info(
            "Autotest run completed (attempts=%s, correct=%s, almost=%s, wrong=%s)",
            self._stats.attempts,
            self._stats.correct,
            self._stats.almost,
            self._stats.wrong,
        )
        return exit_code

    async def _ensure_test_user(self, session: AsyncSession) -> None:
        user = await session.get(User, self.config.user_id)
        if not user:
            user = User(id=self.config.user_id, is_approved=True, ui_lang=self.config.ui_lang)
            session.add(user)
        else:
            user.is_approved = True
            user.ui_lang = self.config.ui_lang
        state = await session.get(UserState, self.config.user_id)
        if not state:
            state = UserState(tg_user_id=self.config.user_id)
            session.add(state)
        state.mode = "check"
        if not state.acceptance_mode:
            state.acceptance_mode = "normal"
        self._acceptance_mode = state.acceptance_mode
        state.pending_due_item_id = None
        state.pending_placement_item_id = None
        state.last_attempt_id = None
        state.updated_at = utcnow()
        await session.commit()

    async def _run_sweep(self, session: AsyncSession) -> int:
        if self.config.mode != "sweep":
            raise ValueError("only sweep mode is implemented")
        exercises = (await session.execute(
            select(UnitExercise).order_by(UnitExercise.unit_key, UnitExercise.exercise_index)
        )).scalars().all()
        for exercise in exercises:
            try:
                items = json.loads(exercise.items_json)
            except Exception:
                items = []
            if not isinstance(items, list):
                items = []
            for idx, item in enumerate(items, start=1):
                if self._stats.attempts >= self.config.total_attempts:
                    return 0
                ctx = self._make_context(exercise, idx, item)
                await self._handle_attempt(session, ctx)
        return 0

    def _make_context(self, exercise: UnitExercise, item_idx: int, item: dict[str, Any]) -> QuestionContext:
        item_type = item.get("item_type") or exercise.exercise_type
        question_key = f"{exercise.unit_key}:{exercise.exercise_index}:{item_idx}"
        return QuestionContext(
            unit_key=exercise.unit_key,
            exercise_index=exercise.exercise_index,
            item_idx=item_idx,
            exercise_type=item_type,
            instruction=exercise.instruction,
            item=item,
            question_key=question_key,
            acceptance_mode=self._acceptance_mode,
        )

    async def _handle_attempt(self, session: AsyncSession, ctx: QuestionContext) -> None:
        self._last_ctx = {
            "instruction": ctx.instruction,
            "prompt": ctx.item.get("prompt"),
            "options": ctx.item.get("options"),
            "canonical": ctx.item.get("canonical"),
        }
        jsonl_ref = await self._log_event("question_loaded", ctx)
        turn = Turn(
            turn_id=self._turn_id,
            question_key=ctx.question_key,
            bot_message=format_bot_message(
                ctx,
                max_options=self.config.dialogue_max_options,
                max_chars=self.config.dialogue_max_chars,
            ),
            user_message="",
            bot_feedback="",
            issues=[],
            jsonl_ref=jsonl_ref if self.config.dialogue_include_jsonl_ref else None,
        )
        self._turn_id += 1
        for issue in gather_issues(ctx):
            await self._record_issue(ctx, issue, turn)
        force_wrong = self._mistake_scheduler.should_force_wrong()
        try:
            attempt = await self._solver.solve(ctx)
        except asyncio.TimeoutError as exc:
            count = self._timeout_counts.get(ctx.question_key, 0) + 1
            self._timeout_counts[ctx.question_key] = count
            await self._log_event("exception", ctx, details={"error": "timeout"})
            await self._record_issue(ctx, Issue("LLM_TIMEOUT", "error", "solver timeout"), turn)
            turn.user_message = "[NO_ANSWER: timeout]"
            turn.bot_feedback = "[ERROR]"
            self._dialogue_logger.append_turn(turn)
            if count >= 3:
                await self._log_stuck(ctx, "gemini_timeout")
                await self._append_stop_turn(ctx, reason="gemini_timeout", turn=turn)
                await self._write_summary(reason="stuck", exception=exc)
                raise StuckError("gemini_timeout")
            return
        except Exception as exc:
            await self._log_event("exception", ctx, details={"error": "solver_exception", "message": str(exc)})
            await self._record_issue(ctx, Issue("LLM_EXCEPTION", "error", str(exc)), turn)
            turn.user_message = f"[NO_ANSWER: {exc}]"
            turn.bot_feedback = "[ERROR]"
            self._dialogue_logger.append_turn(turn)
            await self._write_summary(reason="exception", exception=exc)
            raise
        await self._log_event("answer_generated", ctx, details={"answer_raw": attempt.raw})

        if force_wrong:
            attempt = self._force_wrong(ctx, attempt)
            await self._log_event(
                "answer_forced_wrong",
                ctx,
                details={
                    "answer_raw": attempt.raw,
                    "force_reason": attempt.force_reason,
                },
            )
            self._stats.forced_wrong += 1
            if attempt.force_reason == "canonical-unreliable":
                await self._record_issue(
                    ctx,
                    Issue(
                        "OPTIONS_CANONICAL_MISMATCH",
                        "warning",
                        "canonical or correct options could not be resolved; forced random wrong option",
                    ),
                    turn,
                )

        verdict, answer_norm, canonical_display = self._grade(ctx, attempt.raw)
        await self._log_event(
            "graded",
            ctx,
            details={
                "answer_raw": attempt.raw,
                "answer_norm": answer_norm,
                "canonical_raw": ctx.item.get("canonical"),
                "canonical_norm": norm_cmp_text(str(ctx.item.get("canonical") or "")),
                "verdict": verdict,
            },
        )
        self._stats.attempts += 1
        if verdict == "correct":
            self._stats.correct += 1
        elif verdict == "almost":
            self._stats.almost += 1
        else:
            self._stats.wrong += 1
        if self._stats.attempts == 1 or self._stats.attempts % 50 == 0:
            logger.info(
                "Progress: attempts=%s correct=%s almost=%s wrong=%s",
                self._stats.attempts,
                self._stats.correct,
                self._stats.almost,
                self._stats.wrong,
            )
        if force_wrong and verdict == "correct":
            self._stats.forced_wrong_accepted += 1
            await self._record_issue(
                ctx,
                Issue(
                    "WRONG_GRADED_CORRECT",
                    "error",
                    "forced wrong answer graded correct",
                ),
                turn,
            )
        if norm_cmp_text(str(ctx.item.get("canonical") or "")) == norm_cmp_text(attempt.raw):
            if verdict == "wrong":
                await self._record_issue(
                    ctx,
                    Issue(
                        "CANONICAL_GRADED_WRONG",
                        "error",
                        "canonical answer graded wrong",
                    ),
                    turn,
                )
        progress_key = ctx.question_key
        stuck = self._stuck.record(question_key=ctx.question_key, progress_key=progress_key)
        if stuck:
            if stuck[0] == "no_progress":
                await self._record_issue(
                    ctx,
                    Issue(
                        "NO_PROGRESS",
                        "error",
                        f"no progress for {stuck[1]} attempts",
                    ),
                    turn,
                )
            await self._log_stuck(ctx, f"{stuck[0]}:{stuck[1]}")
            self._finalize_turn(ctx, turn, verdict, answer_norm, canonical_display, attempt, force_wrong)
            self._dialogue_logger.append_turn(turn)
            await self._append_stop_turn(ctx, reason=f"{stuck[0]}:{stuck[1]}", turn=turn)
            await self._write_summary(reason="stuck")
            raise StuckError("stuck")
        self._last_verdicts.append(
            {
                "question_key": ctx.question_key,
                "answer": attempt.raw,
                "verdict": verdict,
            }
        )
        if len(self._last_verdicts) > 5:
            self._last_verdicts = self._last_verdicts[-5:]
        self._finalize_turn(ctx, turn, verdict, answer_norm, canonical_display, attempt, force_wrong)
        self._dialogue_logger.append_turn(turn)
        await self._insert_attempt(session, ctx, answer_norm, verdict)

    def _force_wrong(self, ctx: QuestionContext, attempt: AnswerAttempt) -> AnswerAttempt:
        options = [str(x) for x in (ctx.item.get("options") or [])]
        canonical = str(ctx.item.get("canonical") or "")
        if options:
            config = resolve_option_item_config(
                item_type=ctx.exercise_type,
                item={"canonical": canonical, "options": options},
                instruction=ctx.instruction,
            )
            correct = {norm_cmp_text(opt) for opt in config.correct_options}
            letters = [chr(ord("A") + idx) for idx in range(len(options))]
            wrong_letters = [
                letter
                for letter, opt in zip(letters, options)
                if norm_cmp_text(opt) not in correct
            ]
            if not wrong_letters:
                choice = self._rng.choice(letters) if letters else "X"
                return AnswerAttempt(raw=choice, normalized=choice, forced_wrong=True, force_reason="canonical-unreliable")
            choice = self._rng.choice(wrong_letters)
            return AnswerAttempt(raw=choice, normalized=choice, forced_wrong=True, force_reason="option-swap")

        canonical_norm = norm_cmp_text(canonical)
        candidate = attempt.raw or canonical
        if norm_cmp_text(candidate) == canonical_norm:
            candidate = f"{candidate} not"
        else:
            candidate = f"{candidate} extra"
        normalized = norm_multiselect_raw(candidate) if ctx.exercise_type == "multiselect" else candidate.strip()
        return AnswerAttempt(raw=candidate, normalized=normalized, forced_wrong=True, force_reason="corrupted-text")

    def _grade(self, ctx: QuestionContext, answer: str) -> tuple[str, str, str]:
        item = ctx.item
        canonical = str(item.get("canonical") or "")
        accepted = item.get("accepted_variants") or []
        options = [str(x) for x in (item.get("options") or [])]
        if ctx.exercise_type in ("mcq", "multiselect"):
            config = resolve_option_item_config(
                item_type=ctx.exercise_type,
                item={
                    "canonical": canonical,
                    "options": options,
                    "selection_policy": item.get("selection_policy"),
                    "correct_options": item.get("correct_options"),
                },
                instruction=ctx.instruction,
            )
            order_sensitive = bool(item.get("order_sensitive"))
            result = grade_option_item(
                answer,
                canonical,
                accepted,
                options,
                selection_policy=config.selection_policy,
                correct_options=config.correct_options,
                order_sensitive=order_sensitive,
                explicit_correct_options=config.explicit_correct_options,
            )
            return (result.verdict, result.user_answer_norm, result.canonical)
        result = grade_freetext(answer, canonical, accepted, ctx.acceptance_mode)
        return (result.verdict, result.user_answer_norm, result.canonical)

    async def _insert_attempt(
        self,
        session: AsyncSession,
        ctx: QuestionContext,
        answer_norm: str,
        verdict: str,
    ) -> None:
        attempt = Attempt(
            tg_user_id=self.config.user_id,
            mode="check",
            placement_item_id=None,
            due_item_id=None,
            unit_key=ctx.unit_key,
            prompt=str(ctx.item.get("prompt") or ""),
            canonical=str(ctx.item.get("canonical") or ""),
            user_answer_norm=answer_norm,
            verdict=verdict,
            rule_keys_json=None,
        )
        session.add(attempt)
        await session.commit()
        state = await session.get(UserState, self.config.user_id)
        if state:
            state.last_attempt_id = attempt.id
            state.updated_at = utcnow()
            await session.commit()

    def _finalize_turn(
        self,
        ctx: QuestionContext,
        turn: Turn,
        verdict: str,
        answer_norm: str,
        canonical_display: str,
        attempt: AnswerAttempt,
        force_wrong: bool,
    ) -> None:
        user_lines = [attempt.raw]
        if answer_norm != attempt.raw:
            user_lines.append(f"Normalized: {answer_norm}")
        turn.user_message = "\n".join(user_lines)
        feedback_lines = [f"Verdict: {verdict}"]
        if canonical_display:
            feedback_lines.append(f"Canonical: {canonical_display}")
        if force_wrong:
            feedback_lines.append("ForcedWrong: yes")
            if attempt.force_reason:
                feedback_lines.append(f"ForceReason: {attempt.force_reason}")
        issue_tags = [f"[{issue['issue_type']}]" for issue in turn.issues]
        if issue_tags:
            feedback_lines.append("Issues: " + " ".join(issue_tags))
        turn.bot_feedback = "\n".join(feedback_lines)

    async def _append_stop_turn(self, ctx: QuestionContext, *, reason: str, turn: Turn) -> None:
        stop_issue = Issue("STOPPED", "error", reason)
        stop_turn = Turn(
            turn_id=self._turn_id,
            question_key=ctx.question_key,
            bot_message=(
                f"[STOPPED] reason={reason}\n"
                "Last question:\n"
                + format_bot_message(
                    ctx,
                    max_options=self.config.dialogue_max_options,
                    max_chars=self.config.dialogue_max_chars,
                )
            ),
            user_message=turn.user_message or "[NO_ANSWER]",
            bot_feedback="[ERROR]",
            issues=[{"issue_type": stop_issue.issue_type, "severity": stop_issue.severity, "details": stop_issue.details}],
            jsonl_ref=turn.jsonl_ref if self.config.dialogue_include_jsonl_ref else None,
        )
        self._turn_id += 1
        self._dialogue_logger.mark_problem(stop_turn.turn_id, stop_issue)
        self._dialogue_logger.append_turn(stop_turn)

    async def _record_issue(self, ctx: QuestionContext, issue: Issue, turn: Turn | None = None) -> None:
        self._issues[issue.issue_type] += 1
        if issue.issue_type not in self._issue_examples:
            self._issue_examples[issue.issue_type] = []
        if len(self._issue_examples[issue.issue_type]) < 3:
            self._issue_examples[issue.issue_type].append(
                {
                    "question_key": ctx.question_key,
                    "details": issue.details,
                }
            )
        if turn:
            turn.issues.append(
                {
                    "issue_type": issue.issue_type,
                    "severity": issue.severity,
                    "details": issue.details,
                }
            )
            self._dialogue_logger.mark_problem(turn.turn_id, issue)
        await self._log_event(
            "issue_detected",
            ctx,
            details={
                "issue_type": issue.issue_type,
                "severity": issue.severity,
                "details": issue.details,
            },
        )

    async def _log_event(self, event: str, ctx: QuestionContext, *, details: dict[str, Any] | None = None) -> int:
        payload = {
            "ts": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "run_id": self.config.run_id,
            "event": event,
            "question_key": ctx.question_key,
            "unit_key": ctx.unit_key,
            "exercise_index": ctx.exercise_index,
            "item_idx": ctx.item_idx,
            "exercise_type": ctx.exercise_type,
            "acceptance_mode": ctx.acceptance_mode,
            "model": self.config.model,
        }
        if details:
            payload.update(details)
        self._events.append(payload)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self._event_index += 1
        return self._event_index

    async def _log_exception(self, event: str, exc: Exception) -> None:
        logger.exception("Autotest exception: %s", event)
        payload = {
            "ts": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "run_id": self.config.run_id,
            "event": event,
            "exception": str(exc),
            "trace": traceback.format_exc(),
        }
        self._events.append(payload)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self._event_index += 1

    async def _log_stuck(self, ctx: QuestionContext, reason: str) -> None:
        logger.warning("Autotest stuck: %s (%s)", reason, ctx.question_key)
        await self._log_event("stuck", ctx, details={"reason": reason})

    async def _write_summary(self, *, reason: str | None, exception: Exception | None = None) -> None:
        dialogue_summary = self._dialogue_logger.finalize()
        summary = {
            "run_id": self.config.run_id,
            "totals": {
                "attempts": self._stats.attempts,
                "correct": self._stats.correct,
                "almost": self._stats.almost,
                "wrong": self._stats.wrong,
            },
            "issues": {
                issue_type: {
                    "count": count,
                    "examples": self._issue_examples.get(issue_type, []),
                }
                for issue_type, count in self._issues.items()
            },
            "forced_wrong": {
                "total": self._stats.forced_wrong,
                "accepted": self._stats.forced_wrong_accepted,
            },
            "stop_reason": reason,
            "last_events": list(self._events),
            "last_context": self._last_ctx,
            "last_verdicts": self._last_verdicts,
            "dialogue_log_path": str(self.config.dialogue_log_path),
            "problem_dialogue_log_path": str(self.config.problem_dialogue_log_path),
            "problem_turn_count": dialogue_summary.problem_turn_count,
            "issue_turn_count_by_type": dialogue_summary.issue_turn_count_by_type,
        }
        if exception:
            summary["exception"] = {
                "type": type(exception).__name__,
                "message": str(exception),
                "trace": traceback.format_exc(),
            }
        self._summary_path.parent.mkdir(parents=True, exist_ok=True)
        self._summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
