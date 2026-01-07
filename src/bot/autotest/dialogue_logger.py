from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .types import Issue, Turn


@dataclass(frozen=True)
class DialogueSummary:
    problem_turn_count: int
    issue_turn_count_by_type: dict[str, int]


class DialogueLogger:
    def __init__(
        self,
        dialogue_path: Path,
        problems_path: Path,
        context_turns: int,
        max_options: int,
        max_chars: int,
        include_ref: bool,
        include_info_problems: bool,
    ) -> None:
        self._dialogue_path = dialogue_path
        self._problems_path = problems_path
        self._context_turns = context_turns
        self._max_options = max_options
        self._max_chars = max_chars
        self._include_ref = include_ref
        self._include_info_problems = include_info_problems
        self._turns: list[Turn] = []
        self._problem_turns: dict[int, set[str]] = {}
        self._issue_turns_by_type: dict[str, set[int]] = {}
        self._dialogue_path.parent.mkdir(parents=True, exist_ok=True)
        self._problems_path.parent.mkdir(parents=True, exist_ok=True)
        self._dialogue_handle = self._dialogue_path.open("w", encoding="utf-8", buffering=1)
        self._finalized = False

    def append_turn(self, turn: Turn) -> None:
        self._turns.append(turn)
        formatted = self._format_turn(turn)
        self._dialogue_handle.write(formatted)
        self._dialogue_handle.write("\n")

    def mark_problem(self, turn_index: int, issue: Issue) -> None:
        if self._include_info_problems or issue.severity in ("warning", "error"):
            self._problem_turns.setdefault(turn_index, set()).add(issue.issue_type)
        self._issue_turns_by_type.setdefault(issue.issue_type, set()).add(turn_index)

    def finalize(self) -> DialogueSummary:
        if self._finalized:
            return DialogueSummary(
                problem_turn_count=len(self._problem_turns),
                issue_turn_count_by_type={
                    issue_type: len(turns) for issue_type, turns in self._issue_turns_by_type.items()
                },
            )
        self._finalized = True
        self._dialogue_handle.close()
        self._write_problem_excerpt()
        return DialogueSummary(
            problem_turn_count=len(self._problem_turns),
            issue_turn_count_by_type={
                issue_type: len(turns) for issue_type, turns in self._issue_turns_by_type.items()
            },
        )

    def _write_problem_excerpt(self) -> None:
        if not self._turns:
            self._problems_path.write_text("", encoding="utf-8")
            return
        ranges = self._build_problem_ranges()
        with self._problems_path.open("w", encoding="utf-8", buffering=1) as handle:
            for start, end in ranges:
                issue_types = self._issues_for_range(start, end)
                handle.write(f"----- PROBLEM CONTEXT (turns {start}..{end}) -----\n")
                if issue_types:
                    handle.write(f"Issues: {', '.join(sorted(issue_types))}\n")
                for turn in self._turns[start : end + 1]:
                    handle.write(self._format_turn(turn))
                    handle.write("\n")

    def _build_problem_ranges(self) -> list[tuple[int, int]]:
        if not self._problem_turns:
            return []
        max_turn = len(self._turns) - 1
        ranges: list[tuple[int, int]] = []
        for turn_id in sorted(self._problem_turns):
            start = max(0, turn_id - self._context_turns)
            end = min(max_turn, turn_id + self._context_turns)
            if not ranges:
                ranges.append((start, end))
                continue
            last_start, last_end = ranges[-1]
            if start <= last_end + 1:
                ranges[-1] = (last_start, max(last_end, end))
            else:
                ranges.append((start, end))
        return ranges

    def _issues_for_range(self, start: int, end: int) -> set[str]:
        issue_types: set[str] = set()
        for turn_id, types in self._problem_turns.items():
            if start <= turn_id <= end:
                issue_types.update(types)
        return issue_types

    def _format_turn(self, turn: Turn) -> str:
        feedback = self._with_detect_line(turn)
        parts = [
            self._format_block(self._label("Bot", turn), self._safe_text(turn.bot_message)),
            self._format_block(self._label("User", turn), self._safe_text(turn.user_message)),
            self._format_block(self._label("Bot", turn), self._safe_text(feedback)),
        ]
        return "\n".join(parts)

    def _with_detect_line(self, turn: Turn) -> str:
        if not turn.issues:
            return turn.bot_feedback
        detect_line = self._detect_line(turn.issues)
        if not detect_line:
            return turn.bot_feedback
        if not turn.bot_feedback:
            return detect_line
        return f"{turn.bot_feedback}\n{detect_line}"

    def _detect_line(self, issues: list[dict[str, object]]) -> str | None:
        for issue in issues:
            metadata = issue.get("metadata") or {}
            if not isinstance(metadata, dict):
                continue
            task_kind = metadata.get("task_kind")
            example_kind = metadata.get("example_kind")
            trigger = metadata.get("trigger")
            if task_kind or example_kind or trigger:
                return f"Detect: task_kind={task_kind} example_kind={example_kind} trigger={trigger}"
        return None

    def _label(self, role: str, turn: Turn) -> str:
        if not self._include_ref or turn.jsonl_ref is None:
            return role
        return f"{role} [ref={turn.jsonl_ref}]"

    def _format_block(self, prefix: str, text: str) -> str:
        lines = text.splitlines() or [""]
        if not lines:
            return f"{prefix}:"
        indented = [f"{prefix}: {lines[0]}"]
        indent = " " * (len(prefix) + 2)
        for line in lines[1:]:
            indented.append(f"{indent}{line}")
        return "\n".join(indented)

    def _safe_text(self, text: str) -> str:
        safe_chars: list[str] = []
        for char in text:
            if char in ("\n", "\t"):
                safe_chars.append(char)
                continue
            code = ord(char)
            if code < 32 or code == 127:
                safe_chars.append(f"\\x{code:02x}")
            else:
                safe_chars.append(char)
        return "".join(safe_chars)
