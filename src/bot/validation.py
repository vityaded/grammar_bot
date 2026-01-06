from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .normalize import norm_cmp_text


@dataclass(frozen=True)
class ValidationIssue:
    severity: str  # "error" | "warning"
    message: str
    unit_key: str | None = None
    exercise_index: int | None = None
    item_index: int | None = None


def _iter_exercises(unit_exercises_json) -> Iterable[dict]:
    if isinstance(unit_exercises_json, dict):
        exercises = unit_exercises_json.get("exercises")
        if isinstance(exercises, list):
            return exercises
    if isinstance(unit_exercises_json, list):
        return unit_exercises_json
    return []


def _option_lookup(options: list[str]) -> set[str]:
    return {norm_cmp_text(opt) for opt in options if norm_cmp_text(opt)}


def _canonical_parts(canonical: str) -> list[str]:
    return [p.strip() for p in (canonical or "").split(",") if p.strip()]


def validate_unit_exercises(unit_exercises_json) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    exercises = _iter_exercises(unit_exercises_json)
    for ex in exercises:
        if not isinstance(ex, dict):
            continue
        unit_key = ex.get("unit_key")
        exercise_index = ex.get("exercise_index")
        exercise_type = ex.get("exercise_type")
        items = ex.get("items")
        if not isinstance(items, list):
            continue
        for idx, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                continue
            item_type = item.get("item_type") or exercise_type
            if item_type not in ("mcq", "multiselect"):
                continue
            options = item.get("options") or []
            if not isinstance(options, list) or not options:
                continue
            option_set = _option_lookup([str(x) for x in options])
            selection_policy = item.get("selection_policy")
            if selection_policy is not None and selection_policy not in ("any", "all"):
                issues.append(
                    ValidationIssue(
                        "error",
                        "selection_policy must be 'any' or 'all'",
                        unit_key,
                        exercise_index,
                        idx,
                    )
                )
            correct_options = item.get("correct_options")
            if correct_options is not None:
                if not isinstance(correct_options, list) or not correct_options:
                    issues.append(
                        ValidationIssue(
                            "error",
                            "correct_options must be a non-empty list",
                            unit_key,
                            exercise_index,
                            idx,
                        )
                    )
                else:
                    for raw in correct_options:
                        if norm_cmp_text(str(raw)) not in option_set:
                            issues.append(
                                ValidationIssue(
                                    "error",
                                    "correct_options entry not in options",
                                    unit_key,
                                    exercise_index,
                                    idx,
                                )
                            )
            if selection_policy == "any":
                if not correct_options:
                    issues.append(
                        ValidationIssue(
                            "error",
                            "selection_policy='any' requires correct_options",
                            unit_key,
                            exercise_index,
                            idx,
                        )
                    )
            if selection_policy == "all":
                if not correct_options:
                    issues.append(
                        ValidationIssue(
                            "error",
                            "selection_policy='all' requires correct_options",
                            unit_key,
                            exercise_index,
                            idx,
                        )
                    )
            canonical = str(item.get("canonical") or "")
            parts = _canonical_parts(canonical)
            if item_type == "mcq" and len(parts) > 1:
                if all(norm_cmp_text(part) in option_set for part in parts):
                    issues.append(
                        ValidationIssue(
                            "error",
                            "mcq canonical matches multiple options",
                            unit_key,
                            exercise_index,
                            idx,
                        )
                    )
            if (
                item_type == "multiselect"
                and selection_policy is None
                and len(parts) > 1
                and all(norm_cmp_text(part) in option_set for part in parts)
            ):
                issues.append(
                    ValidationIssue(
                        "warning",
                        "multiselect canonical has multiple options without selection_policy",
                        unit_key,
                        exercise_index,
                        idx,
                    )
                )
    return issues
