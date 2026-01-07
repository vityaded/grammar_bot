#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

EXAMPLE_LINE_RE = re.compile(r"^\s*example:", re.IGNORECASE)
ANSWER_LINE_RE = re.compile(r"^\s*answer:", re.IGNORECASE)
PAREN_SLASH_RE = re.compile(r"\([^()]*?/[^()]*?\)")
WORD_ORDER_RE = re.compile(r"put .* in the correct (place|order)")


FAMILY_FALLBACKS = {
    "short_answer": "Write short answers.",
    "sentence_build": "Write a sentence.",
    "word_order": "Put the words in the correct order.",
    "choice": "Choose the correct answer.",
    "unknown": "Complete the exercise.",
}


def split_example_block(instruction: str) -> Tuple[str, Optional[str], Optional[int]]:
    lines = instruction.splitlines()
    for idx, line in enumerate(lines):
        if EXAMPLE_LINE_RE.match(line):
            head = "\n".join(lines[:idx])
            example_block = "\n".join(lines[idx:])
            return head, example_block, idx
    return instruction, None, None


def find_answer_line_index(instruction: str) -> Optional[int]:
    lines = instruction.splitlines()
    for idx, line in enumerate(lines):
        if ANSWER_LINE_RE.match(line):
            return idx
    return None


def infer_prompt_family(items: List[Dict[str, Any]]) -> str:
    for item in items:
        options = item.get("options")
        if isinstance(options, list) and len(options) > 0:
            return "choice"

    for item in items:
        prompt = str(item.get("prompt", ""))
        lower = prompt.lower()
        if "write the short answer" in lower or "short answer" in lower:
            return "short_answer"

    for item in items:
        prompt = str(item.get("prompt", ""))
        if PAREN_SLASH_RE.search(prompt):
            return "sentence_build"

    for item in items:
        prompt = str(item.get("prompt", ""))
        lower = prompt.lower()
        if WORD_ORDER_RE.search(lower):
            return "word_order"

    return "unknown"


def infer_example_family(example_block: Optional[str]) -> str:
    if not example_block:
        return "unknown"
    lower = example_block.lower()
    if "write a sentence" in lower or PAREN_SLASH_RE.search(example_block):
        return "sentence_build"
    if "short answer" in lower:
        return "short_answer"
    if WORD_ORDER_RE.search(lower):
        return "word_order"
    return "unknown"


def normalize_instruction_head(head: str) -> str:
    trimmed = head.strip()
    if not trimmed:
        return ""
    if "\n" not in trimmed and trimmed[-1] not in ".!?":
        return f"{trimmed}."
    return trimmed


def should_remove_for_answer_block(
    example_block: Optional[str],
    example_start_idx: Optional[int],
    answer_idx: Optional[int],
    prompt_family: str,
) -> bool:
    if not example_block or example_start_idx is None or answer_idx is None:
        return False
    if prompt_family == "choice":
        return False
    if answer_idx < example_start_idx:
        return False
    return EXAMPLE_LINE_RE.search(example_block) and ANSWER_LINE_RE.search(example_block)


def build_report_entry(
    exercise: Dict[str, Any],
    prompt_family: str,
    example_family: str,
    reason: str,
    old_instruction: str,
    new_instruction: str,
    did_modify: bool,
) -> Dict[str, Any]:
    items = exercise.get("items", [])
    first_prompt = ""
    if items:
        first_prompt = str(items[0].get("prompt", ""))
    return {
        "unit_key": exercise.get("unit_key"),
        "exercise_index": exercise.get("exercise_index"),
        "exercise_type": exercise.get("exercise_type"),
        "prompt_family": prompt_family,
        "example_family": example_family,
        "reason": reason,
        "old_instruction": old_instruction,
        "new_instruction": new_instruction,
        "first_prompt": first_prompt,
        "did_modify": did_modify,
    }


def summarize_instruction(text: str, max_len: int = 180) -> str:
    flattened = " ".join(text.split())
    if len(flattened) <= max_len:
        return flattened
    return f"{flattened[:max_len - 3]}..."


def process_exercises(exercises: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    reports: List[Dict[str, Any]] = []
    modified_indices: List[int] = []

    for idx, exercise in enumerate(exercises):
        instruction = str(exercise.get("instruction", ""))
        head, example_block, example_start_idx = split_example_block(instruction)
        answer_idx = find_answer_line_index(instruction)

        prompt_family = infer_prompt_family(exercise.get("items", []))
        example_family = infer_example_family(example_block)

        has_example_block = example_block is not None
        mismatch = (
            has_example_block
            and prompt_family != "unknown"
            and example_family != "unknown"
            and prompt_family != example_family
        )

        remove_for_answer = should_remove_for_answer_block(
            example_block, example_start_idx, answer_idx, prompt_family
        )

        did_modify = False
        new_instruction = instruction
        report_reason = None

        if mismatch:
            report_reason = "example_mismatch"
        elif answer_idx is not None:
            report_reason = "answer_block_in_instruction"

        if mismatch or remove_for_answer:
            new_head = normalize_instruction_head(head)
            if not new_head:
                fallback_family = prompt_family if prompt_family in FAMILY_FALLBACKS else "unknown"
                new_head = FAMILY_FALLBACKS[fallback_family]
            new_instruction = new_head
            did_modify = True
            modified_indices.append(idx)
        elif report_reason:
            new_instruction = instruction

        if report_reason:
            reports.append(
                build_report_entry(
                    exercise,
                    prompt_family,
                    example_family,
                    report_reason,
                    instruction,
                    new_instruction,
                    did_modify,
                )
            )

        if did_modify:
            exercise["instruction"] = new_instruction

    return exercises, reports


def validate_outputs(
    original_instructions: List[str],
    updated: List[Dict[str, Any]],
    reports: List[Dict[str, Any]],
) -> None:
    if len(original_instructions) != len(updated):
        raise AssertionError("Exercise count mismatch after update.")

    modified = {
        idx
        for idx, exercise in enumerate(updated)
        if original_instructions[idx] != exercise.get("instruction")
    }

    for idx in modified:
        instruction = str(updated[idx].get("instruction", ""))
        for line in instruction.splitlines():
            if EXAMPLE_LINE_RE.match(line):
                raise AssertionError("Modified instruction still contains Example line.")

    for exercise in updated:
        if exercise.get("unit_key") == "unit_2" and exercise.get("exercise_index") == 5:
            instruction = str(exercise.get("instruction", ""))
            if "Write a sentence:" in instruction:
                raise AssertionError("unit_2 exercise_index 5 still contains 'Write a sentence:'")

    _ = reports


def build_markdown_report(reports: List[Dict[str, Any]]) -> str:
    totals = {}
    for entry in reports:
        totals[entry["reason"]] = totals.get(entry["reason"], 0) + 1

    lines = ["# Exercises mismatch report", "", "## Totals"]
    if totals:
        for reason, count in sorted(totals.items()):
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("- No issues found")

    lines.extend(["", "## Details"])
    if not reports:
        lines.append("- No flagged exercises")
        return "\n".join(lines) + "\n"

    for entry in reports:
        unit_key = entry.get("unit_key")
        exercise_index = entry.get("exercise_index")
        exercise_type = entry.get("exercise_type")
        reason = entry.get("reason")
        old_summary = summarize_instruction(entry.get("old_instruction", ""))
        new_summary = summarize_instruction(entry.get("new_instruction", ""))
        lines.append(
            f"- {unit_key} ex {exercise_index} ({exercise_type}) [{reason}]\n"
            f"  - Instruction: \"{old_summary}\" → \"{new_summary}\""
        )

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify and fix exercises JSON.")
    parser.add_argument("input_json", type=Path)
    parser.add_argument("--check", action="store_true", help="Check only; do not write outputs.")
    args = parser.parse_args()

    data = json.loads(args.input_json.read_text(encoding="utf-8"))
    exercises = data.get("exercises", [])
    original_instructions = [str(exercise.get("instruction", "")) for exercise in exercises]

    updated_exercises, reports = process_exercises(exercises)

    if args.check:
        if reports:
            return 2
        return 0

    output_path = Path("data/unit_exercises_v2.fixed.json")
    report_json_path = Path("reports/exercises_mismatch_report.json")
    report_md_path = Path("reports/exercises_mismatch_report.md")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_json_path.parent.mkdir(parents=True, exist_ok=True)

    output_data = {"exercises": updated_exercises}
    output_path.write_text(
        json.dumps(output_data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    report_json_path.write_text(
        json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    report_md_path.write_text(build_markdown_report(reports), encoding="utf-8")

    reloaded = json.loads(output_path.read_text(encoding="utf-8"))
    validate_outputs(original_instructions, reloaded.get("exercises", []), reports)

    return 0


if __name__ == "__main__":
    sys.exit(main())
