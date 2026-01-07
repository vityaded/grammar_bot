#!/usr/bin/env python3
"""
fix_exercise.py

Regenerate and repair exercise instructions in unit_exercises_v2.json using Gemini,
exercise-by-exercise, based on the exercise items themselves.

What it changes:
- ONLY modifies: exercise["instruction"] (string).
- Example/Answer are built deterministically from an existing item (no invented sentences).

Auth / config (loaded from .env by default):
- GEMINI_API_KEY=...
- LLM_MODEL=gemini-...

Precedence:
1) CLI flags (if provided)
2) .env variables
3) OS environment variables
4) defaults (model only)

Requires:
  pip install google-genai pydantic python-dotenv
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from dotenv import load_dotenv

# Google GenAI SDK
from google import genai
from google.genai import types


LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


# ----------------------------
# Env loading
# ----------------------------

def load_env_upwards() -> Optional[str]:
    """
    Loads .env from the current working directory or the nearest parent directory.
    Returns the resolved .env path if found; otherwise None.
    """
    cwd = Path.cwd().resolve()
    for p in [cwd] + list(cwd.parents):
        env_path = p / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=False)
            return str(env_path)
    # no .env found; still load_dotenv (no-op for file) so user can rely on env vars
    load_dotenv(override=False)
    return None


def get_env(name: str) -> str:
    return (os.getenv(name) or "").strip()


# ----------------------------
# Utilities
# ----------------------------

def now_ts() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def json_dump(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = s.replace("’", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"[.?!]+$", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def safe_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    return str(x)


def is_nonempty_list(x: Any) -> bool:
    return isinstance(x, list) and len(x) > 0


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


# ----------------------------
# Gemini output schema
# ----------------------------

class GeminiExerciseFix(BaseModel):
    instruction_head: str = Field(
        ...,
        description=(
            "A short instruction that matches ALL items in the exercise. "
            "Do NOT include 'Example:' or 'Answer:' lines."
        ),
    )
    example_item_index: int = Field(
        ...,
        ge=1,
        description="1-based index of an item to be used as the example.",
    )
    notes: Optional[List[str]] = Field(
        default=None,
        description="Optional brief notes/warnings.",
    )


# ----------------------------
# JSON traversal
# ----------------------------

@dataclass
class ExerciseRef:
    unit_key: str
    exercise_index: int
    exercise_type: str
    exercise_obj: Dict[str, Any]  # mutated in-place
    path_hint: str


def iter_exercises(root: Any) -> List[ExerciseRef]:
    """
    Supports common shapes:

    A) [
         { "unit_key": "...", "exercises": [ {...}, {...} ] },
         ...
       ]

    B) { "units": [ { "unit_key": "...", "exercises": [ ... ] }, ... ] }

    C) { "exercises": [ ... ] }  (unit_key inside exercise or omitted)
    """
    out: List[ExerciseRef] = []

    def add_ex(unit_key: str, ex: Dict[str, Any], unit_i: Optional[int], ex_i: int) -> None:
        exercise_index = int(ex.get("exercise_index", ex_i))
        exercise_type = safe_str(ex.get("exercise_type", ex.get("item_type", ""))).strip()
        path_hint = f"unit[{unit_i}]::exercise[{ex_i}]" if unit_i is not None else f"exercise[{ex_i}]"
        out.append(
            ExerciseRef(
                unit_key=unit_key,
                exercise_index=exercise_index,
                exercise_type=exercise_type,
                exercise_obj=ex,
                path_hint=path_hint,
            )
        )

    if isinstance(root, list):
        for ui, unit in enumerate(root):
            if not isinstance(unit, dict):
                continue
            unit_key = safe_str(unit.get("unit_key", f"unit_{ui+1}"))
            exercises = unit.get("exercises")
            if isinstance(exercises, list):
                for ei, ex in enumerate(exercises):
                    if isinstance(ex, dict):
                        add_ex(unit_key, ex, ui, ei)
        return out

    if isinstance(root, dict):
        if isinstance(root.get("units"), list):
            for ui, unit in enumerate(root["units"]):
                if not isinstance(unit, dict):
                    continue
                unit_key = safe_str(unit.get("unit_key", f"unit_{ui+1}"))
                exercises = unit.get("exercises")
                if isinstance(exercises, list):
                    for ei, ex in enumerate(exercises):
                        if isinstance(ex, dict):
                            add_ex(unit_key, ex, ui, ei)
            return out

        if isinstance(root.get("exercises"), list):
            for ei, ex in enumerate(root["exercises"]):
                if not isinstance(ex, dict):
                    continue
                unit_key = safe_str(ex.get("unit_key", "unknown_unit"))
                add_ex(unit_key, ex, None, ei)
            return out

    raise ValueError("Unsupported JSON shape for unit_exercises_v2.json")


# ----------------------------
# Answer-format and example building
# ----------------------------

def detect_answer_format(exercise_type: str, items: List[Dict[str, Any]]) -> str:
    """
    Returns one of: 'text', 'letter', 'letters_csv'
    """
    et = (exercise_type or "").strip().lower()
    has_options = any(is_nonempty_list(it.get("options")) for it in items)

    if et in {"multiselect", "multi_select"}:
        return "letters_csv"
    if et in {"multichoice", "multi_choice", "single_choice", "choice"}:
        return "letter"
    if has_options:
        return "letter"
    return "text"


def format_line_for_answer_format(fmt: str) -> str:
    if fmt == "letters_csv":
        return "Reply with the letters in order, separated by commas (e.g., A, C)."
    if fmt == "letter":
        return "Reply with one letter (A, B, C, D)."
    return ""


def options_with_letters(options: List[str]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for i, opt in enumerate(options):
        if i >= len(LETTERS):
            break
        out.append((LETTERS[i], safe_str(opt)))
    return out


def canonical_targets(canonical: Any, fmt: str) -> List[str]:
    if canonical is None:
        return []

    if isinstance(canonical, list):
        return [safe_str(x).strip() for x in canonical if safe_str(x).strip()]

    c = safe_str(canonical).strip()
    if not c:
        return []

    if fmt == "letters_csv":
        if re.fullmatch(r"[A-Z](\s*,\s*[A-Z])+", c.strip().upper()):
            return [c.strip().upper()]
        if "," in c:
            parts = [p.strip() for p in c.split(",") if p.strip()]
            if len(parts) >= 2:
                return parts

    return [c]


def map_canonical_to_letters(canonical: Any, options: List[str], fmt: str) -> Optional[str]:
    if not is_nonempty_list(options):
        return None

    opt_norm = [normalize_text(safe_str(o)) for o in options]
    targets = canonical_targets(canonical, fmt)
    if not targets:
        return None

    # canonical already letters?
    if fmt == "letter" and len(targets) == 1 and re.fullmatch(r"[A-Z]", targets[0].strip().upper()):
        return targets[0].strip().upper()
    if fmt == "letters_csv" and len(targets) == 1 and re.fullmatch(r"[A-Z](\s*,\s*[A-Z])+", targets[0].strip().upper()):
        return targets[0].strip().upper()

    letters: List[str] = []
    for t in targets:
        t_norm = normalize_text(t)
        if t_norm in opt_norm:
            idx = opt_norm.index(t_norm)
            if idx < len(LETTERS):
                letters.append(LETTERS[idx])
        else:
            return None

    if not letters:
        return None

    if fmt == "letter":
        return letters[0]
    return ", ".join(letters)


def build_example_block(item: Dict[str, Any], fmt: str) -> Tuple[str, str]:
    """
    Returns (example_question, example_answer) built deterministically from the item.
    """
    prompt = safe_str(item.get("prompt")).strip()
    canonical = item.get("canonical")
    options = item.get("options") or []

    if fmt in {"letter", "letters_csv"}:
        mapped = map_canonical_to_letters(canonical, options, fmt)
        if mapped:
            return prompt, mapped
        return prompt, safe_str(canonical).strip()

    return prompt, safe_str(canonical).strip()


def strip_example_answer_lines(instruction_head: str) -> str:
    """
    Defensive: remove accidental Example/Answer lines Gemini might add.
    """
    lines = [ln.rstrip() for ln in instruction_head.splitlines()]
    kept: List[str] = []
    for ln in lines:
        if re.match(r"^\s*(example|answer)\s*:", ln, re.IGNORECASE):
            continue
        kept.append(ln)
    while kept and not kept[-1].strip():
        kept.pop()
    return "\n".join(kept).strip()


# ----------------------------
# Gemini call (one per exercise)
# ----------------------------

def gemini_fix_one_exercise(
    client: genai.Client,
    model: str,
    ex_ref: ExerciseRef,
    items: List[Dict[str, Any]],
    answer_format: str,
    max_items_in_prompt: int,
    temperature: float,
    retries: int,
    sleep_between: float,
    log_jsonl_path: Optional[Path],
) -> Tuple[str, int, List[str]]:
    """
    Returns: (instruction_head, example_item_index_1based, notes)
    """
    fmt_line = format_line_for_answer_format(answer_format)

    prompt_items: List[Dict[str, Any]] = []
    for i, it in enumerate(items[:max_items_in_prompt], start=1):
        opt = it.get("options")
        opt_pairs = options_with_letters(opt) if is_nonempty_list(opt) else None
        prompt_items.append(
            {
                "i": i,
                "prompt": safe_str(it.get("prompt")).strip(),
                "options": opt_pairs,
                "canonical": safe_str(it.get("canonical")).strip(),
            }
        )

    payload = {
        "unit_key": ex_ref.unit_key,
        "exercise_index": ex_ref.exercise_index,
        "exercise_type": ex_ref.exercise_type,
        "answer_format": answer_format,
        "format_line": fmt_line,
        "items": prompt_items,
        "constraints": [
            "Write ONE instruction_head that matches ALL items in this exercise.",
            "Do NOT include 'Example:' or 'Answer:' lines in instruction_head.",
            "Do NOT invent new sentences; examples will be added by the script.",
            "If items require multiple forms, make instruction inclusive (e.g., 'present perfect simple or continuous').",
        ],
    }

    system_text = (
        "You are a dataset editor for a grammar-learning Telegram bot.\n"
        "Task: write a corrected instruction_head that matches ALL items in the exercise.\n"
        "Keep it short, clear, and accurate. Do not add Example/Answer lines.\n"
        "Return JSON strictly matching the schema."
    )

    user_text = "Exercise JSON:\n" + json.dumps(payload, ensure_ascii=False, indent=2)
    payload_size = len(user_text.encode("utf-8"))

    config = types.GenerateContentConfig(
        temperature=temperature,
        response_mime_type="application/json",
        response_schema=GeminiExerciseFix,
    )

    last_err: Optional[str] = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            log(f"Gemini call: payload_bytes={payload_size}, attempt={attempt}, model={model}")
            start_time = time.monotonic()
            resp = client.models.generate_content(
                model=model,
                contents=[system_text, user_text],
                config=config,
            )
            elapsed = time.monotonic() - start_time

            parsed = getattr(resp, "parsed", None)
            if parsed is None:
                data = json.loads(resp.text)
                parsed = GeminiExerciseFix(**data)

            head = strip_example_answer_lines(parsed.instruction_head or "")
            ex_idx = int(parsed.example_item_index)
            notes = parsed.notes or []
            log(
                "Gemini success: elapsed_seconds="
                f"{elapsed:.2f}, example_item_index={ex_idx}, notes_returned={bool(notes)}"
            )

            if log_jsonl_path:
                log_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
                with log_jsonl_path.open("a", encoding="utf-8") as f:
                    f.write(
                        json.dumps(
                            {
                                "ts": now_ts(),
                                "unit_key": ex_ref.unit_key,
                                "exercise_index": ex_ref.exercise_index,
                                "exercise_type": ex_ref.exercise_type,
                                "answer_format": answer_format,
                                "model": model,
                                "instruction_head": head,
                                "example_item_index": ex_idx,
                                "notes": notes,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

            if sleep_between > 0:
                time.sleep(sleep_between)
                log(f"Sleep between exercises: seconds={sleep_between}")

            return head, ex_idx, notes

        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            will_retry = attempt < retries
            log(f"Gemini error: {last_err}; will_retry={will_retry}")
            if attempt < retries:
                time.sleep(min(2.0 * attempt, 8.0))
                continue
            raise RuntimeError(f"Gemini failed after {retries} attempts: {last_err}") from e

    raise RuntimeError(f"Gemini failed: {last_err}")


# ----------------------------
# Instruction composition
# ----------------------------

def default_fallback_instruction(answer_format: str) -> str:
    if answer_format in {"letter", "letters_csv"}:
        return "Choose the correct option."
    return "Complete the sentences."


def compose_final_instruction(instruction_head: str, answer_format: str, example_q: str, example_a: str) -> str:
    head = instruction_head.strip() if instruction_head.strip() else default_fallback_instruction(answer_format)
    fmt_line = format_line_for_answer_format(answer_format)
    # Keep the format line (inside instruction) to prevent UI/acceptance confusion on option tasks.
    if fmt_line:
        head = head.rstrip() + "\n" + fmt_line
    return f"{head}\nExample: {example_q}\nAnswer: {example_a}".strip()


# ----------------------------
# Safety: only-instruction-changed check
# ----------------------------

def strip_instructions(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "instruction":
                out[k] = "__INSTRUCTION__"
            else:
                out[k] = strip_instructions(v)
        return out
    if isinstance(obj, list):
        return [strip_instructions(x) for x in obj]
    return obj


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    env_path = load_env_upwards()
    log(f"Loaded env path: {env_path or 'not found'}")

    ap = argparse.ArgumentParser(description="Fix unit_exercises_v2.json instruction fields via Gemini (exercise-by-exercise).")
    ap.add_argument("--in", dest="in_path", default="data/unit_exercises_v2.json", help="Input JSON path")
    ap.add_argument("--out", dest="out_path", default="", help="Output JSON path (default: overwrite input with backup)")
    ap.add_argument("--model", default="", help="Model override (else LLM_MODEL/GEMINI_MODEL from .env)")
    ap.add_argument("--api-key", default="", help="API key override (else GEMINI_API_KEY from .env)")
    ap.add_argument("--max-items-in-prompt", type=int, default=12, help="Max items included in each Gemini prompt")
    ap.add_argument("--temperature", type=float, default=0.0, help="Generation temperature")
    ap.add_argument("--retry", type=int, default=3, help="Retries per exercise")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between exercises (rate limiting)")
    ap.add_argument("--max-exercises", type=int, default=0, help="Process at most N exercises (0 = all)")
    ap.add_argument("--only-unit", action="append", default=[], help="Restrict to a unit_key (can repeat)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write output JSON; still write reports")
    ap.add_argument("--log-jsonl", default="reports/fix_exercise_calls.jsonl", help="JSONL log path")
    ap.add_argument("--patch-report", default="", help="Patch report JSON path (default: reports/fix_exercise_patch_<ts>.json)")
    ap.add_argument("--no-log", action="store_true", help="Disable JSONL call logging")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    if not in_path.exists():
        print(f"ERROR: input not found: {in_path}", file=sys.stderr)
        return 2

    out_path = Path(args.out_path) if args.out_path else in_path
    patch_report_path = Path(args.patch_report) if args.patch_report else Path(f"reports/fix_exercise_patch_{now_ts()}.json")
    log_jsonl_path = None if args.no_log else Path(args.log_jsonl)

    # Resolve config with required .env names:
    # - GEMINI_API_KEY
    # - LLM_MODEL
    cli_key = (args.api_key or "").strip()
    cli_model = (args.model or "").strip()

    api_key = cli_key or get_env("GEMINI_API_KEY") or get_env("LLM_API_KEY")
    model = cli_model or get_env("LLM_MODEL") or get_env("GEMINI_MODEL") or "gemini-2.5-flash"
    log(f"Resolved config: model={model}, api_key_found={bool(api_key)}")

    if not api_key:
        print(
            "ERROR: Missing API key. Set GEMINI_API_KEY in .env or environment, or pass --api-key.\n"
            f"  .env loaded from: {env_path or 'not found'}",
            file=sys.stderr,
        )
        return 2

    client = genai.Client(api_key=api_key)

    root = read_json(in_path)
    if isinstance(root, list):
        top_level_label = "units"
        top_level_count = len(root)
    elif isinstance(root, dict) and isinstance(root.get("units"), list):
        top_level_label = "units"
        top_level_count = len(root["units"])
    elif isinstance(root, dict) and isinstance(root.get("exercises"), list):
        top_level_label = "exercises"
        top_level_count = len(root["exercises"])
    else:
        top_level_label = "unknown"
        top_level_count = 0
    log(f"Loaded JSON: path={in_path} top_level_{top_level_label}={top_level_count}")
    root_before = copy.deepcopy(root)

    exercises = iter_exercises(root)
    if args.only_unit:
        only = set(args.only_unit)
        exercises = [e for e in exercises if e.unit_key in only]
    if args.max_exercises and args.max_exercises > 0:
        exercises = exercises[: args.max_exercises]

    patches: List[Dict[str, Any]] = []
    processed = 0
    changed = 0

    total_exercises = len(exercises)
    log(f"Exercises to process: total={total_exercises}")

    for current_index, ex_ref in enumerate(exercises, start=1):
        log(
            "Exercise start: "
            f"{current_index}/{total_exercises} unit_key={ex_ref.unit_key} "
            f"exercise_index={ex_ref.exercise_index} exercise_type={ex_ref.exercise_type}"
        )
        ex = ex_ref.exercise_obj
        items = ex.get("items") or []
        if not isinstance(items, list) or not items:
            log(
                "Skipping exercise: missing items list or empty items "
                f"unit_key={ex_ref.unit_key} exercise_index={ex_ref.exercise_index}"
            )
            continue

        processed += 1

        old_instruction = safe_str(ex.get("instruction"))
        exercise_type = ex_ref.exercise_type or safe_str(ex.get("exercise_type", ""))
        answer_format = detect_answer_format(exercise_type, items)

        try:
            head, example_idx_1b, notes = gemini_fix_one_exercise(
                client=client,
                model=model,
                ex_ref=ex_ref,
                items=items,
                answer_format=answer_format,
                max_items_in_prompt=max(1, int(args.max_items_in_prompt)),
                temperature=float(args.temperature),
                retries=max(1, int(args.retry)),
                sleep_between=float(args.sleep),
                log_jsonl_path=log_jsonl_path,
            )
        except Exception as e:
            patches.append(
                {
                    "unit_key": ex_ref.unit_key,
                    "exercise_index": ex_ref.exercise_index,
                    "path_hint": ex_ref.path_hint,
                    "status": "error",
                    "error": f"{type(e).__name__}: {e}",
                }
            )
            continue

        if example_idx_1b < 1 or example_idx_1b > len(items):
            example_idx_1b = 1
            notes = (notes or []) + ["example_item_index_out_of_range -> coerced_to_1"]

        example_item = items[example_idx_1b - 1]
        ex_q, ex_a = build_example_block(example_item, answer_format)
        new_instruction = compose_final_instruction(head, answer_format, ex_q, ex_a)

        if new_instruction != old_instruction:
            ex["instruction"] = new_instruction
            changed += 1
            status = "patched"
        else:
            status = "unchanged"

        patches.append(
            {
                "unit_key": ex_ref.unit_key,
                "exercise_index": ex_ref.exercise_index,
                "exercise_type": exercise_type,
                "answer_format": answer_format,
                "path_hint": ex_ref.path_hint,
                "status": status,
                "old_instruction": old_instruction,
                "new_instruction": new_instruction,
                "example_item_index": example_idx_1b,
                "notes": notes or [],
            }
        )

    # Safety check: ONLY instruction fields changed
    before_stripped = strip_instructions(root_before)
    after_stripped = strip_instructions(root)
    if before_stripped != after_stripped:
        print("ERROR: Non-instruction fields were modified. Aborting write.", file=sys.stderr)
        json_dump(
            patch_report_path,
            {
                "ts": now_ts(),
                "status": "aborted_non_instruction_change",
                "processed_exercises": processed,
                "changed_exercises": changed,
                "input": str(in_path),
                "output": str(out_path),
                "model": model,
                "env_path": env_path,
                "patches_preview": patches[:50],
            },
        )
        log(
            "Patch report written (aborted): "
            f"path={patch_report_path} processed={processed} changed={changed}"
        )
        return 3

    # Patch report
    json_dump(
        patch_report_path,
        {
            "ts": now_ts(),
            "status": "ok",
            "input": str(in_path),
            "output": str(out_path),
            "model": model,
            "env_path": env_path,
            "processed_exercises": processed,
            "changed_exercises": changed,
            "patches": patches,
        },
    )
    log(f"Patch report written: path={patch_report_path} processed={processed} changed={changed}")

    if args.dry_run:
        print(f"DRY RUN: processed={processed}, changed={changed}")
        print(f"Patch report: {patch_report_path}")
        return 0

    # If overwriting input, create backup
    if out_path.resolve() == in_path.resolve():
        backup = in_path.with_suffix(in_path.suffix + f".bak_{now_ts()}")
        backup.write_text(in_path.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"Backup written: {backup}")

    json_dump(out_path, root)
    log(f"Output JSON written: path={out_path} processed={processed} changed={changed}")

    print(f"Done. processed={processed}, changed={changed}")
    print(f"Output JSON: {out_path}")
    print(f"Patch report: {patch_report_path}")
    if log_jsonl_path:
        print(f"JSONL log: {log_jsonl_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
