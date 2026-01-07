import argparse
import asyncio
import datetime as dt
import logging
import os
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv

from bot.autotest import AutotestRunner, RunnerConfig


def _timestamp() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%d_%H%M%S")


def _build_db_url(path: Path) -> str:
    return f"sqlite+aiosqlite:///{path}"


def main(argv: list[str]) -> int:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="./data/app.db")
    parser.add_argument("--inplace", action="store_true", default=False)
    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--user-id", type=int, default=999000111)
    parser.add_argument("--mistake-min", type=int, default=10)
    parser.add_argument("--mistake-max", type=int, default=30)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--max-same-item", type=int, default=6)
    parser.add_argument("--max-no-progress", type=int, default=20)
    parser.add_argument("--timeout-sec", type=float, default=15.0)
    parser.add_argument("--log-dir", default="./logs")
    parser.add_argument("--dialogue-log")
    parser.add_argument("--problem-dialogue-log")
    parser.add_argument("--dialogue-context", type=int, default=3)
    parser.add_argument("--dialogue-max-options", type=int, default=8)
    parser.add_argument("--dialogue-max-chars", type=int, default=1800)
    parser.add_argument(
        "--dialogue-include-jsonl-ref",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--mode", choices=["sweep"], default="sweep")
    args = parser.parse_args(argv)

    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_API_KEY or GEMINI_API_KEY is required")
        return 1
    model = os.getenv("LLM_MODEL", "gemini-3-flash-preview").strip() or "gemini-3-flash-preview"

    timestamp = _timestamp()
    db_path = Path(args.db).resolve()
    if not db_path.exists():
        print(f"ERROR: DB not found at {db_path}")
        return 1

    if args.inplace:
        run_db_path = db_path
    else:
        copy_path = db_path.parent / f"app_autotest_copy_{timestamp}.db"
        shutil.copy2(db_path, copy_path)
        run_db_path = copy_path

    log_dir = Path(args.log_dir)
    dialogue_log_path = Path(args.dialogue_log) if args.dialogue_log else log_dir / f"autotest_{timestamp}_dialogue.txt"
    problem_dialogue_log_path = (
        Path(args.problem_dialogue_log)
        if args.problem_dialogue_log
        else log_dir / f"autotest_{timestamp}_dialogue_problems.txt"
    )
    logger.info("Autotest starting (model=%s, attempts=%s, inplace=%s)", model, args.n, args.inplace)
    config = RunnerConfig(
        db_url=_build_db_url(run_db_path),
        total_attempts=args.n,
        user_id=args.user_id,
        model=model,
        mistake_min=args.mistake_min,
        mistake_max=args.mistake_max,
        seed=args.seed,
        max_same_item=args.max_same_item,
        max_no_progress=args.max_no_progress,
        timeout_sec=args.timeout_sec,
        log_dir=log_dir,
        run_id=timestamp,
        dialogue_log_path=dialogue_log_path,
        problem_dialogue_log_path=problem_dialogue_log_path,
        dialogue_context=args.dialogue_context,
        dialogue_max_options=args.dialogue_max_options,
        dialogue_max_chars=args.dialogue_max_chars,
        dialogue_include_jsonl_ref=args.dialogue_include_jsonl_ref,
        mode=args.mode,
    )
    runner = AutotestRunner(config, api_key=api_key)
    return asyncio.run(runner.run())


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
