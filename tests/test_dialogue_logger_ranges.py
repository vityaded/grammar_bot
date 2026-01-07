from pathlib import Path

from bot.autotest.dialogue_logger import DialogueLogger
from bot.autotest.types import Issue, Turn


def test_dialogue_logger_merges_overlapping_ranges(tmp_path: Path) -> None:
    dialogue_path = tmp_path / "dialogue.txt"
    problems_path = tmp_path / "problems.txt"
    logger = DialogueLogger(
        dialogue_path=dialogue_path,
        problems_path=problems_path,
        context_turns=2,
        max_options=8,
        max_chars=1800,
        include_ref=False,
    )
    for idx in range(10):
        logger.append_turn(
            Turn(
                turn_id=idx,
                question_key=f"q{idx}",
                bot_message=f"q{idx}",
                user_message=f"answer {idx}",
                bot_feedback="Verdict: correct",
                issues=[],
                jsonl_ref=None,
            )
        )
    logger.mark_problem(2, Issue("ISSUE_A", "error", "a"))
    logger.mark_problem(4, Issue("ISSUE_B", "error", "b"))
    logger.finalize()

    contents = problems_path.read_text(encoding="utf-8")
    assert "----- PROBLEM CONTEXT (turns 0..6) -----" in contents
    assert "q0" in contents
    assert "q6" in contents
    assert "Issues: ISSUE_A, ISSUE_B" in contents
