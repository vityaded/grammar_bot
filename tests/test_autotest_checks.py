from bot.autotest.checks import gather_issues
from bot.autotest.types import QuestionContext


def _make_ctx(*, instruction: str, prompt: str, options: list[str] | None = None) -> QuestionContext:
    item: dict[str, object] = {"prompt": prompt, "canonical": "x"}
    if options is not None:
        item["options"] = options
    return QuestionContext(
        unit_key="unit_1",
        exercise_index=1,
        item_idx=1,
        exercise_type="gap_fill",
        instruction=instruction,
        item=item,
        question_key="unit_1:1:1",
        acceptance_mode="normal",
    )


def test_gap_fill_example_matches_gap_fill_prompt() -> None:
    ctx = _make_ctx(
        instruction="Complete: (work) My neighbour ____ every weekend.\nExample: Complete: (run) My neighbour ____ every weekend.",
        prompt="My neighbour ____ every weekend.",
    )
    issues = gather_issues(ctx)
    assert not any(issue.issue_type == "EXAMPLE_TASK_MISMATCH" for issue in issues)


def test_choice_example_matches_choice_prompt() -> None:
    ctx = _make_ctx(
        instruction=(
            "Choose the correct option.\n"
            "Example: A) alpha\nB) bravo\nC) charlie\nD) delta"
        ),
        prompt="Pick one.",
        options=["alpha", "bravo", "charlie", "delta"],
    )
    issues = gather_issues(ctx)
    assert not any(issue.issue_type == "EXAMPLE_TASK_MISMATCH" for issue in issues)


def test_choose_or_is_choice_implicit_without_format_mismatch() -> None:
    ctx = _make_ctx(
        instruction="Choose good or well.",
        prompt="He did the job ____.",
    )
    issues = gather_issues(ctx)
    assert not any(issue.issue_type == "INSTRUCTION_FORMAT_MISMATCH" for issue in issues)


def test_example_answer_suspicious_is_flagged() -> None:
    ctx = _make_ctx(
        instruction="Example: I have dinner. Answer: have ate",
        prompt="I have dinner.",
    )
    issues = gather_issues(ctx)
    assert any(issue.issue_type == "EXAMPLE_ANSWER_SUSPICIOUS" for issue in issues)
