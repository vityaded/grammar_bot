from bot.autotest.runner import format_bot_message
from bot.autotest.types import QuestionContext


def test_format_bot_message_includes_instruction_question_options() -> None:
    ctx = QuestionContext(
        unit_key="unit_1",
        exercise_index=2,
        item_idx=3,
        exercise_type="mcq",
        instruction="Use the correct form.\nExample: I am here.\nAnswer: am",
        item={
            "prompt": "She ___ here.",
            "options": ["is", "are"],
        },
        question_key="unit_1:2:3",
        acceptance_mode="normal",
    )
    message = format_bot_message(ctx, max_options=8, max_chars=1800)
    assert "Instruction:" in message
    assert "Question:" in message
    assert "Options:" in message
    assert "A) is" in message
    assert "B) are" in message
