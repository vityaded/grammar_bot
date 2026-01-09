from bot.handlers import build_feedback_message


def test_feedback_correct_label():
    kwargs = build_feedback_message("correct", "answer", "answer", "en", "normal")
    assert "Correct" in kwargs["text"]


def test_feedback_wrong_label():
    kwargs = build_feedback_message("wrong", "answer", "answer", "en", "strict")
    assert "Wrong" in kwargs["text"]


def test_feedback_message_allows_special_chars():
    text = r"test (a) [b] _x_ *y* . ! \\ # + - = | { } ~ ` >"
    kwargs = build_feedback_message("wrong", text, text, "en", "normal")
    assert text in kwargs["text"]
    assert "parse_mode" not in kwargs or kwargs["parse_mode"] is None
    assert "entities" in kwargs
