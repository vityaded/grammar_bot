from bot.handlers import build_feedback_message


def test_feedback_almost_label_acceptance():
    kwargs = build_feedback_message("almost", "answer", "answer", "en", "normal")
    assert "Almost (accepted)" in kwargs["text"]


def test_feedback_almost_label_strict():
    kwargs = build_feedback_message("almost", "answer", "answer", "en", "strict")
    assert "counts as wrong" in kwargs["text"]


def test_feedback_message_allows_special_chars():
    text = r"test (a) [b] _x_ *y* . ! \\ # + - = | { } ~ ` >"
    kwargs = build_feedback_message("wrong", text, text, "en", "normal")
    assert text in kwargs["text"]
    assert "parse_mode" not in kwargs or kwargs["parse_mode"] is None
    assert "entities" in kwargs
