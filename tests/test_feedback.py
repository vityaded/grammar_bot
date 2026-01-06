from bot.handlers import _feedback_text


def test_feedback_almost_label_acceptance():
    text = _feedback_text("almost", "answer", "answer", "en", "normal")
    assert "Almost (accepted)" in text


def test_feedback_almost_label_strict():
    text = _feedback_text("almost", "answer", "answer", "en", "strict")
    assert "counts as wrong" in text
