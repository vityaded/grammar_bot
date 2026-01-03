from bot.grader import grade_mcq
from bot.normalize import norm_text

def test_mcq_letter_mapping():
    options = ["I’m starting", "I start"]
    canonical = "I’m starting"
    assert grade_mcq("A", canonical, [], options).verdict == "correct"
    assert grade_mcq("1", canonical, [], options).verdict == "correct"
    assert grade_mcq("I’m starting", canonical, [], options).verdict == "correct"

def test_quote_normalization():
    assert norm_text("I'm") == norm_text("I’m")
