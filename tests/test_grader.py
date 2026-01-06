import pytest
from bot.grader import grade_freetext, grade_mcq, grade_multiselect
from bot.normalize import norm_text, norm_answer_text

@pytest.mark.parametrize("mode", ["easy", "normal", "strict"])
def test_mcq_letter_mapping(mode):
    options = ["I’m starting", "I start"]
    canonical = "I’m starting"
    assert grade_mcq("A", canonical, [], options, mode).verdict == "correct"
    assert grade_mcq("1", canonical, [], options, mode).verdict == "correct"
    assert grade_mcq("I’m starting", canonical, [], options, mode).verdict == "correct"

def test_quote_normalization():
    assert norm_text("I'm") == norm_text("I’m")

@pytest.mark.parametrize("mode", ["easy", "normal", "strict"])
def test_freetext_punctuation_and_case(mode):
    canonical = "I'm here"
    user = "I’m here."
    assert grade_freetext(user, canonical, [], mode).verdict == "correct"

def test_freetext_close_typos():
    canonical = "doesn't"
    user = "doesnt"
    assert grade_freetext(user, canonical, [], "easy").verdict == "correct"
    assert grade_freetext(user, canonical, [], "normal").verdict == "almost"
    assert grade_freetext(user, canonical, [], "strict").verdict == "almost"

def test_multiselect_ordering_modes():
    options = ["were", "was"]
    canonical = "were, was"
    user = "B, A"
    assert grade_multiselect(user, canonical, options, [], order_sensitive=False).verdict == "correct"
    assert grade_multiselect(user, canonical, options, [], order_sensitive=True).verdict == "wrong"

def test_multiselect_ordering_override():
    options = ["were", "was"]
    canonical = "were, was"
    user = "B, A"
    assert grade_multiselect(user, canonical, options, [], order_sensitive=True).verdict == "wrong"

def test_norm_answer_text_trailing_punct():
    assert norm_answer_text("here .") == "here"
    assert norm_answer_text("here,  ") == "here"
