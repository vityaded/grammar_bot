import pytest
from bot.grader import (
    grade_freetext,
    grade_mcq,
    grade_multiselect,
    grade_option_item,
    resolve_option_item_config,
)
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
    assert grade_freetext(user, canonical, [], "normal").verdict == "correct"
    assert grade_freetext(user, canonical, [], "strict").verdict == "correct"


def test_freetext_missing_letters_is_wrong():
    canonical = "spelling"
    user = "spelin"
    assert grade_freetext(user, canonical, [], "normal").verdict == "wrong"

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


def test_any_of_correctness():
    options = ["you are feeling", "do you feel", "are you feeling"]
    correct = ["do you feel", "are you feeling"]
    res_b = grade_option_item(
        "B",
        "",
        [],
        options,
        selection_policy="any",
        correct_options=correct,
        order_sensitive=False,
        explicit_correct_options=True,
    )
    assert res_b.verdict == "correct"
    res_c = grade_option_item(
        "C",
        "",
        [],
        options,
        selection_policy="any",
        correct_options=correct,
        order_sensitive=False,
        explicit_correct_options=True,
    )
    assert res_c.verdict == "correct"
    res_a = grade_option_item(
        "A",
        "",
        [],
        options,
        selection_policy="any",
        correct_options=correct,
        order_sensitive=False,
        explicit_correct_options=True,
    )
    assert res_a.verdict == "wrong"
    assert res_a.canonical == "do you feel / are you feeling"


def test_any_of_multiple_selection_is_correct():
    options = ["you are feeling", "do you feel", "are you feeling"]
    correct = ["do you feel", "are you feeling"]
    res = grade_option_item(
        "B, C",
        "",
        [],
        options,
        selection_policy="any",
        correct_options=correct,
        order_sensitive=False,
        explicit_correct_options=True,
    )
    assert res.verdict == "correct"
    assert res.note == ""


def test_all_of_correctness():
    options = ["alpha", "bravo", "charlie"]
    correct = ["bravo", "charlie"]
    res_partial = grade_option_item(
        "B",
        "",
        [],
        options,
        selection_policy="all",
        correct_options=correct,
        order_sensitive=False,
        explicit_correct_options=True,
    )
    assert res_partial.verdict == "wrong"
    res_full = grade_option_item(
        "B, C",
        "",
        [],
        options,
        selection_policy="all",
        correct_options=correct,
        order_sensitive=False,
        explicit_correct_options=True,
    )
    assert res_full.verdict == "correct"
    res_extra = grade_option_item(
        "A, B, C",
        "",
        [],
        options,
        selection_policy="all",
        correct_options=correct,
        order_sensitive=False,
        explicit_correct_options=True,
    )
    assert res_extra.verdict == "correct"


def test_legacy_multiselect_needs_review():
    item = {
        "canonical": "alpha, bravo",
        "options": ["alpha", "bravo", "charlie"],
    }
    config = resolve_option_item_config(
        item_type="multiselect",
        item=item,
        instruction="Reply with the letters in order, separated by commas (e.g., A, C).",
    )
    assert config.needs_review is True
