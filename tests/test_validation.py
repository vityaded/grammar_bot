from bot.validation import validate_unit_exercises


def test_validator_flags_ambiguous_multiselect():
    payload = {
        "exercises": [
            {
                "unit_key": "unit_1",
                "exercise_index": 1,
                "exercise_type": "multiselect",
                "instruction": "Reply with the letters in order, separated by commas (e.g., A, C).",
                "items": [
                    {
                        "prompt": "Pick",
                        "options": ["alpha", "bravo", "charlie"],
                        "canonical": "alpha, bravo",
                        "accepted_variants": [],
                    }
                ],
            }
        ]
    }
    issues = validate_unit_exercises(payload)
    assert any(issue.severity == "warning" for issue in issues)
