from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import Optional
from google import genai

logger = logging.getLogger(__name__)

@dataclass
class LLMClient:
    api_key: str
    model: str = "gemini-3-flash-preview"

    def _client(self):
        return genai.Client(api_key=self.api_key)

    def explain_and_regrade(self, *, prompt: str, canonical: str, user_answer: str, mode: str, ui_lang: str) -> str:
        # Keep output short (1-4 sentences). Explanation can be uk/en; content stays English.
        logger.info(
            "llm_usage: explain_and_regrade model=%s mode=%s ui_lang=%s prompt_len=%s canonical_len=%s user_answer_len=%s",
            self.model,
            mode,
            ui_lang,
            len(prompt),
            len(canonical),
            len(user_answer),
        )
        lang = "Ukrainian" if ui_lang == "uk" else "English"
        contents = f"""You are a strict English grammar checker.
Task mode: {mode}
Question: {prompt}
User answer: {user_answer}
Canonical correct answer: {canonical}

1) Decide if user's answer should be accepted as correct. Reply with ONLY one of: CORRECT or WRONG.
2) Then on a new line, provide a short explanation in {lang} (1-4 sentences), do NOT translate examples.
3) If CORRECT but different wording, say why it's acceptable.
"""
        client = self._client()
        resp = client.models.generate_content(model=self.model, contents=contents)
        return (resp.text or "").strip()

    def generate_unit_exercise(self, *, unit_key: str, exercise_index: int, rule_text: str, examples: list[str]) -> str:
        example_block = "\n".join(f"- {ex}" for ex in examples) if examples else "(no examples)"
        logger.info(
            "llm_usage: generate_unit_exercise model=%s unit_key=%s exercise_index=%s rule_text_len=%s examples=%s",
            self.model,
            unit_key,
            exercise_index,
            len(rule_text),
            len(examples),
        )
        contents = f"""You generate English grammar exercises.
Unit: {unit_key}
Exercise index: {exercise_index}
Rule text: {rule_text}
Examples:
{example_block}

Return ONLY valid JSON with this schema:
{{
  "exercise_type": "freetext",
  "instruction": "English instruction",
  "items": [
    {{
      "prompt": "Question text",
      "canonical": "Correct answer",
      "accepted_variants": ["variant 1", "variant 2"]
    }}
  ]
}}
Constraints:
- exercise_type should be "freetext" unless you must use mcq/multiselect; then include options.
- Provide at least 2 items.
- Keep everything in English.
"""
        client = self._client()
        resp = client.models.generate_content(model=self.model, contents=contents)
        return (resp.text or "").strip()
