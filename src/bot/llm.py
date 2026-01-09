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

    def explain_and_regrade(
        self,
        *,
        prompt: str,
        canonical: str,
        user_answer: str,
        flow_mode: str,
        difficulty: str,
        ui_lang: str,
    ) -> str:
        # Keep output short (1-4 sentences). Explanation can be uk/en; content stays English.
        logger.info(
            "llm_usage: explain_and_regrade model=%s flow_mode=%s difficulty=%s ui_lang=%s prompt_len=%s canonical_len=%s user_answer_len=%s",
            self.model,
            flow_mode,
            difficulty,
            ui_lang,
            len(prompt),
            len(canonical),
            len(user_answer),
        )
        lang = "Ukrainian" if ui_lang == "uk" else "English"
        contents = f"""You are an English grammar checker.
Task mode: {flow_mode}
Difficulty: {difficulty}
Question: {prompt}
User answer: {user_answer}
Canonical correct answer: {canonical}

Normalization rules (always ignore):
- Letter case differences.
- Non-letter characters (digits, punctuation, symbols, whitespace).
- Curly vs straight quotes.

Difficulty rules:
EASY:
- Accept minor typos and missing apostrophes if obviously intended and meaning/grammar remains correct.
- If the answer is effectively correct, output CORRECT.
NORMAL:
- Accept case/punct/quotes differences.
- Accept only very minor typos (clearly intended) as CORRECT; otherwise WRONG.
STRICT:
- Accept case/punct/quotes differences only.
- Do NOT accept typos as CORRECT unless it is clearly the same correct form (do not forgive spelling errors).

Output format (must follow):
Line 1: CORRECT or WRONG (only).
Line 2+: short explanation in {lang} (1-4 sentences). Do NOT translate rule examples.
If CORRECT but different wording, say why it's acceptable.
"""
        client = self._client()
        resp = client.models.generate_content(model=self.model, contents=contents)
        return (resp.text or "").strip()

    def generate_unit_exercise(
        self,
        *,
        unit_key: str,
        exercise_index: int,
        rule_text: str,
        examples: list[str],
        topic_lock: str,
        unit_topic_hint: str,
        extra_constraints: Optional[str] = None,
    ) -> str:
        example_block = "\n".join(f"- {ex}" for ex in examples) if examples else "(no examples)"
        logger.info(
            "llm_usage: generate_unit_exercise model=%s unit_key=%s exercise_index=%s rule_text_len=%s examples=%s",
            self.model,
            unit_key,
            exercise_index,
            len(rule_text),
            len(examples),
        )
        topic_hint_block = unit_topic_hint or ""
        extra_block = f"\nExtra constraints: {extra_constraints}" if extra_constraints else ""
        contents = f"""You generate English grammar exercises.
Unit: {unit_key}
Exercise index: {exercise_index}
Topic lock: {topic_lock}
{topic_hint_block}
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
{extra_block}
"""
        client = self._client()
        resp = client.models.generate_content(model=self.model, contents=contents)
        return (resp.text or "").strip()
