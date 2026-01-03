from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from google import genai

@dataclass
class LLMClient:
    api_key: str
    model: str = "gemini-3-flash-preview"

    def _client(self):
        return genai.Client(api_key=self.api_key)

    def explain_and_regrade(self, *, prompt: str, canonical: str, user_answer: str, mode: str, ui_lang: str) -> str:
        # Keep output short (1-4 sentences). Explanation can be uk/en; content stays English.
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
