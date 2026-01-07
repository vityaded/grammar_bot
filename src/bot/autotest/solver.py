from __future__ import annotations

import asyncio
from dataclasses import dataclass

from google import genai

from ..normalize import norm_answer_text, norm_multiselect_raw
from .types import AnswerAttempt, QuestionContext


@dataclass
class GeminiSolver:
    api_key: str
    model: str
    timeout_sec: float

    def _client(self) -> genai.Client:
        return genai.Client(api_key=self.api_key)

    def _render_prompt(self, ctx: QuestionContext) -> str:
        instruction = ctx.instruction or ""
        prompt = str(ctx.item.get("prompt") or "")
        options = ctx.item.get("options") or []
        option_lines: list[str] = []
        for idx, opt in enumerate(options):
            label = chr(ord("A") + idx)
            option_lines.append(f"{label}) {opt}")
        options_block = "\n".join(option_lines)
        if option_lines:
            option_format = "Reply with the letter(s) only."
            if ctx.exercise_type == "multiselect":
                option_format = "Reply with the letters only, comma-separated if multiple."
        else:
            option_format = "Reply with the answer only."
        return (
            "You are completing an English grammar exercise.\n"
            f"Instruction: {instruction}\n"
            f"Question: {prompt}\n"
            f"Options:\n{options_block}\n"
            f"{option_format}\n"
            "Return only the answer, no explanation."
        )

    def _normalize_answer(self, raw: str, ctx: QuestionContext) -> str:
        raw = raw.strip()
        if ctx.exercise_type == "multiselect":
            return norm_multiselect_raw(raw)
        return norm_answer_text(raw)

    async def solve(self, ctx: QuestionContext) -> AnswerAttempt:
        prompt = self._render_prompt(ctx)
        client = self._client()
        config = None
        if hasattr(genai, "types") and hasattr(genai.types, "GenerateContentConfig"):
            config = genai.types.GenerateContentConfig(temperature=0)

        def _call() -> str:
            if config is None:
                resp = client.models.generate_content(
                    model=self.model,
                    contents=prompt,
                )
            else:
                resp = client.models.generate_content(
                    model=self.model,
                    contents=prompt,
                    config=config,
                )
            return (resp.text or "").strip()

        raw = await asyncio.wait_for(asyncio.to_thread(_call), timeout=self.timeout_sec)
        normalized = self._normalize_answer(raw, ctx)
        return AnswerAttempt(raw=raw, normalized=normalized)
