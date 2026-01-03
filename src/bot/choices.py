from __future__ import annotations

from .normalize import norm_text

def resolve_choice(user_input: str, options: list[str]) -> str | None:
    if not options:
        return None
    raw = norm_text(user_input or "")
    if not raw:
        return None
    cleaned = raw.strip()

    if len(cleaned) == 1 and cleaned.isalpha():
        idx = ord(cleaned.upper()) - ord("A")
        if 0 <= idx < len(options):
            return str(options[idx])

    if cleaned.isdigit():
        idx = int(cleaned)
        if 1 <= idx <= len(options):
            return str(options[idx - 1])

    normalized = {norm_text(opt).casefold(): str(opt) for opt in options}
    key = norm_text(cleaned).casefold()
    if key in normalized:
        return normalized[key]
    return None
