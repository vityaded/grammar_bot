from __future__ import annotations

STRINGS: dict[str, dict[str, str]] = {
    "choose_lang": {"en": "Choose UI language:", "uk": "Оберіть мову інтерфейсу:"},
    "start_placement": {"en": "Start placement test", "uk": "Почати вступний тест"},
    "access_required": {"en": "Access required. Choose UI language:", "uk": "Потрібен доступ. Оберіть мову інтерфейсу:"},
    "approved_choose_lang": {"en": "Approved. Choose UI language:", "uk": "Доступ надано. Оберіть мову інтерфейсу:"},
    "rule_header": {"en": "Rule:", "uk": "Правило:"},
    "your_answer": {"en": "Your answer:", "uk": "Ваша відповідь:"},
    "correct_answer": {"en": "Correct:", "uk": "Правильна відповідь:"},
    "press_next": {"en": "press ▶️ Next or ❓ Why", "uk": "натисніть ▶️ Далі або ❓ Чому"},
    "press_next_only": {"en": "press ▶️ Next", "uk": "натисніть ▶️ Далі"},
    "use_buttons": {"en": "Use Next / Why buttons.", "uk": "Використовуйте кнопки Далі / Чому."},
    "progress_reset": {
        "en": "Progress reset. Use Start placement to begin again.",
        "uk": "Прогрес скинуто. Почніть знову через Почати вступний тест.",
    },
}

def t(key: str, lang: str) -> str:
    return STRINGS.get(key, {}).get(lang, STRINGS.get(key, {}).get("en", key))
