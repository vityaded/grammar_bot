from __future__ import annotations
import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

load_dotenv()

def _split_csv_ints(s: str) -> List[int]:
    out = []
    for part in (s or "").split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return out

@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_ids: List[int]
    database_url: str
    gemini_api_key: str | None
    ui_default_lang: str = "uk"  # uk/en

def load_settings() -> Settings:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required")

    admin_ids = _split_csv_ints(os.getenv("ADMIN_IDS", ""))
    if not admin_ids:
        raise RuntimeError("ADMIN_IDS is required (comma-separated Telegram user ids)")

    database_url = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./data/app.db")
    gemini_api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY") or None

    return Settings(
        bot_token=bot_token,
        admin_ids=admin_ids,
        database_url=database_url,
        gemini_api_key=gemini_api_key,
    )
