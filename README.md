# EGIU Placement + Detour Telegram Bot (aiogram v3 + SQLAlchemy + SQLite)

This repo is a working starter that implements:
- Access by invite-link + admin approval
- Language setting (UI language: uk/en). Content questions/answers are **English**; explanations + rules can be localized; examples stay **English**
- Placement test (MCQ + multiselect + free text)
- Feedback with **❓ Why** (LLM explanation + possible verdict flip) + **▶️ Next**
- Detour flow on mistakes (rule + examples in one message, then exercises), with regeneration + batch limits + revisit scheduling
- Revisit (2 days later) and 7-day check

## 1) Requirements
- Python 3.11+
- Telegram bot token (BotFather)
- (Optional) Gemini key for LLM fallback: `GOOGLE_API_KEY` (or legacy `GEMINI_API_KEY`)

## 2) Quick start (local)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m src.bot.init_db
python -m src.bot.run
```

Create a `.env` file (plain `KEY=VALUE` lines, no `export`):
```
BOT_TOKEN=...
ADMIN_IDS=123456789,987654321
DATABASE_URL=sqlite+aiosqlite:///./data/app.db
GOOGLE_API_KEY=...
```

## 3) Load content
This starter expects 2 JSON files:
- placement dataset: `data/placement.json`
- unit exercises dataset: `data/unit_exercises_v2.json`
- rules i18n (v2 subpoints): `data/rules_i18n_v2.json` (optional; you can fill later)

Import commands:
```bash
python -m src.tools.migrate_v2
python -m src.tools.import_placement data/placement.json
python -m src.tools.import_unit_exercises data/unit_exercises_v2.json
python -m src.tools.import_rules_i18n_v2 data/rules_i18n_v2.json
```

Dev-only reset option (simplest):
- delete `./data/app.db`
- `python -m src.bot.init_db`
- re-import datasets with the commands above

Sample datasets are included in `data/samples/`.

## 4) Deployment notes
- Run as a systemd service (long polling). See `deploy/systemd/egiu-bot.service`.
- Use a reverse-proxy / webhook only if you want; this repo uses polling by default.

## 5) What you still need to do
1) Replace sample datasets with your real extracted datasets.
2) Ensure every exercise item has:
   - `canonical` (single string shown as correct)
   - `accepted_variants` list (normalized variants allowed)
   - if MCQ: `options` list (4) for each item
   - multiselect: `options` list + `canonical` is comma-separated in option order
3) Fill rules:
   - `rule_text_uk` / `rule_short_uk` etc.
   - examples are stored in `examples_json` and must remain English.
4) Configure admins and invite link flow:
   - Share your invite link that contains a token, e.g. `https://t.me/<bot>?start=INV_<token>`
   - Admin approves requests inside the bot.
5) Add Alembic migrations (recommended) once schema stabilizes.
