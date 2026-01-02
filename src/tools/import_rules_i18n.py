import asyncio, json, sys
from sqlalchemy import delete
from src.bot.config import load_settings
from src.bot.db import make_engine, make_sessionmaker
from src.bot.models import RuleI18n

async def main(path: str):
    settings = load_settings()
    engine = make_engine(settings)
    Session = make_sessionmaker(engine)
    data = json.loads(open(path, "r", encoding="utf-8").read())
    rows = data["rules"] if isinstance(data, dict) else data

    async with Session() as s:
        await s.execute(delete(RuleI18n))
        for r in rows:
            row = RuleI18n(
                unit_key=r["unit_key"],
                rule_text_en=r.get("rule_text_en"),
                rule_text_uk=r.get("rule_text_uk"),
                rule_short_en=r.get("rule_short_en"),
                rule_short_uk=r.get("rule_short_uk"),
                examples_json=json.dumps(r.get("examples", []), ensure_ascii=False) if r.get("examples") is not None else None,
            )
            s.add(row)
        await s.commit()
    await engine.dispose()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.tools.import_rules_i18n data/rules_i18n.json")
        raise SystemExit(2)
    asyncio.run(main(sys.argv[1]))
