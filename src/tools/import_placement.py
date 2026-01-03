import asyncio, json, sys
from sqlalchemy import delete
from src.bot.config import load_settings
from src.bot.db import make_engine, make_sessionmaker
from src.bot.models import PlacementItem, Base

async def main(path: str):
    settings = load_settings()
    engine = make_engine(settings)
    Session = make_sessionmaker(engine)
    data = json.loads(open(path, "r", encoding="utf-8").read())
    items = data["items"] if isinstance(data, dict) else data

    async with Session() as s:
        await s.execute(delete(PlacementItem))
        for i, it in enumerate(items, start=1):
            p = PlacementItem(
                order_index=it.get("order_index", i),
                unit_key=it["unit_key"],
                prompt=it["prompt"],
                item_type=it["item_type"],
                canonical=it["canonical"],
                accepted_variants_json=json.dumps(it.get("accepted_variants", []), ensure_ascii=False),
                options_json=json.dumps(it.get("options")) if it.get("options") is not None else None,
                instruction=it.get("instruction"),
                study_units_json=json.dumps(it.get("meta", {}).get("study_units")) if it.get("meta", {}).get("study_units") is not None else None,
            )
            s.add(p)
        await s.commit()
    await engine.dispose()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.tools.import_placement data/placement.json")
        raise SystemExit(2)
    asyncio.run(main(sys.argv[1]))
