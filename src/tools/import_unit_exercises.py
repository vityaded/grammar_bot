import asyncio, json, sys
from sqlalchemy import delete
from src.bot.config import load_settings
from src.bot.db import make_engine, make_sessionmaker
from src.bot.models import UnitExercise

async def main(path: str):
    settings = load_settings()
    engine = make_engine(settings)
    Session = make_sessionmaker(engine)
    data = json.loads(open(path, "r", encoding="utf-8").read())
    rows = data["exercises"] if isinstance(data, dict) else data

    async with Session() as s:
        await s.execute(delete(UnitExercise))
        for ex in rows:
            row = UnitExercise(
                unit_key=ex["unit_key"],
                exercise_index=int(ex["exercise_index"]),
                exercise_type=ex["exercise_type"],
                instruction=ex["instruction"],
                items_json=json.dumps(ex["items"], ensure_ascii=False),
            )
            s.add(row)
        await s.commit()
    await engine.dispose()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.tools.import_unit_exercises data/unit_exercises.json")
        raise SystemExit(2)
    asyncio.run(main(sys.argv[1]))
