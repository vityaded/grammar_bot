import argparse
import json
import sys

def _load_json(path: str):
    return json.loads(open(path, "r", encoding="utf-8").read())

def _normalize_unit_key(raw) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, int):
        return f"unit_{raw}"
    s = str(raw).strip()
    if not s:
        return None
    if s.isdigit():
        return f"unit_{s}"
    return s

def validate(placement_path: str, unit_exercises_path: str, rules_path: str) -> int:
    errors: list[str] = []
    placement = _load_json(placement_path)
    items = placement.get("items") if isinstance(placement, dict) else placement
    if not isinstance(items, list):
        errors.append("placement: expected list of items")
        items = []

    placement_units: set[str] = set()
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            errors.append(f"placement item {idx}: not an object")
            continue
        for key in ("unit_key", "prompt", "item_type", "canonical"):
            if not item.get(key):
                errors.append(f"placement item {idx}: missing {key}")
        item_type = item.get("item_type")
        if item_type in ("mcq", "multiselect"):
            options = item.get("options")
            if not isinstance(options, list) or not options:
                errors.append(f"placement item {idx}: options required for {item_type}")
        meta = item.get("meta") or {}
        study_units = meta.get("study_units")
        if not isinstance(study_units, list) or not study_units:
            errors.append(f"placement item {idx}: meta.study_units required")
        else:
            for unit in study_units:
                key = _normalize_unit_key(unit)
                if key:
                    placement_units.add(key)

    unit_exercises = _load_json(unit_exercises_path)
    if not isinstance(unit_exercises, list):
        errors.append("unit_exercises: expected list")
        unit_exercises = []
    exercises_index1 = {
        (ex.get("unit_key"), ex.get("exercise_index"))
        for ex in unit_exercises
        if isinstance(ex, dict)
    }
    for unit_key in sorted(placement_units):
        if (unit_key, 1) not in exercises_index1:
            errors.append(f"unit_exercises: missing exercise_index=1 for {unit_key}")

    rules = _load_json(rules_path)
    rules_items = rules.get("items") if isinstance(rules, dict) else rules
    if not isinstance(rules_items, list):
        errors.append("rules_i18n: expected list of items")
        rules_items = []
    rules_units = {
        r.get("unit_key")
        for r in rules_items
        if isinstance(r, dict) and r.get("unit_key")
    }
    for unit_key in sorted(placement_units):
        if unit_key not in rules_units:
            errors.append(f"rules_i18n: missing rule for {unit_key}")

    if errors:
        for err in errors:
            print(f"ERROR: {err}")
        return 1
    print("OK")
    return 0

def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--placement", default="data/placement.json")
    parser.add_argument("--unit-exercises", default="data/unit_exercises.json")
    parser.add_argument("--rules", default="data/rules_i18n.json")
    args = parser.parse_args(argv)
    return validate(args.placement, args.unit_exercises, args.rules)

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
