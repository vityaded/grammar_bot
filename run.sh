#!/usr/bin/env bash
set -euo pipefail
python -m src.bot.init_db
python -m src.bot.run
