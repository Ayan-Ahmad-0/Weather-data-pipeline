#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Activate the venv explicitly — a scheduled task runs a bare shell,
# it doesn't inherit whatever venv is active in your interactive terminal.
source .venv/Scripts/activate

echo "[1/4] Extracting + loading current weather..."
python -m src.load.load_to_postgres

echo "[2/4] Running dbt models..."
cd dbt_project
dbt run

echo "[3/4] Running dbt tests..."
dbt test

echo "[4/4] Done."