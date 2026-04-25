#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -x .venv/bin/python ]]; then
  echo "missing .venv/bin/python; run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-autopilot.txt"
  exit 1
fi

echo "[1/4] unit tests"
.venv/bin/python -m unittest discover -p 'test_*.py' -v

echo "[2/4] schema/bootstrap check"
.venv/bin/python scripts/bootstrap_db.py

echo "[3/4] workflow snapshot"
.venv/bin/python scripts/check_workflow.py

echo "[4/4] publish-ready dry-run"
.venv/bin/python scripts/publish_ready_batch.py --dry-run

echo "local smoke passed"
