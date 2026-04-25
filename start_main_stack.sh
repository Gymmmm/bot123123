#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/Users/a1/projects/qiaolian_dual_bots_local"
cd "$BASE_DIR"

if [[ ! -f "run_integrated_stack.py" ]]; then
  echo "[start] run_integrated_stack.py not found in $BASE_DIR"
  exit 1
fi

if pgrep -f "run_integrated_stack.py" >/dev/null 2>&1; then
  echo "[start] run_integrated_stack.py is already running. stop first:"
  echo "        ./stop_all_qiaolian_bots.sh"
  exit 1
fi

echo "[start] launching main stack from $BASE_DIR"
echo "[start] args: $*"

nohup python3 run_integrated_stack.py "$@" > logs/stack.out 2>&1 &
echo "[start] started pid=$!"
echo "[start] tail log: tail -f logs/stack.out"
