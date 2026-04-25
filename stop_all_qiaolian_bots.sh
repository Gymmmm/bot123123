#!/usr/bin/env bash
set -euo pipefail

echo "[stop] stopping qiaolian-related local processes..."

patterns=(
  "run_integrated_stack.py"
  "run_user_bot.py"
  "qiaolian_dual_user_bot.py"
  "autopilot_publish_bot.py"
  "v2/run_publisher_bot_v2.py"
  "collector_bot.py"
  "run_pipeline_autopilot.py"
  "run_pipeline.py"
)

for p in "${patterns[@]}"; do
  pids="$(pgrep -f "$p" || true)"
  if [[ -n "${pids}" ]]; then
    echo "[stop] kill pattern: $p"
    echo "$pids" | xargs kill -TERM || true
  fi
done

sleep 1

for p in "${patterns[@]}"; do
  pids="$(pgrep -f "$p" || true)"
  if [[ -n "${pids}" ]]; then
    echo "[stop] force kill pattern: $p"
    echo "$pids" | xargs kill -KILL || true
  fi
done

echo "[stop] done."
