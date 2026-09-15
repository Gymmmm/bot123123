#!/usr/bin/env bash
set -euo pipefail

COMPONENT="${1:-}"
CODE_ROOT="${QIAOLIAN_CODE_ROOT:-/opt/qiaolian_v3/current}"
VENV="${QIAOLIAN_VENV:-/opt/qiaolian_v3/.venv}"
PYTHON="${VENV}/bin/python"

cd "$CODE_ROOT"

case "$COMPONENT" in
  collector)
    exec "$PYTHON" "$CODE_ROOT/run_v3_collector.py"
    ;;
  canonical)
    exec "$PYTHON" "$CODE_ROOT/run_v3_canonical_worker.py" \
      --loop \
      --interval "${V3_CANONICAL_WORKER_INTERVAL_SECONDS:-5}"
    ;;
  publisher)
    exec "$PYTHON" "$CODE_ROOT/run_v3_publisher_bot.py"
    ;;
  user)
    exec "$PYTHON" "$CODE_ROOT/run_v3_user_bot.py"
    ;;
  *)
    echo "unknown V3 component: $COMPONENT" >&2
    exit 64
    ;;
esac
