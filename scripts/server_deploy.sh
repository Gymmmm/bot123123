#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST=""
USER=""
PORT="22"
TARGET="/opt/qiaolian_dual_bots"
SERVICE_USER="qiaolianbot"
SKIP_INSTALL="0"
SKIP_RESTART="0"

usage() {
  cat <<EOF
Usage:
  $0 --host <server-ip-or-domain> --user <ssh-user> [--port 22] [--target /opt/qiaolian_dual_bots] [--service-user qiaolianbot] [--skip-install] [--skip-restart]

Notes:
  - This script syncs code only (does NOT overwrite remote .env / data / media).
  - Remote service names expected:
      qiaolian-user-bot.service
      qiaolian-publisher-bot.service
      qiaolian-collector.service
      qiaolian-admin-web.service
  - Optional (recommended for full automation):
      qiaolian-pipeline.timer (run_pipeline_autopilot periodic runner)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2;;
    --user) USER="$2"; shift 2;;
    --port) PORT="$2"; shift 2;;
    --target) TARGET="$2"; shift 2;;
    --service-user) SERVICE_USER="$2"; shift 2;;
    --skip-install) SKIP_INSTALL="1"; shift;;
    --skip-restart) SKIP_RESTART="1"; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

if [[ -z "$HOST" || -z "$USER" ]]; then
  usage
  exit 1
fi

SSH="ssh -p $PORT"
RSYNC_SSH="ssh -p $PORT"
REMOTE="$USER@$HOST"

echo "[1/5] Ensure remote directories"
$SSH "$REMOTE" "mkdir -p '$TARGET' '$TARGET/logs' '$TARGET/data' '$TARGET/media'"

echo "[2/5] Sync code"
rsync -az --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '.venv_broken*/' \
  --exclude '.env' \
  --exclude '.pytest_cache/' \
  --exclude 'data/*' \
  --exclude 'data/discussion_map.json' \
  --exclude 'data/discussion_bridge.json' \
  --exclude 'data/discussion_pending.json' \
  --exclude 'logs/*' \
  --exclude 'media/*' \
  --exclude 'reports/' \
  --exclude 'tmp_preview/' \
  --exclude '*.bak_*' \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  -e "$RSYNC_SSH" \
  "$ROOT/" "$REMOTE:$TARGET/"

if [[ "$SKIP_INSTALL" != "1" ]]; then
  echo "[3/6] Fix runtime ownership"
  $SSH "$REMOTE" "if id -u '$SERVICE_USER' >/dev/null 2>&1; then chown -R '$SERVICE_USER':'$SERVICE_USER' '$TARGET/data' '$TARGET/logs' '$TARGET/media' '$TARGET/v2'; fi"

  echo "[4/6] Install/update venv + deps"
  $SSH "$REMOTE" "cd '$TARGET' && \
    (test -d .venv || python3 -m venv .venv) && \
    .venv/bin/python -m pip install --upgrade pip && \
    .venv/bin/pip install -r requirements.txt -r requirements-autopilot.txt"
else
  echo "[3/6] Fix runtime ownership"
  $SSH "$REMOTE" "if id -u '$SERVICE_USER' >/dev/null 2>&1; then chown -R '$SERVICE_USER':'$SERVICE_USER' '$TARGET/data' '$TARGET/logs' '$TARGET/media' '$TARGET/v2'; fi"
fi

echo "[5/6] Apply DB schema safely"
$SSH "$REMOTE" "cd '$TARGET' && if id -u '$SERVICE_USER' >/dev/null 2>&1; then sudo -u '$SERVICE_USER' .venv/bin/python scripts/bootstrap_db.py; else .venv/bin/python scripts/bootstrap_db.py; fi"

if [[ "$SKIP_RESTART" != "1" ]]; then
  echo "[6/6] Restart services (requires sudo permission on server)"
  $SSH "$REMOTE" "sudo systemctl restart qiaolian-user-bot.service qiaolian-publisher-bot.service qiaolian-collector.service qiaolian-admin-web.service && \
    if systemctl list-unit-files | grep -q '^qiaolian-pipeline.timer'; then sudo systemctl restart qiaolian-pipeline.timer && sudo systemctl start qiaolian-pipeline.service; fi && \
    sudo systemctl --no-pager --full status qiaolian-user-bot.service qiaolian-publisher-bot.service qiaolian-collector.service qiaolian-admin-web.service | sed -n '1,120p'"
fi

echo "Deploy done: $REMOTE:$TARGET"
