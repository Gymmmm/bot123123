#!/usr/bin/env bash
set -euo pipefail

HOST=""
USER=""
PORT="22"
TARGET="/opt/qiaolian_dual_bots"

usage() {
  cat <<EOF
Usage:
  $0 --host <server-ip-or-domain> --user <ssh-user> [--port 22] [--target /opt/qiaolian_dual_bots]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2;;
    --user) USER="$2"; shift 2;;
    --port) PORT="$2"; shift 2;;
    --target) TARGET="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

if [[ -z "$HOST" || -z "$USER" ]]; then
  usage
  exit 1
fi

REMOTE="$USER@$HOST"
SSH="ssh -p $PORT"

echo "[1/4] service states"
$SSH "$REMOTE" "systemctl is-active qiaolian-user-bot.service qiaolian-publisher-bot.service qiaolian-collector.service qiaolian-admin-web.service"

echo "[2/4] workflow snapshot"
$SSH "$REMOTE" "cd '$TARGET' && .venv/bin/python scripts/check_workflow.py"

echo "[3/4] recent db errors (last 10 min)"
$SSH "$REMOTE" "echo user_bot; journalctl -u qiaolian-user-bot.service --since '-10 min' --no-pager | grep -Ei 'readonly|OperationalError' || true; echo publisher; journalctl -u qiaolian-publisher-bot.service --since '-10 min' --no-pager | grep -Ei 'readonly|OperationalError' || true; echo collector; journalctl -u qiaolian-collector.service --since '-10 min' --no-pager | grep -Ei 'readonly|OperationalError' || true"

echo "[4/4] token/basic API checks"
$SSH "$REMOTE" "cd '$TARGET' && .venv/bin/python - <<'PY'
import os
import time
import requests
from dotenv import load_dotenv
load_dotenv('.env')
ok_all = True
for name, key in [('user', os.getenv('USER_BOT_TOKEN')), ('publisher', os.getenv('PUBLISHER_BOT_TOKEN'))]:
    if not key:
        print(name, 'token_missing')
        ok_all = False
        continue
    last_err = None
    for i in range(3):
        try:
            r = requests.get(f'https://api.telegram.org/bot{key}/getMe', timeout=15).json()
            print(name, 'getMe_ok=', r.get('ok'), 'username=', (r.get('result') or {}).get('username'))
            if not r.get('ok'):
                ok_all = False
            break
        except Exception as e:
            last_err = e
            if i < 2:
                time.sleep(2)
    else:
        ok_all = False
        print(name, 'getMe_error=', repr(last_err))
if not ok_all:
    raise SystemExit(1)
PY"

echo "server smoke passed"
