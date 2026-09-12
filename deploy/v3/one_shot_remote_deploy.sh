#!/usr/bin/env bash
set -Eeuo pipefail

DEPLOY_BRANCH=$1
EXPECTED_SHA=$2
ROOT=/opt/qiaolian_v3
CURRENT="$ROOT/current"
RELEASE="$ROOT/releases/$EXPECTED_SHA"
VENV="$ROOT/.venv"
RUNTIME_ENV="$ROOT/runtime.env"
DB="$ROOT/runtime/data/qiaolian_dual_bot.db"
SERVICES=(qiaolian-v3@collector.service qiaolian-v3@canonical.service qiaolian-v3@publisher.service qiaolian-v3@user.service)
OLD_RELEASE=$(readlink -f "$CURRENT")
OLD_SHA=$(cat "$ROOT/.deployed_sha" 2>/dev/null || basename "$OLD_RELEASE")
BACKUP_DIR="$ROOT/runtime/deploy_backups"
BACKUP_DB="$BACKUP_DIR/pre-${EXPECTED_SHA}.sqlite3"
ACTIVATED=0

rollback() {
  rc=$?
  trap - EXIT
  if [ "$rc" -eq 0 ]; then exit 0; fi
  echo "ROLLBACK_BEGIN rc=$rc old_sha=$OLD_SHA"
  systemctl stop "${SERVICES[@]}" 2>/dev/null || true
  if [ -n "$OLD_RELEASE" ] && [ -d "$OLD_RELEASE" ]; then
    ln -sfn "$OLD_RELEASE" "$ROOT/current.rollback"
    mv -Tf "$ROOT/current.rollback" "$CURRENT"
  fi
  if [ "$ACTIVATED" -eq 1 ] && [ -f "$BACKUP_DB" ]; then
    "$VENV/bin/python" - "$BACKUP_DB" "$DB" <<'PY'
import sqlite3, sys
src, dst = sys.argv[1:3]
with sqlite3.connect(src) as source, sqlite3.connect(dst) as target:
    source.backup(target)
PY
  fi
  systemctl restart "${SERVICES[@]}" 2>/dev/null || true
  sleep 5
  echo "ROLLBACK_DONE old_sha=$OLD_SHA"
  exit "$rc"
}
trap rollback EXIT

test -x "$VENV/bin/python"
test -f "$RUNTIME_ENV"
test -f "$DB"
test -d "$OLD_RELEASE"
install -d -m 0750 -o qiaolianbot -g qiaolianbot "$BACKUP_DIR"

"$VENV/bin/python" - "$DB" "$BACKUP_DB" <<'PY'
import sqlite3, sys
src, dst = sys.argv[1:3]
with sqlite3.connect(src) as source, sqlite3.connect(dst) as target:
    source.backup(target)
PY
chmod 0640 "$BACKUP_DB"
chown qiaolianbot:qiaolianbot "$BACKUP_DB"
echo "DB_BACKUP_OK old_sha=$OLD_SHA"

if [ ! -d "$RELEASE/.git" ]; then
  rm -rf -- "$RELEASE"
  git clone -q --single-branch --branch "$DEPLOY_BRANCH" https://github.com/Gymmmm/bot123123.git "$RELEASE"
fi
test "$(git -C "$RELEASE" rev-parse HEAD)" = "$EXPECTED_SHA"
"$VENV/bin/python" -m pip install --disable-pip-version-check -q -r "$RELEASE/requirements.txt"
chown -R qiaolianbot:qiaolianbot "$RELEASE"

ln -sfn "$RELEASE" "$ROOT/current.next"
mv -Tf "$ROOT/current.next" "$CURRENT"
install -m 0644 "$CURRENT/deploy/v3/qiaolian-v3@.service" /etc/systemd/system/qiaolian-v3@.service
systemctl daemon-reload

runuser -u qiaolianbot -- bash -lc "set -a; source '$RUNTIME_ENV'; set +a; exec '$VENV/bin/python' '$CURRENT/run_v3_preflight.py' --component all --db '$DB'"

ACTIVATED=1
ACTIVATED_AT=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
systemctl restart "${SERVICES[@]}"
sleep 10

HEALTH_FAILED=0
for service in "${SERVICES[@]}"; do
  ACTIVE=$(systemctl is-active "$service" 2>/dev/null || true)
  PID=$(systemctl show "$service" -p MainPID --value 2>/dev/null || echo 0)
  RESTARTS=$(systemctl show "$service" -p NRestarts --value 2>/dev/null || echo unknown)
  echo "SERVICE_HEALTH unit=$service active=$ACTIVE pid=$PID restarts=$RESTARTS"
  [ "$ACTIVE" = active ] || HEALTH_FAILED=1
  [ "${PID:-0}" -gt 0 ] 2>/dev/null || HEALTH_FAILED=1
  [ "$RESTARTS" = 0 ] || HEALTH_FAILED=1
done
[ "$HEALTH_FAILED" -eq 0 ]
! journalctl -u qiaolian-v3@collector.service -u qiaolian-v3@canonical.service -u qiaolian-v3@publisher.service -u qiaolian-v3@user.service --since "$ACTIVATED_AT" --no-pager | grep -Eq 'Traceback|ModuleNotFoundError|ImportError|database is locked|no such table|Conflict: terminated by other getUpdates request|Main process exited|Failed with result'

# One-time scoped rebuild hook. It is guarded by both the deployed SHA and a durable marker.
REBUILD_MARKER="$ROOT/.rebuild_zufang555_20260912_done"
if [ "$EXPECTED_SHA" = "523577c39824de4c39024e1b592bbbdbe2f2c45e" ] && [ ! -f "$REBUILD_MARKER" ]; then
  echo "REBUILD_HOOK_BEGIN source=zufang555 target=50"
  rebuild_backup="$BACKUP_DIR/pre-rebuild-zufang555-20260912.sqlite3"
  "$VENV/bin/python" - "$DB" "$rebuild_backup" <<'PY'
import sqlite3, sys
src, dst = sys.argv[1:3]
with sqlite3.connect(src) as source, sqlite3.connect(dst) as target:
    source.backup(target)
PY
  chmod 0640 "$rebuild_backup"
  chown qiaolianbot:qiaolianbot "$rebuild_backup"
  curl -fsSL https://raw.githubusercontent.com/Gymmmm/bot123123/master/deploy/v3/open_rebuild_batch_zufang555.py -o /tmp/open_rebuild_batch_zufang555.py
  set -a
  source "$RUNTIME_ENV"
  set +a
  REBUILD_SOURCE_NAME=zufang555 REBUILD_TARGET_GROUPS=50 REBUILD_BATCH_ID=zufang555_20260912 \
    "$VENV/bin/python" /tmp/open_rebuild_batch_zufang555.py
  rm -f /tmp/open_rebuild_batch_zufang555.py
  touch "$REBUILD_MARKER"
  systemctl restart qiaolian-v3@publisher.service
  sleep 8
  systemctl is-active --quiet qiaolian-v3@publisher.service
  echo "REBUILD_HOOK_DONE marker=$REBUILD_MARKER"
fi

printf '%s\n' "$EXPECTED_SHA" > "$ROOT/.deployed_sha"
chmod 0644 "$ROOT/.deployed_sha"
echo "V3_DEPLOY_OK sha=$EXPECTED_SHA previous=$OLD_SHA"
trap - EXIT
