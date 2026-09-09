#!/usr/bin/env bash
set -Eeuo pipefail

APP=/opt/qiaolian_v3_smoke
OLD=/opt/qiaolian_dual_bots
PY="$OLD/.venv/bin/python"
DB="$APP/data/v3_smoke.db"
EXPECTED_SHA=c5a9a462cd129f7cb3563b9324280414f4d42fc9

[[ "$(git -C "$APP" rev-parse HEAD)" == "$EXPECTED_SHA" ]]
[[ -x "$PY" ]]
[[ -f "$APP/.env" ]]
[[ -f "$APP/sources.json" ]]

systemctl stop qiaolian-user-bot.service qiaolian-publisher-bot.service qiaolian-collector.service 2>/dev/null || true
systemctl stop qiaolian-v3-smoke-user.service qiaolian-v3-smoke-publisher.service 2>/dev/null || true

rm -f "$DB" "$DB-wal" "$DB-shm"
install -d "$APP/data" "$APP/media/collector_downloads"
cd "$APP"
DB_PATH="$DB" PYTHONPATH="$APP" "$PY" "$APP/bootstrap_db.py"

set -a
source "$APP/.env"
set +a
export DB_PATH="$DB"
export PYTHONPATH="$APP"
export TELETHON_SESSION_PATH="$APP/telethon_sessions/qiaolian_collector"

"$PY" /tmp/v3_smoke_collect10.py | tee /tmp/v3_collect10.log

cat > /etc/systemd/system/qiaolian-v3-smoke-user.service <<EOF
[Unit]
Description=Qiaolian V3 smoke user bot
[Service]
Type=simple
WorkingDirectory=$APP
Environment=DB_PATH=$DB
Environment=PYTHONPATH=$APP
ExecStart=$PY $APP/run_user_bot.py
Restart=on-failure
EOF

cat > /etc/systemd/system/qiaolian-v3-smoke-publisher.service <<EOF
[Unit]
Description=Qiaolian V3 smoke publisher bot
[Service]
Type=simple
WorkingDirectory=$APP
Environment=DB_PATH=$DB
Environment=PYTHONPATH=$APP
ExecStart=$PY $APP/v2/run_publisher_bot_v2.py
Restart=on-failure
EOF

systemctl daemon-reload
systemctl start qiaolian-v3-smoke-user.service qiaolian-v3-smoke-publisher.service
sleep 5
systemctl is-active --quiet qiaolian-v3-smoke-user.service
systemctl is-active --quiet qiaolian-v3-smoke-publisher.service

AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes "$PY" /tmp/v3_smoke_publish10.py | tee /tmp/v3_publish10.log
sleep 3
systemctl is-active --quiet qiaolian-v3-smoke-user.service
systemctl is-active --quiet qiaolian-v3-smoke-publisher.service

"$PY" - <<'PY'
import os, sqlite3
conn = sqlite3.connect(os.environ['DB_PATH'])
for table in ['source_posts','drafts','media_assets','publication_packages','posts','v3_source_posts','canonical_records','v3_publication_packages','v3_channel_posts']:
    try:
        value = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
    except Exception:
        value = -1
    print('COUNT', table, value)
print('POSTS', conn.execute("SELECT draft_id,channel_chat_id,channel_message_id,publish_status FROM posts ORDER BY id DESC LIMIT 10").fetchall())
PY

systemctl --no-pager --full status qiaolian-v3-smoke-user.service qiaolian-v3-smoke-publisher.service || true
journalctl -u qiaolian-v3-smoke-user.service -u qiaolian-v3-smoke-publisher.service --since '-15 minutes' --no-pager -n 120 || true
echo "SMOKE_COMPLETE sha=$EXPECTED_SHA db=$DB"
