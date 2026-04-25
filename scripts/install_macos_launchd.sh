#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/logs"
PLIST_DIR="$ROOT/scripts/macos"
AGENT_DIR="$HOME/Library/LaunchAgents"
PY_BIN="$ROOT/.venv/bin/python"

if [[ ! -x "$PY_BIN" ]]; then
  PY_BIN="/usr/bin/python3"
fi

mkdir -p "$LOG_DIR" "$PLIST_DIR" "$AGENT_DIR"

create_plist() {
  local label="$1"
  local program="$2"
  local out="$PLIST_DIR/${label}.plist"
  local stdout_file="$LOG_DIR/${label##com.qiaolian.}_launchd.log"
  local stderr_file="$LOG_DIR/${label##com.qiaolian.}_launchd.err"

  cat > "$out" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${label}</string>
  <key>WorkingDirectory</key>
  <string>${ROOT}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PY_BIN}</string>
    <string>${ROOT}/${program}</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${stdout_file}</string>
  <key>StandardErrorPath</key>
  <string>${stderr_file}</string>
</dict>
</plist>
EOF
}

create_pipeline_plist() {
  local label="com.qiaolian.pipeline"
  local out="$PLIST_DIR/${label}.plist"
  cat > "$out" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${label}</string>
  <key>WorkingDirectory</key>
  <string>${ROOT}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PY_BIN}</string>
    <string>${ROOT}/run_pipeline_autopilot.py</string>
  </array>
  <key>StartInterval</key>
  <integer>300</integer>
  <key>RunAtLoad</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/pipeline_launchd.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/pipeline_launchd.err</string>
</dict>
</plist>
EOF
}

create_pipeline_plist
create_plist "com.qiaolian.publisher" "v2/run_publisher_bot_v2.py"
create_plist "com.qiaolian.collector" "collector_bot.py"

cp "$PLIST_DIR"/*.plist "$AGENT_DIR/"

for p in com.qiaolian.pipeline com.qiaolian.publisher com.qiaolian.collector; do
  launchctl bootout "gui/$(id -u)" "$AGENT_DIR/${p}.plist" 2>/dev/null || true
done

launchctl bootstrap "gui/$(id -u)" "$AGENT_DIR/com.qiaolian.pipeline.plist"
launchctl bootstrap "gui/$(id -u)" "$AGENT_DIR/com.qiaolian.publisher.plist"

echo "已启动: pipeline(每5分钟) + publisher。"
echo "collector 需先手动完成 Telethon 登录后再启用："
echo "  cd '$ROOT' && $PY_BIN collector_bot.py"
echo "  launchctl bootstrap gui/$(id -u) '$AGENT_DIR/com.qiaolian.collector.plist'"
