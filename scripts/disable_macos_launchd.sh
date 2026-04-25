#!/usr/bin/env bash
set -euo pipefail

AGENT_DIR="$HOME/Library/LaunchAgents"
for p in com.qiaolian.pipeline com.qiaolian.publisher com.qiaolian.collector; do
  launchctl bootout "gui/$(id -u)" "$AGENT_DIR/${p}.plist" 2>/dev/null || true
done

echo "已尝试卸载本机 LaunchAgents: pipeline / publisher / collector"
echo "可验证: launchctl list | grep qiaolian"
