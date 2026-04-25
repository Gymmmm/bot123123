#!/usr/bin/env bash
# 生产机上线：代码已在 /opt/qiaolian_dual_bots 后执行。需 root 或 sudo。
set -euo pipefail
TARGET="${TARGET:-/opt/qiaolian_dual_bots}"
cd "$TARGET"

echo "[1/4] 语法检查"
python3 -m py_compile run_user_bot.py meihua_publisher.py discussion_map_store.py collector_bot.py 2>/dev/null || true
PYTHONPATH="$TARGET" python3 -c "import qiaolian_dual.user_bot; import discussion_map_store" 

echo "[2/4] 应用 schema（幂等）"
if [[ -f scripts/bootstrap_db.py ]]; then
  python3 scripts/bootstrap_db.py || true
fi

echo "[3/4] 确保项目内 .py 对运行用户可读（避免 600 导致 collector 无法 import db）"
# 若服务用户为 qiaolianbot，需保证 o+r 或属主正确
find "$TARGET" -maxdepth 1 -name '*.py' ! -perm -004 -exec chmod a+r {} \; 2>/dev/null || true

echo "[4/4] 滚动重启服务"
for u in qiaolian-user-bot qiaolian-publisher-bot qiaolian-collector; do
  systemctl restart "${u}.service" && systemctl is-active "${u}.service" && echo "OK $u" || echo "FAIL $u"
done

echo "版本: $(python3 scripts/print_version.py 2>/dev/null || cat VERSION 2>/dev/null)"
echo "上线命令已执行完毕。请 journalctl -u qiaolian-user-bot -n 30 确认无报错。"
