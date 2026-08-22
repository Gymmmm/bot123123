# 生产部署命令

> r3 强制门禁：部署前必须在候选目录运行全量 `python3 -m pytest -q` 并得到 0 failed；不得使用 CSV、`send-next` 或 zufang555 旧脚本直发。批量发布仅允许 `scripts/publish_ready_batch.py`，且 draft 与 publication package 必须同时为 `approved`。

以下命令必须在生产主机由有 sudo 权限的运维人员人工执行。先在维护窗口和测试频道演练。示例路径与现有 systemd 单元一致。

## 1. 定义本次部署路径

```bash
export QL_APP=/opt/qiaolian_dual_bots
export QL_RELEASE=/opt/releases/qiaolian-final-verified-20260822-r2
export QL_BACKUP=/opt/backups/qiaolian-$(date +%Y%m%d-%H%M%S)
```

不要把这些变量改成 `/`、`$HOME` 或其他宽泛目录。

## 2. 解压到独立 release 目录

```bash
sudo install -d -m 0755 /opt/releases /opt/backups
sudo unzip -q qiaolian_final_verified_release.zip -d /opt/releases
sudo mv /opt/releases/qiaolian_final_verified_release "$QL_RELEASE"
sudo chown -R root:root "$QL_RELEASE"
```

确认 ZIP 内没有 `.env`、DB、session 和密钥：

```bash
find "$QL_RELEASE" -type f \( -name '.env*' -o -name '*.db' -o -name '*.sqlite*' -o -name '*.session*' -o -name '*ed25519*' \) -print
```

预期无输出。

## 3. 备份现有代码、配置和数据库

```bash
sudo install -d -m 0700 "$QL_BACKUP"
sudo rsync -a "$QL_APP/" "$QL_BACKUP/app/"
sudo cp -a "$QL_APP/.env" "$QL_BACKUP/.env"
sudo -u qiaolianbot sqlite3 "$QL_APP/data/qiaolian_dual_bot.db" ".backup '$QL_BACKUP/qiaolian_dual_bot.db'"
sudo sha256sum "$QL_BACKUP/qiaolian_dual_bot.db" | sudo tee "$QL_BACKUP/qiaolian_dual_bot.db.sha256"
```

## 4. 在数据库副本执行迁移和预检

```bash
sudo cp -a "$QL_BACKUP/qiaolian_dual_bot.db" "$QL_BACKUP/migration-test.db"
sudo -u qiaolianbot env DB_PATH="$QL_BACKUP/migration-test.db" "$QL_APP/.venv/bin/python" "$QL_RELEASE/scripts/bootstrap_db.py"
sudo -u qiaolianbot sqlite3 "$QL_BACKUP/migration-test.db" "PRAGMA integrity_check; PRAGMA foreign_key_check;"
```

预期第一行 `ok`，foreign key 检查无记录。不要使用 `QL_REBUILD_DB=1` 处理真实数据库。

在这个数据库副本上审计旧 Canonical location；命令只读，不修改副本：

```bash
sudo -u qiaolianbot env \
  PYTHONPATH="$QL_RELEASE" \
  QL_AUDIT_DB="$QL_BACKUP/migration-test.db" \
  "$QL_APP/.venv/bin/python" - <<'PY'
import json
import os
import sqlite3
from qiaolian_dual.canonical_fact_projection import validate_facts

prefixes = (
    "canonical_area", "city_used", "public_location", "market_location",
    "publication_location", "level_1", "level_2", "unknown_level",
)
conn = sqlite3.connect(f"file:{os.environ['QL_AUDIT_DB']}?mode=ro", uri=True)
rows = conn.execute("SELECT draft_id, normalized_data FROM drafts").fetchall()
bad = []
for draft_id, raw in rows:
    try:
        facts = json.loads(raw or "{}")
    except json.JSONDecodeError:
        bad.append((draft_id, ["normalized_data_invalid_json"]))
        continue
    errors = [item for item in validate_facts(facts) if item.startswith(prefixes)]
    if errors:
        bad.append((draft_id, errors))
print(json.dumps({"checked": len(rows), "invalid_location_drafts": bad}, ensure_ascii=False, indent=2))
PY
```

任何 `invalid_location_drafts` 都要先人工复核。不要修改 approved/published package；对未冻结 draft 使用管理工具重新选择 area，或重新解析来源。

## 5. 停止服务并部署代码

```bash
sudo systemctl stop qiaolian-collector.service qiaolian-publisher-bot.service qiaolian-user-bot.service
sudo rsync -a --delete \
  --exclude '.env' --exclude 'data/' --exclude 'media/' --exclude 'logs/' \
  --exclude 'telethon_sessions/' --exclude '.venv/' \
  "$QL_RELEASE/" "$QL_APP/"
sudo chown -R root:qiaolianbot "$QL_APP"
sudo chown -R qiaolianbot:qiaolianbot "$QL_APP/data" "$QL_APP/media" "$QL_APP/logs" "$QL_APP/telethon_sessions"
sudo chmod 0640 "$QL_APP/.env"
```

## 6. 安装依赖、字体与 Chromium

```bash
sudo -u qiaolianbot "$QL_APP/.venv/bin/pip" install -r "$QL_APP/requirements.txt"
sudo -u qiaolianbot "$QL_APP/.venv/bin/python" -m playwright install chromium
fc-match -f '%{family}\n' 'Noto Sans CJK SC'
```

最后一行应显示 Noto CJK 家族。Debian/Ubuntu 若缺失，由运维审查后安装：

```bash
sudo apt-get update
sudo apt-get install -y fonts-noto-cjk fontconfig
fc-cache -f
fc-match -f '%{family}\n' 'Noto Sans CJK SC'
```

如主机缺 Chromium 系统库，按 Playwright 输出由运维安装，不要在未审查情况下运行来源不明脚本。

## 7. 对真实 DB 执行 additive bootstrap

```bash
sudo -u qiaolianbot env DB_PATH="$QL_APP/data/qiaolian_dual_bot.db" "$QL_APP/.venv/bin/python" "$QL_APP/scripts/bootstrap_db.py"
sudo -u qiaolianbot sqlite3 "$QL_APP/data/qiaolian_dual_bot.db" "PRAGMA integrity_check; PRAGMA foreign_key_check;"
```

## 8. 代码与配置预检

```bash
sudo -u qiaolianbot "$QL_APP/.venv/bin/python" -m py_compile \
  "$QL_APP/collector_bot.py" "$QL_APP/autopilot_publish_bot.py" \
  "$QL_APP/meihua_publisher.py" "$QL_APP/run_user_bot.py" \
  "$QL_APP/publication_package.py" "$QL_APP/html_cover_renderer.py" \
  "$QL_APP/qiaolian_dual/listing_taxonomy.py" \
  "$QL_APP/qiaolian_dual/canonical_facts.py" \
  "$QL_APP/qiaolian_dual/canonical_fact_projection.py" \
  "$QL_APP/qiaolian_dual/area_admin.py" \
  "$QL_APP/v2/run_publisher_bot_v2.py"
sudo -u qiaolianbot env DB_PATH="$QL_APP/data/qiaolian_dual_bot.db" "$QL_APP/.venv/bin/python" "$QL_APP/check_workflow.py"
```

然后在管理员私聊执行 `/cover_test DRF_xxx`；确认封面只出现在私聊且中文字体、项目别名、物业类型、价格单位、区域、户型和裁切正确。另预览一张详情图，确认磨砂角标在安静角落、没有遮住主体；若仍显示英文品牌，说明进程未发现 CJK 字体，修正后重启 publisher。

## 9. 安装/核对 systemd 并启动

```bash
sudo install -m 0644 "$QL_APP/deploy/systemd/qiaolian-collector.service" /etc/systemd/system/
sudo install -m 0644 "$QL_APP/deploy/systemd/qiaolian-publisher-bot.service" /etc/systemd/system/
sudo install -m 0644 "$QL_APP/deploy/systemd/qiaolian-user-bot.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable qiaolian-collector.service qiaolian-publisher-bot.service qiaolian-user-bot.service
sudo systemctl start qiaolian-user-bot.service
sudo systemctl start qiaolian-publisher-bot.service
sudo systemctl start qiaolian-collector.service
sudo systemctl --no-pager --full status qiaolian-user-bot.service qiaolian-publisher-bot.service qiaolian-collector.service
```

Publisher Bot 重启时会刷新 Telegram 命令菜单。人工确认 `/send` 的说明为“打开已审核发布队列”，`/approve` 的说明包含 QC 编号与 A/B/C；如未刷新，先检查 publisher 日志，不要继续正式发布。

## 10. 人工冒烟

```bash
sudo journalctl -u qiaolian-user-bot.service -u qiaolian-publisher-bot.service -u qiaolian-collector.service --since '-10 minutes' --no-pager
```

在专用测试频道验证：

1. 各采集一条公寓、别墅、商办房源，`/pending` 应分别默认 A 标准、B 亮点、C 专业。
2. 预览时手动切换一次版本，点击“审核并冻结”；再次打开时必须保持冻结版本。
3. `/send` 应直接显示可点击的已审核队列，不要求复制内部 `DRF_` ID。
4. 完成一条四图混合语言房源：待审 → 预览 → 重做封面 → 审核冻结 → `/send` → 评论详情 → 用户 Bot 咨询/预约。
5. 在手机 Telegram 检查粗体、斜体、下划线、代码字体、删除线、caption 长度和按钮点击。
6. 用一条出售测试房源确认封面不出现 `/月`；用一条“仅出租，不出售”房源确认 gate 仍判定为租赁；用真实租售混合文案确认仍被阻断。
7. 用 `R&F City`、`Peng Huoth` 和一个明确 Level 2 地址各做一次人工 area 修正，确认 UI、封面、caption、listing 与 package 使用同一位置，并且冻结后不能再改。

确认 `publication_delivery_attempts.state='committed'` 后才允许正式频道流量。
