# 回滚命令

回滚前使用部署时记录的明确备份目录，不要用通配符猜测最新备份。

```bash
export QL_APP=/opt/qiaolian_dual_bots
export QL_BACKUP=/opt/backups/qiaolian-YYYYMMDD-HHMMSS
```

## 1. 停止服务

```bash
sudo systemctl stop qiaolian-collector.service qiaolian-publisher-bot.service qiaolian-user-bot.service
```

## 2. 优先执行代码回滚

本次 schema 变化为 additive。若数据库完整且问题只在代码，保留当前 DB，只恢复代码和原 systemd 文件：

```bash
sudo rsync -a --delete \
  --exclude '.env' --exclude 'data/' --exclude 'media/' --exclude 'logs/' \
  --exclude 'telethon_sessions/' --exclude '.venv/' \
  "$QL_BACKUP/app/" "$QL_APP/"
sudo cp -a "$QL_BACKUP/.env" "$QL_APP/.env"
sudo chown -R root:qiaolianbot "$QL_APP"
sudo chown -R qiaolianbot:qiaolianbot "$QL_APP/data" "$QL_APP/media" "$QL_APP/logs" "$QL_APP/telethon_sessions"
sudo chmod 0640 "$QL_APP/.env"
```

## 3. 仅在数据库迁移损坏时恢复 DB

先保留故障现场，再恢复部署前备份：

```bash
sudo cp -a "$QL_APP/data/qiaolian_dual_bot.db" "$QL_BACKUP/failed-after-deploy.db"
sudo sha256sum -c "$QL_BACKUP/qiaolian_dual_bot.db.sha256"
sudo cp -a "$QL_BACKUP/qiaolian_dual_bot.db" "$QL_APP/data/qiaolian_dual_bot.db"
sudo chown qiaolianbot:qiaolianbot "$QL_APP/data/qiaolian_dual_bot.db"
sudo chmod 0640 "$QL_APP/data/qiaolian_dual_bot.db"
sudo -u qiaolianbot sqlite3 "$QL_APP/data/qiaolian_dual_bot.db" "PRAGMA integrity_check; PRAGMA foreign_key_check;"
```

恢复旧 DB 会丢失部署后产生的草稿、线索、预约和发布状态，因此只有数据库确实损坏且业务负责人确认后才能执行。

## 4. 恢复服务

```bash
sudo systemctl daemon-reload
sudo systemctl start qiaolian-user-bot.service
sudo systemctl start qiaolian-publisher-bot.service
sudo systemctl start qiaolian-collector.service
sudo systemctl --no-pager --full status qiaolian-user-bot.service qiaolian-publisher-bot.service qiaolian-collector.service
sudo journalctl -u qiaolian-user-bot.service -u qiaolian-publisher-bot.service -u qiaolian-collector.service --since '-10 minutes' --no-pager
```

## 5. Telegram 对账

如果回滚原因发生在 `publication_delivery_attempts.state='sending'` 或 `unknown`，禁止重新执行 `/send`。先人工检查频道是否已有消息，再按 Telegram message IDs 与数据库 receipt 对账；只有确认“未发送”才允许创建新的人工处理方案。
