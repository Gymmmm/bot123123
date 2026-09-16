# 侨联地产双 Bot · 回滚文档

## 回滚原则

回滚只恢复代码，不删除或重写生产数据库、WAL 文件、媒体、Telethon session 或历史 publication packages。已发布的 Telegram 帖子不做修改。回滚前必须暂停自动发布入口，保存当前服务状态和最近日志。

## 本轮关键备份

预约幂等变更前的文件备份位于服务器：

```text
/opt/qiaolian_dual_bots/backups/pre-appointment-idempotency-20260820-appointment_flow.py
```

当前已发布包仍保留在：

```text
/opt/qiaolian_dual_bots/media/publication_packages/PKG_4f991a659a22_v6/
```

## 回滚步骤

```bash
cd /opt/qiaolian_dual_bots
cp -a qiaolian_dual/appointment_flow.py backups/rollback-current-$(date -u +%Y%m%dT%H%M%SZ).py
cp -a backups/pre-appointment-idempotency-20260820-appointment_flow.py qiaolian_dual/appointment_flow.py
.venv/bin/python -m py_compile qiaolian_dual/appointment_flow.py
systemctl restart qiaolian-user-bot.service
systemctl is-active qiaolian-user-bot.service
journalctl -u qiaolian-user-bot.service -n 80 --no-pager
```

如果需要回滚更早的代码，必须使用已确认的生产备份中的对应文件，并逐一执行 `sha256sum`、`py_compile` 和 systemd 路径核对。禁止凭文件名猜测备份版本，禁止恢复数据库。

## 回滚后检查

确认三项服务状态、数据库可读性、公开门禁、approved package 状态和 User Bot `/start` 链接。若回滚涉及 Publisher，禁止重新发布已经存在 posts 记录的 draft；先确认重复发布保护仍返回跳过。若预约幂等被回滚，必须明确记录为已知风险，不得把回滚版本标记为最终生产基线。
