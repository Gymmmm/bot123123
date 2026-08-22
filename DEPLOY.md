# 侨联地产双 Bot · 部署文档

## 生产基线

生产目录为 `/opt/qiaolian_dual_bots`，运行用户为 `qiaolianbot`。系统使用 Python 3.12 虚拟环境 `/opt/qiaolian_dual_bots/.venv` 和 SQLite WAL 数据库 `data/qiaolian_dual_bot.db`。生产服务为 `qiaolian-collector.service`、`qiaolian-publisher-bot.service` 和 `qiaolian-user-bot.service`。

## 发布流程

所有新房源必须先从来源频道进入 source_posts 和 media_assets，再经过 Parser 清洗、canonical area normalization、draft 质量评估、publication package 构建和人工审批。`package_ready` 只代表可审核预览，不代表公开。Publisher 只允许读取状态为 approved 的冻结包；正式发送成功并写入 posts 后，业务房态才可转为 active。

正式发布前必须核对 source identity、grouped_id、媒体资产数量、listing snapshot、cover identity、正文 content_hash 和 public_token。公开正文、封面和链接不得包含 `{{...}}`、B/QC/L/l_ 内部编号或测试标记。具体区域不明确的房源继续 pending，不得用“金边”替代具体区域，也不得猜测区域。

## 服务操作

```bash
cd /opt/qiaolian_dual_bots
systemctl status qiaolian-collector.service qiaolian-publisher-bot.service qiaolian-user-bot.service
journalctl -u qiaolian-collector.service -u qiaolian-publisher-bot.service -u qiaolian-user-bot.service -n 100 --no-pager
```

部署代码变更后，应先备份实际变更文件，执行生产虚拟环境 `py_compile`，再按依赖顺序重启受影响服务。涉及用户 Bot 的代码变更后重启 `qiaolian-user-bot.service`；Publisher 变更后重启 `qiaolian-publisher-bot.service`；Collector 变更后重启 `qiaolian-collector.service`。不要直接删除数据库、publication_packages、media 或 Telethon session。

## 部署后验收

发布后必须回读 posts 的 draft_id、listing_id、channel_message_id、media_group_id 和 publish_status；打开频道公开预览核对正文与图片；从频道 opaque Deep Link 进入 User Bot；按 canonical area 搜索验证房源；完成一次真实预约并检查 appointments 入库；重复确认必须保持单条预约；最后使用 `/listing_status` 确认房态，并确认三项服务处于 active。
