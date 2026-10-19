# QiaolianBot 项目状态

更新时间：2026-10-20（Asia/Phnom_Penh）

## 当前结论

- GitHub 主仓库：`Gymmmm/bot123123`
- 当前封面/频道版本分支：`codex/mobile-cover-landscape-20261016`
- 当前生产 SHA：`9945bb1dad912cf7909c67aba7a6758c784f6020`
- 生产目录：`/opt/qiaolian_v3/current`
- 正式数据库：`/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db`
- 四个 V3 服务：canonical、collector、publisher、user 均为 active
- 最新完整 V3 回归：1214 passed
- Telegram 手机端最终验收：仍须以实际新帖子和用户手机点击结果为准

## 已上线

- 1200×800、3:2 手机端房源封面。
- 一大二小和一大三小横向拼图。
- 可选上大下二、上大下三模板能力。
- 主贴使用 Telegram 原生相册：合成封面加真实照片。
- 主贴按钮与 User Bot 房源详情、预约、中文顾问流程相连。
- 正式发布保留 Frozen Publication Package、receipt 和 publication instance。

## 当前交接入口

- 普通运营入口：[FRIEND_HANDOFF.md](FRIEND_HANDOFF.md)
- 按钮、文字和完整流程：[docs/QIAOLIANBOT_BUTTON_TEXT_FLOW_HANDOFF.md](docs/QIAOLIANBOT_BUTTON_TEXT_FLOW_HANDOFF.md)
- 生产部署与恢复：现有 deploy-control 工作流和服务器运行配置

## 尚未完成

- 本轮新版式尚未产生新的频道帖子手机端验收记录。
- 服务器图片目录约2.7GB，尚未按数据库引用关系完成安全去重。
- 历史 release、dry-run 输出和本地旧 worktree 尚未执行最终归档清理。

## 清理边界

- 不删除正式数据库。
- 不删除数据库或历史帖子仍引用的图片。
- 不直接清空历史 release；至少保留当前版本和一个已验证回滚版本。
- 所有清理先生成引用清单和备份，再移动到隔离目录，最后才决定永久删除。
