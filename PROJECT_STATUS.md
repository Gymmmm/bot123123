# 项目交付状态

更新时间：2026-08-10

## 当前结论

- 唯一主版本：本目录
- Git 分支：`agent/consolidate-single-version`
- GitHub：`Gymmmm/bot123123`
- 本地代码测试：部分通过；2026-08-10 结果为 91 passed、14 failed、2 skipped、21 subtests passed
- 本地数据库初始化：通过，发布流水线与用户 Bot 共用表已统一
- Telegram 测试环境：`@XxxXiaopengbot` 已联网，测试频道为 `@Jinbianzufanz`
- Mac 运行状态：用户 Bot 已由 `com.qiaolian.user` 常驻，开机登录后自动恢复
- 服务器现状：`132.243.218.75:22` 连接超时，未验证、未修改
- 正式交付：先完成新测试 Bot/频道效果验收，再决定生产迁移
- 最终归属：Bot 通过 BotFather 永久转让给朋友；朋友的群组/频道由朋友自己控制

## 已完成

- 合并移动端频道正文、评论区图片、深链咨询和预约流程。
- 当前代码移除硬编码 Telegram Token。
- 一键体检改为运行完整 pytest。
- 数据库初始化同时创建发布端与用户端所需表。
- 旧本地空数据库已保留备份。
- 当前跟踪文件未发现真实格式 Token。
- 已将旧 Git 工作树、5 月本地/服务器快照、临时补丁目录和严格重复 HTML 移入废纸篓；本目录已转为独立 Git 仓库。
- 旧工作树的唯一部署说明已并入 `docs/archive/`，旧提交保留为本仓库 `archive/*` 分支。
- Telegram `getMe`、频道管理员权限、真实发帖和删除回执已通过。
- 本地日志已禁止记录含 Token 的 Bot API INFO 请求，既有启动日志已脱敏。

## 已验证

- `.venv/bin/python -m pytest -q tests`
- 91 passed、14 failed、2 skipped、21 subtests passed；失败集中在旧按钮/文案断言、直发安全开关和当前测试品牌配置，因此不标记为完整验收通过
- `.venv/bin/pip check`：无损坏依赖
- workflow 只读检查：通过
- ready 发布 dry-run：通过，当前队列为空

## 当前剩余工作

1. 新建/接入独立发布 Bot Token；当前 USER/PUBLISHER 配置指向同一个 Bot，已禁止同时长轮询。
2. 由朋友在 Telegram 打开 `@XxxXiaopengbot`，发送 `/start` 并走一遍咨询/预约。
3. 修复或确认最新未提交界面改动对应的 14 项测试预期差异。
4. 选择可连通的新服务器后迁移 Mac 常驻服务。
5. 朋友验收后，通过 BotFather 完成所有权转让。

旧公开历史 Token 属于后续安全清理项，不阻塞新测试环境。

## 验收状态定义

- 已写代码：是
- 已本地测试：是
- 已本地完整启动：用户 Bot 是；发布管理 Bot 否（需独立 Token）
- 已真实联网：是，用户 Bot 与频道发帖/删除已验证
- 已交给朋友测试：否
- 已完成验收：否
