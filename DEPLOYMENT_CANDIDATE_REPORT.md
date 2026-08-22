# DEPLOYMENT CANDIDATE REPORT

## 0. 审计范围
- Repository: `Gymmmm/bot123123`
- Branch: `codex/server-snapshot-complete-20260822`
- Audited base commit: `c450bddbf4c727f5f3660007b63eab0cc74bfafd`
- Audit result commit: `e9650637f6ab4c32ca931d7457af9805a9a49369`

## 1. 已验证事实
- 采集入口为 `collector_bot.py:main_async()`，注册：
  - `events.NewMessage(... grouped_id is None)` → `handle_single_message()`
  - `events.Album(...)` → `handle_album()`
- 两条采集路径最终都调用 `persist_source_post(...)` 写入 `source_posts`。
- `persist_source_post(...)` 的关键入参来源：
  - 单条：`message.id`、`message.message`、下载后的 `raw_images/raw_videos`、`message.grouped_id`
  - 相册：`anchor.id`、`event.text/event.raw_text`、遍历相册下载后的 `raw_images/raw_videos`、`grouped_id`
- `persist_source_post(...)` 在写库前执行 `sanitize_source_text(raw_text)`，并将清洗结果写入 `raw_meta_json.sanitized_text`，提取的联系方式写入 `raw_contact`。
- 自动采集场景下 `classify_after_insert=True`，因此 `persist_source_post(...)` 成功写入后会立即触发：
  - `AIParserModule.process_single_source_post(source_post_pk)`
  - `publication_package.build_package(db_path, draft_id)`
- `source_posts -> drafts` 当前不是依赖独立 `run_pipeline_autopilot.py`；生产主链已改为采集后即时解析/建包。
- 当前 ready/approved 队列消费逻辑在 `autopilot_publish_bot.py`，由 v2 发布 Bot 进程内复用。
- `source_sanitizer.py` 会去除：
  - 电话
  - 微信/WeChat/WX
  - Telegram 用户名 / `t.me`
  - 来源频道链接
  - 中介推广话术
- 公开文案在 `publication_package.build_package()` 中经过：
  - `_public_clean_text(...)`
  - `assert_public_output_safe(...)`
  因此来源联系方式/来源归因若残留会直接阻断构包。
- 非出租源在 `ai_parser.py` 中被 `hard_flags` 拦截，`parse_status` 标记为 `skipped_non_rental`。
- 无价格源在 `ai_parser.py` 中被 `hard_flags` 拦截，`parse_status` 标记为 `skipped_no_price`。
- 无明确公开区域/项目、无户型、无价格、图片不足、无封面时，`evaluate_publishability(...)` / `evaluate_publish_gate(...)` 会阻断自动发布。
- `publication_package` 在审核后冻结：
  - `approved_package_frozen`
  - `snapshot_json`
  - `content_hash`
  - `frozen_file_hashes`
- 发布前 `meihua_publisher.py` 强制读取 approved package；无 approved package 会退回 `pending` 并记日志。
- 发布状态链已实现：
  - `pending/ready/approved` → `publishing` → `published`
- 发布失败后的安全性：
  - 发送前失败：`mark_failed_before_send(...)`，草稿状态回退
  - Telegram 调用后异常：`mark_unknown(...)`，熔断自动重发，等待回执恢复或人工对账
- 并发保护已存在于：
  - `publication_delivery_attempts UNIQUE(package_id, channel_chat_id)`
  - 已发布 `listing_id` 查重
  - v2 进程单实例锁 `v2/run_publisher_bot_v2.py`
- deep link 已验证：
  - `listing_id` 统一为 `l_<id>`
  - `public_token/post_token` 都有当前格式与历史格式兼容解析
  - 咨询/预约共享同一 listing 上下文
  - 预约仍走既有预约状态机
- User Bot 的 `context.user_data is None` 修复存在于 `qiaolian_dual/message_handlers.py`。
- `/start`、房源详情、收藏、咨询、预约、顾问通知、leads、appointments 都已有测试覆盖。
- PR #21 的 `Procfile` 仅启动 `v2_admin/admin_server:app`，不直接启动 collector / publisher / user bot。
- systemd 主链仍是：
  - `qiaolian-collector.service`
  - `qiaolian-publisher-bot.service`
  - `qiaolian-user-bot.service`
  - `qiaolian-admin-web.service`（可选）
- 测试默认使用临时 DB / 临时下载目录 / 临时 Telethon session：
  - `tests/conftest.py`
- 本次完整测试命令：
  - `python -m pytest tests/ -v`

## 2. 修复内容
- 修复最小调度缺口：v2 发布 Bot 在 simple mode 下重新挂载 `tick_schedules`，无需恢复旧的独立 `run_pipeline_autopilot.py` 架构。
- 新增测试，验证：
  - `register_autopilot_features(..., simple_mode=True, enable_scheduler=True)` 会挂载 scheduler
  - `v2/qiaolian_publisher_v2/bot.py:main()` 会以 simple mode + scheduler 启动 autopilot 特性

## 3. 仍存在的问题
- 尚未连接真实 Telegram，因此以下只完成代码级和测试级验证，未做线上实发验收：
  - 真实频道消息发送
  - 真实 discussion thread 映射
  - 真实用户点击 deep link
  - 真实预约通知链路
- Admin Web 仍属可选运维入口，不属于三个 Bot 的必需主链。
- 测试有 warnings（主要为 PTB ConversationHandler 提示与 Pillow/UTC deprecation），不阻塞本次测试环境部署，但建议后续清理。

## 4. 数据库迁移
- 需要确保目标库已执行共享 schema bootstrap：
  - `python scripts/bootstrap_db.py`
- 本次代码修改本身**没有新增 schema 变更**。
- 但 ready/approved/published 链路依赖以下既有表存在：
  - `source_posts`
  - `drafts`
  - `publication_packages`
  - `publication_delivery_attempts`
  - `posts`
  - `listings`
  - `leads`
  - `appointments`

## 5. 需要安装的依赖
- 运行主仓库：
  - `pip install -r requirements.txt`
- 其中本链路关键依赖包括：
  - `python-telegram-bot[job-queue,socks]==21.*`
  - `telethon`
  - `Pillow`
  - `python-dotenv`
  - `flask==3.1.3`
  - `waitress==3.0.1`

## 6. 需要重启的服务
- 必需：
  - `qiaolian-collector.service`
  - `qiaolian-publisher-bot.service`
  - `qiaolian-user-bot.service`
- 可选：
  - `qiaolian-admin-web.service`

## 7. 部署顺序
1. 备份代码与数据库
2. 更新代码到目标分支/提交
3. 安装依赖
4. 执行 `python scripts/bootstrap_db.py`
5. 重启 `qiaolian-collector.service`
6. 重启 `qiaolian-publisher-bot.service`
7. 重启 `qiaolian-user-bot.service`
8. 如需后台管理页，再重启 `qiaolian-admin-web.service`
9. 执行部署后健康检查

## 8. 回滚顺序
1. 停止或回滚 `qiaolian-admin-web.service`（若本次启用）
2. 回滚代码到上一个已验证提交
3. 重启 `qiaolian-user-bot.service`
4. 重启 `qiaolian-publisher-bot.service`
5. 重启 `qiaolian-collector.service`
6. 保留当前数据库；本次无新增 schema，原则上无需 DB 回滚

## 9. 部署后健康检查
- `systemctl is-active qiaolian-collector.service`
- `systemctl is-active qiaolian-publisher-bot.service`
- `systemctl is-active qiaolian-user-bot.service`
- 如启用后台：
  - `systemctl is-active qiaolian-admin-web.service`
- 检查日志中无持续报错：
  - collector 能正常监听源
  - publisher 能正常挂载 `tick_schedules`
  - user bot 能正常启动 polling
- 抽查数据库状态：
  - 新采集消息进入 `source_posts`
  - 自动生成 `drafts`
  - 自动生成 `publication_packages`
  - approved 后可进入 ready/approved 队列并被消费

## 10. 真实 Telegram 最短验收流程
1. 在测试源频道发送一条带足量图片的出租房源
2. 确认 collector 写入 `source_posts`
3. 确认自动生成 draft 与 package
4. 在发布 Bot 中完成审核并 approved
5. 等待 scheduler 槽位或手动 `/send`
6. 确认频道帖成功发布
7. 点击“咨询这套”
8. 点击“预约看房”
9. 确认 User Bot 进入正确房源上下文
10. 确认 `leads`、`appointments` 均新增正确记录

## 11. 测试结果
- Command: `python -m pytest tests/ -v`
- Total collected: `163`
- Passed: `161`
- Failed: `0`
- Skipped: `2`
- Warnings: `67`
- Subtests passed: `21`

## 12. 修改文件列表
- `autopilot_publish_bot.py`
- `v2/qiaolian_publisher_v2/bot.py`
- `tests/test_launch_readiness.py`
- `DEPLOYMENT_CANDIDATE_REPORT.md`

## 13. 部署结论
- 是否达到“可以部署到测试环境”：**是**
- 是否达到“可以部署到生产”：**有条件可部署**

### 仍未通过的门槛
- 尚未完成真实 Telegram 测试频道验收
- 尚未在目标 VPS 实际验证 systemd 重启后的运行日志
- 尚未在真实环境完成最短咨询/预约闭环演练
