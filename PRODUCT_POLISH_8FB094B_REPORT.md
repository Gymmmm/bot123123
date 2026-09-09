# PRODUCT_POLISH_8FB094B_REPORT

## 1. Baseline

base SHA：`8fb094baba9eb7ef350e55b414960708e4d18b38`

工作分支：`codex/product-polish-8fb094b-20260909`

验证：
- `git merge-base HEAD 8fb094baba9eb7ef350e55b414960708e4d18b38` 精确等于基线 SHA。
- `git diff --check 8fb094baba9eb7ef350e55b414960708e4d18b38..HEAD`：PASS。
- 本次未部署、未启动正式 Bot、未操作正式频道、未 merge。

## 2. Changed

### 2.1 频道房态与预约按钮联动

- 问题：频道新帖固定显示预约按钮；`reserved` 对外状态也与 User Bot 语义不一致；已发布消息状态变化后需要同步按钮。
- 文件：
  - `v3_core/status_labels.py`
  - `v3_core/publishing/channel_renderer.py`
  - `v3_core/publishing/delivery_coordinator.py`
  - `v3_core/publishing/telegram_adapter.py`
  - `v3_core/user_bot/listing_presenter.py`
  - `v3_core/user_bot/channel_status_sync.py`
- 修改方式：把既有 User Bot 房态展示语义集中到共享 V3 status helper；`active/reserved` 才允许预约；`pending/rented/inactive/offline/unknown` 不显示预约。新帖发送读取当前 listing 的 live `inventory_status`；已发布消息编辑时 caption 与 keyboard 一起同步。
- 测试：覆盖 active、reserved、pending、rented、inactive、offline；验证 reserved=`🟡 已有预约 · 仍可预约`；验证 pending 同步后已发布消息去除预约键。

### 2.2 `/admin` 今日预约日期兼容

- 问题：读取端漏掉 `MM-DD`，可能误报“今天暂无预约”。
- 文件：
  - `v3_core/user_bot/appointments.py`
  - `v3_core/user_bot/admin_appointments.py`
- 修改方式：增加读取端统一日期匹配 helper；兼容 `YYYY-MM-DD`、`MM-DD`、`M月D日`，不修改历史预约原数据。
- 测试：三种日期格式同一天都能被今日预约读取。

### 2.3 顾问通知 source 人类可读

- 问题：ADMIN_IDS 通知会直接显示 `hub`、`listing_callback` 等内部 slug。
- 文件：
  - `v3_core/user_bot/source_display.py`
  - `v3_core/user_bot/admin_notification_plans.py`
  - `v3_core/user_bot/listing_contact.py`
- 修改方式：增加集中式展示 mapping；原始 source 入库不变。已覆盖：`hub -> 首页联系我们`、`listing_callback -> 房源咨询`、`daily_broadcast -> 每日广播咨询`、`user_search -> 找房咨询`、`channel/channel_deeplink -> 频道房源`、`search_result -> 找房结果`；未知值安全回退 `用户咨询`。
- 测试：覆盖 hub、listing_callback、daily_broadcast、unknown fallback。

### 2.4 找房无匹配页补“联系我们”

- 问题：文案说顾问可继续留意，但页面没有联系按钮。
- 文件：
  - `v3_core/user_bot/search_no_match_view.py`
  - `v3_core/user_bot/transition_callbacks.py`
- 修改方式：增加 `💬 联系我们`，复用现有 `v3u:home:contact` 流程，不新增第二套咨询逻辑。
- 测试：关键字找房、引导筛选、自定义预算无匹配路径均验证 callback 为现有 contact flow。

### 2.5 房源详情编号字段修正

- 问题：`📸 实拍：{public_id}` 错把 public ID 当实拍字段。
- 文件：`v3_core/user_bot/listing_responses.py`
- 修改方式：仅改显示为 `🆔 房源编号：{public_id}`，public_id 逻辑不变。
- 测试：验证新字段存在，旧字段不存在。

### 2.6 管理员预约详情看房方式人类可读

- 问题：管理员页直接显示 `offline/video`。
- 文件：`v3_core/user_bot/admin_appointments.py`
- 修改方式：复用现有 `APPOINTMENT_MODE_LABELS`，显示 `实地看房/实时视频看房`。
- 测试：offline/video 两种模式覆盖。

### 2.7 富力分类返回路径

- 问题：富力分类页返回到生活服务大首页。
- 文件：`v3_core/user_bot/telegram_service_handler.py`
- 修改方式：分类页运行时返回键改为 `⬅️ 返回富力导航`，callback=`v3u:service:rfcity`；商家正文未修改。
- 测试：验证按钮文字与 callback。

### 2.8 已发布预约显示真实 public ID

- 问题：预约已经关联已发布 listing 时，管理员页仍可能显示“待生成”。
- 文件：`v3_core/user_bot/admin_appointments.py`
- 修改方式：通过预约已有 `listing_id` 关联 `listings_v3`，只读取真实存在的 `public_listing_id`；无真实映射时仍保持“待生成”，不伪造编号。
- 测试：临时 DB 中存在真实映射时，今日预约与预约详情均显示真实 QL public ID。

### 2.9 advisor_url / CHANNEL_URL 安全检查

- 现状确认：当前基线已有安全 fallback，不需要新增业务分支。
- `advisor_url` 为空：联系人按钮编码为现有 Bot 内 `v3u:home:contact` callback，不生成死 URL。
- `CHANNEL_URL` 为空：首页不生成“🏠 最新房源”URL 按钮。
- 测试：两种缺失配置均有回归覆盖；房源咨询页 advisor_url 为空同样回退现有 contact flow。

## 3. Not Changed

- Collector 未改。
- Canonical Parser 未改。
- Dedupe 主逻辑未改。
- listing identity 规则未改。
- Auto Publish Gate 未改。
- DB core schema 未改。
- Frozen Publication Package 主链未改；仅在 Telegram 实际发送命令上附带 live inventory status，用于决定用户可见预约按钮，不改变 frozen snapshot/caption 主设计。
- User Bot 未重写。
- Publisher Bot 未重写。
- 未引入第二套发布链。
- 未删除旧数据兼容代码。
- 未修改生产服务器、正式频道或生产数据库。

## 4. Runtime verification required

以下全部标记 `NEEDS_RUNTIME_VERIFY`，本 PR 不猜测生产环境：

1. `NEEDS_RUNTIME_VERIFY`：生产 `CHANNEL_URL` 当前是否有值。
2. `NEEDS_RUNTIME_VERIFY`：生产 `advisor_url / ADVISOR_URL` 当前是否有值。
3. `NEEDS_RUNTIME_VERIFY`：`/start latest` 现网是否真正按“最新发布”排序。
4. `NEEDS_RUNTIME_VERIFY`：异常页“补充或修改资料”对应 `v3edit` 是否在当前 release 完整注册。
5. `NEEDS_RUNTIME_VERIFY`：当前 release 实际磁盘是否存在 `handover.png`、`handover.pdf`、`deposit.png`、`deposit.pdf`。
6. `NEEDS_RUNTIME_VERIFY`：现网广播当前 `button_key` 是否为 `none`。

## 5. Product decisions deferred

本次只检查、不实现：

- `/admin` 是否恢复“新咨询/房源/服务/来源/历史”。
- 顾问预约通知是否增加确认/无效按钮。
- 顾问报修通知是否增加处理按钮。
- 历史预约是否做完整详情页。
- 物业协调是否改成结构化工单。
- 保洁家政是否新增独立流程。
- “停止发布”和“标记已下架”是否合并。
- `repair_power` 是否新增菜单入口。
- 富力第三方地产商家是否删除。

## 6. Tests

- exact baseline ancestry：PASS。
- `git diff --check`：PASS。
- compile：PASS。
- V3 production regression command：`python -m pytest -q tests/v3_core`
- 结果：`472 passed, 28 warnings`。
- 基线为 `453 passed`；本次新增 19 个测试，原有 V3 测试数量未减少，最终 453 + 19 = 472 全部通过。
- Publisher/User Bot regression tests 均包含在上述 `tests/v3_core` 完整套件并通过。

补充诊断：曾尝试从仓库根目录直接执行不限定范围的 `python -m pytest -q`，该命令在测试执行前即因仓库内历史非 V3 测试依赖已不存在的旧模块而出现 10 个 collection errors（例如旧 `qiaolian_pipeline` / `run_pipeline_autopilot` / `v2_admin` 等）。这些不属于当前 453-test V3 生产基线，本次没有修改、skip、xfail 或恢复这些历史链来掩盖该问题。

## 7. Final verdict

READY_FOR_REVIEW
