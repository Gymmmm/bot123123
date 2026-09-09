# V3 Core Extraction — 租后服务 / 运营补齐审计

基线：`codex/v3-core-extraction-20260908` @ `554915233145620c631690a408832bbc8809e7de`

## 已有能力

V3 Core 已经把采集、canonical inventory、发布包、Telegram publication instance、预约、lead、租客绑定、报修和广播拆成独立模块。现有 `service_repository.py` 能保存租客绑定和报修工单；`user_bot/service_flow.py` 已具备报修品类、详情、时段和幂等 request token；`publication_instances` 能保存正式 Telegram 帖子的稳定外部身份。

## 当前不足

1. **租后数据与房源主链脱节。** `tenant_bindings_v3` 只有 `property_name`，没有 `draft_id / listing_id / offer_id / post_id`。因此无法稳定回答“这个租客来自哪套 canonical listing、哪个出租 offer、哪条频道帖子”。
2. **报修只有提交，没有运营闭环。** `repair_tickets_v3` 只有 `new` 状态和基本描述，没有受理、指派、处理中、完成、取消的状态机，没有负责人、优先级、截止时间和事件历史。
3. **没有租约/交付生命周期实体。** 现有 tenant binding 更像用户入口绑定，不足以承载交房、在租、续租/到期、退租等运营阶段。
4. **没有统一运营队列。** 报修、交房、保洁、水电网、合同跟进、租金跟进、巡检、退租无法进入同一优先级队列。
5. **“drafts/listings/posts”关系没有落到租后。** V3 Core 本身没有独立 `drafts_v3` 真相表；真正的房源真相是 `listings_v3`，真正的已发布帖子身份是 `publication_instances`。因此新增层应保存 `draft_id` 作为来源追踪，但必须校验 `listing_id`，有 `post_id` 时必须校验该 publication instance 确实属于同一 listing，不能再造一套房源/帖子表。
6. **缺少跨层一致性保护。** 当前可以创建一个与 canonical listing 无关联的 tenant binding / repair ticket；后续统计、回访、租后转运营都会失去可追踪性。

## 本分支补齐方式

新增三个完全 additive 的表：

- `rental_cases_v3`：租后/租约运营主档，绑定 `draft_id -> listing_id -> post_id`，同时可记录 `offer_id` 和旧 `tenant_binding_id`。
- `operations_tasks_v3`：统一运营任务，覆盖 `repair / handover / cleaning / utilities / contract / rent_followup / inspection / moveout / other`。
- `operations_task_events_v3`：状态变更审计轨迹。

约束：

- `listing_id` 必须存在于 `listings_v3`。
- `offer_id` 存在时必须属于同一 listing。
- `post_id` 存在时必须是 `publication_instances.instance_id`，且属于同一 listing。
- `draft_id` 只做 provenance，不被提升为 V3 真相源。
- 运营任务使用 request token 幂等。
- 状态机仅允许：`new -> acknowledged/assigned -> in_progress -> resolved`，可在处理中取消；完成后不可倒退。
- `assigned/in_progress` 必须有 assignee。
- 运营队列按 `urgent > high > normal > low` 排序，再按 due time / created time。

## 合并影响

本改动不修改 parser、inventory materializer、publisher、User Bot 文案和 Telegram 协议；不导入 legacy runtime；只在显式 `initialize_v3_storage()` 时增加三个 V3 表，并新增 repository/test。适合作为独立 commit/PR 合入 Core Extraction，冲突面主要只有 `v3_core/storage/bootstrap.py` 的 import / REQUIRED_V3_TABLES / DDL_BLOCKS 三处。
