# 修改文件清单

## r3：针对独立复测 17 项失败的修复

- `schema_core.sql`：把 `listings` 纳入核心冷启动 schema。
- `qiaolian_dual/canonical_listing_materializer.py`：materializer 幂等创建完整 `listings` 表。
- `meihua_publisher.py`：缺 approved package、坏媒体和发布阻断统一回退 `pending` 并记日志；频道咨询键盘恢复受跟踪入口。
- `autopilot_publish_bot.py`：修复没有 package 表或 approved package 的孤立 ready 队列项。
- `collector_bot.py`：分离纯持久化和自动分类边界；真实单图/相册 handler 显式开启分类。
- `qiaolian_dual/app.py`、`user_bot.py`、`flows.py`、`appointment_flow.py`、`start_routes.py`、`callbacks.py`、`callback_search.py`：恢复拆分前兼容调用合同与依赖传递。
- `qiaolian_dual/keyboards_common.py`、`messages.py`、`v2/qiaolian_publisher_v2/keyboards.py`：同步首页、品牌欢迎语和频道四动作按钮。
- `html_cover_renderer.py`、`publication_package.py`：renderer 边界 canonical-first；补 QC/ref、项目别名、物业类型、租售单位与 highlights 同源映射。
- `tools/publish_houses_csv.py`、`scripts/houses_csv_pipeline.py`、`tools/publish_zufang555.py`：关闭直接发送旁路。
- `scripts/publish_ready_batch.py`：只发 draft 与 frozen package 均为 approved 的记录。
- `tools/post_three_caption_samples.py`：只读 approved package 的冻结文案和封面，禁止重建冻结媒体。
- `requirements.txt`：补 Telegram SOCKS 可选依赖声明。
- `tests/test_release_retest_contracts.py`：新增 canonical 封面和批量发布门禁专项回归。

本轮全量结果：`156 passed, 0 failed, 2 skipped`。

以下为完整隔离验收中直接修改或新增的文件。未修改生产服务器。

## 本轮 area、gate 与视觉收敛

- `qiaolian_dual/listing_taxonomy.py`：成为区域、市场位置、项目和物业类型的唯一分类源；新增 Level 1/Level 2 管理别名解析、物理区域/市场查询、候选排序与歧义阻断；公开值统一为“太子幸福广场”。
- `qiaolian_dual/area_normalization.py`：改为从 taxonomy 派生的兼容适配层；物理区域 catalog 不再混入项目、商圈或“金边市区”。
- `qiaolian_dual/area_admin.py`：人工修正同步更新全部 canonical/public location 字段、层级、状态、普通列和 listing 投影；重算 hash、重新 gate、写审计并保护冻结 package；支持可证明的旧工具 hash 痕迹恢复。
- `qiaolian_dual/location_mapping.py`：用户搜索别名和显示值改为从 taxonomy 派生；旧“太子/幸福”只作兼容输入别名。
- `qiaolian_dual/common.py`：用户 Bot 地区按钮和提示读取统一 location mapping；移除“金边市区”。
- `qiaolian_dual/search.py`：地区筛选通过统一别名 catalog 展开。
- `qiaolian_dual/canonical_fact_projection.py`：强化城市、物理区域、市场候选、public key/display、publication level 和项目层级的结构校验。
- `qiaolian_dual/canonical_listing_materializer.py`：直接复用 `facts.public_location_key`，不再用遗漏 Level 2 的第二套算法重算。
- `qiaolian_dual/canonical_facts.py`：删除平行死 taxonomy；统一调用 taxonomy 公共位置算法；过滤“不出售/非出售/not for sale”等否定表达，同时保留真实租售混合阻断。
- `publication_package.py`：封面投影加入 `deal_type`、`project_alias`、`property_type` 和价格后缀；项目别名/物业类型不再丢失，出售不再显示 `/月`。
- `html_cover_renderer.py`：模板 token 支持项目别名、物业类型、交易类型和价格后缀；旧模板的硬编码 `/月` 改为事实驱动。
- `templates/property/01_经典蓝卡模板.html`：租售价格单位使用 `PRICE_SUFFIX`。
- `meihua_publisher.py`：详情图改为自动选择安静角落的磨砂品牌角标；不遮中心、不改变比例；缺 CJK 字体时回退英文品牌字样。
- `user_bot_production.py`：删除。该文件未被 systemd、当前入口或测试引用，实际用户 Bot 始终为 `run_user_bot.py → qiaolian_dual.app`。
- `tests/test_area_gate_single_source.py`：新增 17 项 taxonomy、人工 area、gate、hash、否定出售、materialize 和封面回归。
- `tests/test_channel_mobile_listing_v2.py`：新增详情图角标比例、中心保护和无 CJK 字体回退测试。

## 生产代码

- `ai_parser.py`：Canonical draft 统一写入；可选字段只在新值有效时更新，避免空值覆盖；费用与入住事实投影。
- `autopilot_publish_bot.py`：待审自动选择物业默认 A/B/C；审核前可预览改选；冻结后只读 package 版本；`/approve QC编号 [A/B/C]`；`/send` 可点击已审核队列；缺 package 表时安全跳过。
- `collector_bot.py`：分类与打包使用当前注入 DB；保持单图 caption、相册顺序和采集幂等。
- `collector_db_compat.py`：缺少 drafts 表时不执行无效 ALTER。
- `db.py`：恢复 `DatabaseManager` 兼容导出并补 Canonical listing 字段。
- `html_cover_renderer.py`：支持 Playwright 自带 Chromium 和显式 executable 回退；租售、项目别名及物业类型 token 见上节。
- `meihua_publisher.py`：集中物业类型默认文案路由；新增 A 标准信息、B 亮点价格、C 专业参数三版 Telegram HTML；显式原价删除线；结构化评论详情；冻结包发布、公开文本安全门、opaque deep link 和直接批量发布禁用；详情图角标见上节。
- `migration.sql`：加入 durable publication delivery 表与索引。
- `publication_delivery.py`：新增持久、幂等、可恢复且对不确定发送 fail closed 的发布状态机。
- `publication_package.py`：默认文案从 Canonical 物业类型决定；允许审核动作显式覆盖并固化 `caption_variant`；补齐 schema 迁移、唯一 package/token、冻结规则、source identity 和媒体 hash；修正封面模板路由与租售字段投影。
- `review_queue_view.py`：新增生产待审视图兼容辅助模块。
- `schema_core.sql`：补齐 Canonical draft/package 字段、package 唯一索引和 delivery 状态表。
- `source_sanitizer.py`：同一行联系人清除时保留前部房源事实。
- `qiaolian_dual/area_admin.py`：人工区域修改校验、审计、重投影、重哈希、遗留痕迹恢复和冻结保护。
- `qiaolian_dual/callback_listing.py`：列表咨询回调先捕获会话上下文。
- `qiaolian_dual/canonical_fact_projection.py`：listing/package 统一投影原价、入住、押付、租期、费用和看房字段；新增严格地理层级校验。
- `qiaolian_dual/canonical_facts.py`：只解析清洗文本、原文仅存 hash；显式原价/现价；费用、入住和看房细节；英文 bedrooms/bathrooms；删除重复 taxonomy 并修复否定出售词。
- `qiaolian_dual/canonical_listing_materializer.py`：listing 写入 `hidden_costs`/`available_date`；UPSERT 对可选字段使用非空保护；pending draft 投影不依赖 listings 表；复用 canonical public key。
- `qiaolian_dual/db.py`：Canonical listing 字段；缺 pipeline 表时查询 fail closed；删除重复公开状态实现。
- `qiaolian_dual/flows.py`：通用预约、已知模式跳步和完整 session/focus 保留。
- `qiaolian_dual/listing.py`：咨询按钮改为 session 捕获回调。
- `qiaolian_dual/listing_taxonomy.py`：Latin alias token 边界、显式项目、项目/区域/类型分层，以及全链路唯一 location catalog。
- `qiaolian_dual/message_handlers.py`：优先处理进行中的引导状态，避免被自然语言入口截断。
- `qiaolian_dual/user_bot.py`：保留拆分前状态常量兼容导出。
- `v2/qiaolian_publisher_v2/bot.py`：恢复相册/Canonical 入库/私聊封面预览；管理员质量、看板、发布队列回调接通；Bot 命令说明更新；永久关闭 `/send_variants`。
- `v2/qiaolian_publisher_v2/keyboards.py`：管理员面板新增“已审核待发布”入口。

## 测试代码

- `tests/conftest.py`：每次 pytest 使用临时 DB、媒体目录和 Telethon session。
- `tests/test_admin_caption_workflow.py`：新增显式优惠价、评论详情、未标注双价拦截、物业默认版、HTML 平衡和管理员发布队列测试。
- `tests/test_bootstrap_db.py`：使用当前 Python 解释器，不硬编码 `.venv`。
- `tests/test_caption_variant_queue.py`：定时发布必须读取 approved package 的冻结文案版。
- `tests/test_channel_mobile_listing_v2.py`：更新移动端 caption、评论提示、deep link 和同源价格断言。
- `tests/test_final_acceptance_hardening.py`：事实分层、混合语言、清洗、幂等、冻结、深链、v2 安全入口和 E2E；显式验证冻结 B 版不被改写。
- `tests/test_intake_regressions.py`：更新为当前 collector 媒体追加接口。
- `tests/test_management_new_unified_pipeline.py`：旧直发开关即使设置环境变量也始终关闭。
- `tests/test_publication_delivery.py`：正常提交、重复调用、durable receipt 恢复、unknown 阻断。
- `tests/test_publish_tags.py`：三版事实一致、默认路由、标签、状态文案和评论入口。

## 发布包清理

- `RELEASE_NOTES.txt`：移除旧生产样本与旧日期，改为本次隔离验收范围、结果和 conditional-go 说明。

最终 ZIP 不包含 `.env`、SSH 私钥、Telegram session、SQLite 数据库、日志、运行媒体、虚拟环境、Playwright 浏览器缓存、`__pycache__`、编辑器交换文件、历史备份、`qiaolian_dual_original` 或嵌套 ZIP。
