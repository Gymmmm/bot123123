# 数据库与字段流

> r3 补充：`schema_core.sql` 现在直接创建 `listings`；`ensure_canonical_projection_schema()` 也会幂等创建该表。因此 collector 冷启动后立即 materialize 不再依赖用户 Bot 先运行。`persist_source_post()` 默认只写 `source_posts(parse_status=pending)`，真实 Telegram handler 通过 `classify_after_insert=True` 显式继续 parser/package 链。

本文按实际入口函数、调用关系和 SQL 字段还原运行链路，不按文件名推测。

## 生产入口

- 采集服务：`deploy/systemd/qiaolian-collector.service` → `collector_bot.py:main()` → `main_async()`。
- 管理/发布服务：`deploy/systemd/qiaolian-publisher-bot.service` → `v2/run_publisher_bot_v2.py` → `v2/qiaolian_publisher_v2/bot.py:main()` → `autopilot_publish_bot.register_autopilot_features()`。
- 用户服务：`deploy/systemd/qiaolian-user-bot.service` → `run_user_bot.py` → `qiaolian_dual.app:main()` → `build_application()`。
- 三个服务的工作目录为 `/opt/qiaolian_dual_bots`，解释器为 `/opt/qiaolian_dual_bots/.venv/bin/python`，配置来自 `/opt/qiaolian_dual_bots/.env`。

## 真实端到端链路

### 1. source_post：采集与媒体归属

入口：

- 单条消息：`collector_bot.handle_single_message()`
- Telegram 相册：`collector_bot.handle_album()`
- 统一保存：`collector_bot.persist_source_post()`

写入 `source_posts`：

`source_type, source_name, source_post_id, source_url, source_author, raw_text, raw_images_json, raw_videos_json, raw_contact, raw_meta_json, dedupe_hash, parse_status`

写入 `media_assets`：

`asset_id, owner_type='source_post', owner_ref_id, owner_ref_key, local_path, file_hash, telegram_file_id, telegram_file_unique_id, sort_order`

`raw_meta_json.sanitized_text` 是 parser 的文字输入；原文只用于审计/hash，不进入公开事实。唯一键 `(source_type, source_name, source_post_id)` 和 `dedupe_hash` 保证重复采集幂等。相册图片共享同一 source_post owner，并按 `sort_order` 固定顺序。

### 2. parser：过滤与 Canonical 入口

调用：

`collector_bot._classify_and_package()` → `AIParserModule.process_single_source_post()` → `_process_single_source_post_with_status()` → `AIParserModule._canonicalize()`

读取一条 `source_posts` 和其 owner 对应的 `media_assets`。非出租写 `skipped_non_rental`，缺价格写 `skipped_no_price`，成功写 `parsed`。Parser 不从公开 caption 反向猜事实。

### 3. canonical facts：唯一业务事实

实现：

`qiaolian_dual.canonical_facts.canonicalize_source()` → `qiaolian_dual.listing_taxonomy.classify_listing_taxonomy()` → `public_location_from_fields()`

Canonical JSON 核心字段：

- 来源：`schema_version, source_identity, raw_text_sha256, sanitized_text_sha256`
- 地理：`city_key/display, canonical_area_key/display/level, area_status, market_location_keys/displays, public_location_key/display, publication_location_level`
- 项目：`project_name, project_alias, project_key, project_brand, project_brand_key, community_name`
- 物业：`property_type, property_subtype, property_type_display/status`
- 户型：`layout, bedrooms, living_rooms, bathrooms, helper_rooms`
- 租金：`deal_type, monthly_rent_usd, original_monthly_rent_usd, price_status`
- 条件：`deposit_months, prepay_months, deposit_payment_terms, contract_term_months/display, available_date`
- 费用：`management_fee, internet_fee, water_rate, electric_rate, parking_fee`
- 看房：`viewing_time, video_viewing`
- 其他：`size_sqm, floor, highlights, media_summary, evidence, quality, canonical_facts_hash`

规则：

- `listing_taxonomy.py` 是 location/project/type 的唯一 catalog；`area_normalization.py`、`location_mapping.py`、搜索按钮和管理工具只从它派生。
- `city_display='金边'` 不允许填入 `canonical_area_key` 或 `public_location_key`；`金边市区` 不是合法 area。
- `canonical_area_*` 只保存带“位置/区域/地址/地段”等证据的 Level 2 物理区域。
- 市场位置、项目群、商圈、走廊只进入 Level 1 `market_location_*`；项目、别名、品牌、物业类型和行政区域分别保存。
- 仅在全文出现已知地名但没有精确位置标签时，可保守保存为 Level 1，不能提升为物理地址；同优先级候选无法消歧时加入 `ambiguous_market_location` 并由 gate 阻断。
- 公共位置按 Level 2 物理区域 → Level 1 市场位置 → Level 1 已确认项目选择唯一值。不会把两个候选用 `/` 拼为一个值；`太子/幸福` 只作为旧输入别名，公共显示统一为 `太子幸福广场`。
- 原价/现价只接受显式标签。两个未标注租金不会自动认定为优惠价。
- 交易类型识别前剥离“不出售、非出售、仅出租不售、not for sale”等否定短语；真实租售混合仍为阻断项。
- 费用、入住、看房时间只接受明确标签与有效值；`___`、方括号模板和“待确认”不成为公开事实。

人工 area 修正入口为 `qiaolian_dual.area_admin.set_canonical_area()`：先用同一 taxonomy 解析管理员输入，再同步 canonical/public location、publication level、状态、普通 draft/listing 投影，重算 `canonical_facts_hash`，重新执行 `validate_facts()` 并写审计。approved/published package 禁止修改；旧工具造成的 hash mismatch 只有在移除可证明的遗留 `area`/`normalized_area` 键后能还原原 hash 时才允许恢复。

### 4. draft：Canonical 的审核投影

入口：

`qiaolian_dual.canonical_facts.draft_projection()` → `AIParserModule._create_draft()` / `_write_existing_draft()` → `materialize_draft_facts()`

写入 `drafts`：

`draft_id, source_post_id, title, project, community, area, property_type, price, layout, size, floor, deposit, available_date, highlights, cost_notes, extracted_data, normalized_data, review_status, queue_score, review_note, canonical_facts_hash, canonical_facts_schema, public_location_key/display, publication_location_level, canonical_area_key, property_subtype, project_brand`

`extracted_data` 与 `normalized_data` 保存同一 Canonical JSON；普通列只是审核/UI 投影。`drafts.area` 保存安全的 `public_location_display`，但精度必须看 `publication_location_level`，不能单凭该列推断物理地址。重新解析时，空的可选值不会覆盖已有有效值。

### 5. listing：公开查询模型

入口：

`publication_package.build_package()` → `qiaolian_dual.canonical_listing_materializer.materialize_listing()` → `listing_projection()`

写入/更新 `listings`：

`listing_id, title, property_type, area, community, price, currency, layout, size_sqm, tags_json, highlights, hidden_costs, deposit_rule, available_date, source_post_url, status, canonical_facts_hash/schema, public_location_key/display, publication_location_level, canonical_area_key, property_subtype, project_brand`

只有 `package_gate()` 通过才物化。gate 严格检查物理区域 catalog/display、市场候选、public key/display 配对、location level、project/type 分离和 Canonical hash。UPSERT 对面积、亮点、费用、押付和入住时间使用 `COALESCE(NULLIF(new,''), existing)`，空值不能覆盖已有有效值。`materialize_draft_facts()` 直接复用 Canonical `public_location_key`，不再第二次推导而遗漏 Level 2 区域。

### 6. publication package：冻结发布事实

入口：

- 构建：`publication_package.build_package(db_path, draft_id, caption_variant_override=...)`
- 封面预览：`render_cover_preview()`
- 审核：`approve_package()`

读取 draft Canonical JSON、同一 source_post 的媒体 asset ID/path/hash。写入 `publication_packages`：

`package_id, draft_id, property_id, package_version, source_type, listing_type, media_type, cover_template, status, cover_path, main_images_json, discussion_images_json, post_text, discussion_text, snapshot_json, content_hash, source_identity_json/hash, public_token, canonical_facts_hash/schema, publication_location_level, approved_by/at, published_at`

`snapshot_json.canonical_facts` 固化位置、项目、物业类型、现价、原价、户型、楼层、面积、押付、租期、入住、费用、看房字段和媒体 hash。`caption_variant` 同时冻结在 snapshot 中。`status IN ('approved','published')` 后拒绝重建或 supersede。

默认文案只读取 Canonical `property_type`：

- A：公寓/住宅/未知
- B：别墅/排屋/联排/双拼/整栋
- C：商铺/办公/商业/仓库/厂房/土地

审核前管理员可显式覆盖 A/B/C；审核后发布调用、定时器和旧 `review_note` 都必须服从 snapshot。

### 7. cover 与 caption：同一 package 的两个视图

封面：

`build_package()` → `_render_cover()` → `html_cover_renderer.render_html_cover()` → `templates/*.html` → `cover.png`

封面读取 Canonical 的项目、项目别名、物业类型、公开区域、户型、面积、楼层、交易类型、价格和 highlights，并只使用 package 固定的 source_post 图片。路由：普通 `minimal_white`、信息丰富/微信 `right_price`、别墅 `villa_premium`、高价 `black_gold`、特价 `classic_blue`、视频 `video_vertical`。只有 `deal_type='rent'` 带 `/月`；sale 使用售价语义，不复用月租后缀。

详情图不是另一套事实解释器：`prepare_channel_photo_for_publish()` 只读取冻结图片，调用 Pillow 绘制自适应安静角落的磨砂品牌角标。它不写数据库、不改变图片比例，也不把价格、面积或区域重新画到详情图；无 CJK 字体时使用英文品牌回退。

正文与评论：

- 正文：`meihua_publisher.build_chinese_listing_post()` / `build_channel_caption()`
- 评论：`build_discussion_detail_text()`

A/B/C 只改变排版顺序，不改变事实。Telegram HTML 字体标签由 HTML escape 后的 Canonical 字段组成；原价删除线只在显式原价与现价不同才出现。评论详情按“租赁 / 费用 / 配套 / 看房”输出并过滤占位、联系人、用户名和来源词。

### 8. publish：正式发送与幂等提交

管理员入口：

- `/pending`：列出待审并按物业类型选择默认版。
- 预览按钮：A/B/C、重做封面、拒绝。
- `/approve QC编号 [A/B/C]`：构建并冻结 package，不发送。
- `/send`：列出已审核 package；选择后调用 `MeihuaPublisher.publish_draft()`。

`publish_draft()` 首先读取 `approved_package()` 和 snapshot，验证 Canonical hash、source identity、媒体路径/hash 和 frozen caption，再调用 Telegram sender。

`publication_delivery.PublicationDeliveryRepository` 写 `publication_delivery_attempts`：

`prepared → sending → sent → committed`

同一 `(package_id, channel_chat_id)` 唯一。发送结果不确定进入 `unknown` 并禁止自动重试；有 durable receipt 时只做本地 `commit_saved_result()`，不重复发 Telegram。成功提交后更新：

- `drafts.review_status='published'`
- `publication_packages.status='published'`
- `listings.status='active'`
- `publication_delivery_attempts.state='committed'`

### 9. user bot：只读 active listing

- `/start`：`qiaolian_dual.start_routes.start()` / `route_start_arg()`
- start payload：`qiaolian_dual.session_deeplink.parse_start_arg_payload()`
- 首页、分类、分页、详情：`handle_ui_callback()` → `callback_*` → `search_listings_with_fallback()` → `send_listing_card()`；地区显示和搜索别名从 taxonomy 派生 catalog 读取。
- 咨询：listing callback 先保存 active entry/session，再写 `leads`
- 预约：`flows.start_appointment()` → `appointment_flow.appoint_flow_cb()` → `appointments` / `leads`

用户 Bot 只显示 `listings.status='active'` 且通过公开状态检查的记录；缺 pipeline 表时返回空集合，不放宽查询。

## 核心表

- `source_posts`：来源身份、原文、清洗 meta、dedupe、解析状态。
- `media_assets`：文件身份、owner、hash、顺序和 Telegram file IDs。
- `drafts`：审核投影、Canonical JSON/hash、审核/发布时间。
- `listings`：用户 Bot 的 active 查询投影。
- `publication_packages`：冻结封面、媒体、正文、评论、source identity、public token 和 snapshot。
- `publication_delivery_attempts`：外部发送状态与 durable Telegram result。
- 用户业务：`users, favorites, leads, appointments, tenant_bindings, repair_tickets, subscriptions`。

## 不变量

1. `draft.canonical_facts_hash = listing.canonical_facts_hash = package.canonical_facts_hash`。
2. package 全部媒体必须属于同一 `source_post_db_id`，且冻结文件 hash 与发送前一致。
3. approved/published package 的 `snapshot_json/content_hash/source_identity_hash/caption_variant` 不变。
4. A/B/C 只改变表现层，不能重新解释城市、区域、项目、类型、价格或费用。
5. 同一 `(package_id, channel_chat_id)` 只有一个 delivery attempt。
6. `sending/unknown` 不自动重试；`sent` 只做本地恢复提交。
7. 用户公开查询只读 active listing；联系方式、来源标记和占位字段不能进入公开投影。
8. `canonical_area_*` 永远代表 Level 2 物理区域；Level 1 市场/项目只能进入 `public_location_*`，城市不能进入两者。
9. 详情图水印只改变图片表现，不创建或修改任何房源事实。
