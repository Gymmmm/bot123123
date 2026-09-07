# V3 Phase 1 Report

## 1. 基准与分支

- Repository: `Gymmmm/bot123123`
- 唯一 V2.2 事实基准: `8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- V3 基准分支: `v3/baseline-base`
- V3 工作分支: `v3/refactor-baseline`
- Before SHA: `8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- Implementation head before this report: `97badc7679ba74b768f03448d3c5f59cf7176b72`
- Draft PR: `#37` — `V3 Phase 1: canonical records and offer shadow model`
- PR base 已纠正为 `v3/baseline-base`，不是 master。

说明：当前 master 已经前进到锁定 SHA 之后。Phase 1 的实现判断仍严格以 `8e4605cf...` 为基准，因此本 PR 不应直接按 master 差异解释或直接合并；后续需要单独设计 forward-port/rebase 策略。

---

## 2. 本阶段范围

本阶段只实施：

1. V3 contract tests
2. additive migration/schema
3. `canonical_records` dual-write
4. `listing_offers` dual-write
5. sale 正常保存，但租赁频道 publication hard-block
6. 现有生产回归测试 + V3 tests

未进行 Publisher 全量替换。

---

## 3. 修改/新增文件

### 生产逻辑

- `ai_parser.py`
- `qiaolian_dual/v3_shadow_store.py`（新增）
- `migrations/001_v3_phase1_core.sql`（新增）

### 测试

- `tests/v3/__init__.py`
- `tests/v3/_helpers.py`
- `tests/v3/test_deal_type_routing.py`
- `tests/v3/test_sale_never_published_to_rent_channel.py`
- `tests/v3/test_publish_gate.py`
- `tests/v3/test_source_update_revision.py`
- `tests/v3/test_admin_import_policy.py`
- `tests/v3/test_collector_auto_publish.py`
- `tests/v3/test_package_freeze.py`
- `tests/v3/test_no_csv_runtime_dependency.py`
- `tests/v3/test_parser_authority_boundary.py`

### CI

- `.github/workflows/qiaolian-ui-check.yml`
  - 增加 `v3/refactor-baseline` push 验证
  - 增加 `v3/baseline-base` PR 验证
  - 增加 `python -m pytest -q tests/v3`
  - 原生产 launch gate 测试名单继续保留

曾临时创建 `.github/workflows/v3-phase1-check.yml` 用于验证，但该 workflow 在 GitHub 上未生成 job；现已删除，不作为 Phase 1 交付的一部分。真正的测试证据来自现有 `qiaolian-ui-check`。

---

## 4. Migration / 新数据结构

文件：

`migrations/001_v3_phase1_core.sql`

### source_posts 新增字段

- `revision INTEGER NOT NULL DEFAULT 1`
- `content_hash TEXT NOT NULL DEFAULT ''`
- `ingest_origin TEXT NOT NULL DEFAULT 'collector'`

本阶段只提供 schema 能力；Collector 对“同 source identity、content_hash 改变”的自动 revision/upsert 还未完成，见风险章节。

### canonical_records

新增正式 V3 shadow canonical 表：

- `id`
- `source_post_id`
- `source_revision`
- `schema_version`
- `parser_revision`
- `facts_json`
- `facts_hash`
- `deal_type`
- `quality_score`
- `quality_status`
- `hard_flags_json`
- `review_flags_json`
- `supersedes_id`
- `is_current`
- `created_at`

唯一约束：

`(source_post_id, source_revision, facts_hash)`

### listing_offers

新增：

- `id`
- `listing_id`
- `canonical_record_id`
- `offer_type` (`rent|sale`)
- `currency`
- `monthly_rent_usd`
- `sale_price_usd`
- `original_price_usd`
- `deposit_terms`
- `payment_terms`
- `contract_term`
- `available_date`
- `offer_status`
- `publishable`
- `publish_block_reason`
- timestamps

### review_items

已建立 additive schema，Phase 1 尚未切换旧 drafts review 流。

### listing_media

已建立 additive schema，Phase 1 尚未切换现有 media consumer。

---

## 5. Parser 修改

`ai_parser.py` 仍保留 V2.2 兼容链：

`source_posts -> canonical facts -> drafts`

同时新增 V3 shadow dual-write：

`source_posts -> canonical_records -> shadow listing -> listing_offers`

具体位置：

- `_canonicalize()`：V2 SAFE enrichment 后执行 `normalize_v3_facts()`。
- `_process_single_source_post_with_status()`：canonicalize 后调用 `shadow_write_v3()`。
- `_recanonicalize_pending_drafts()`：recanonicalize 时同步调用 `shadow_write_v3()`。

如果 V3 migration 尚未应用，`shadow_write_v3()` 会返回 `schema_not_applied`，不创建 runtime schema，不影响 V2.2 旧链。

---

## 6. Sale 修改

V2.2 canonical parser 本身已经能解析：

- `deal_type = sale`
- `sale_price_usd`

但旧 `_quality()` 会给所有纯 sale 增加：

`non_rental_source`

导致 Parser 在保存层提前跳过。

Phase 1 新增 `normalize_v3_facts()`，只对合法 sale 移除这个旧 storage-layer blocker，并重新计算 canonical hash。

因此 V3 shadow 行为变为：

`source -> canonical_record -> listing -> sale offer`

sale offer 固定：

- `publishable = 0`
- `publish_block_reason = sale_not_enabled_for_rent_channel`

与此同时现有 `qiaolian_dual.publishability_contract.evaluate_publishability()` 仍保持：

`deal_type != rent -> deal_type_not_rent`

所以 sale 即使已正常入库，仍不能进入租赁 publication path。

本阶段没有修改 Telegram Publisher，也没有给 sale 添加任何发送入口。

---

## 7. Listings 兼容策略

现有 V2.2 `listings` 表仍是 rental-oriented schema，`price` 为非空字段。

Phase 1 为了建立 shadow inventory，没有立刻重做现网 listing schema：

- shadow listing ID 使用稳定格式：`V3SRC_{source_post_id}`
- listing 状态保持 `pending`
- sale 在旧 listing projection 中暂用 `price=0`
- authoritative sale price 只保存在 `listing_offers.sale_price_usd`

`price=0` 不是 V3 最终设计，只是 Phase 1 对旧 schema 的兼容占位；后续必须重做 listings/offer 边界。

---

## 8. Contract Tests

新增 10 个 V3 contract tests，覆盖：

- rent / sale / mixed deal type routing
- sale 正常保存
- sale rent-channel hard block
- publish gate 非 rent 阻断
- source revision schema capability
- admin import 默认不自动发布的策略合同
- collector origin 不能绕过 sale gate
- package canonical hash/media freeze
- Parser/Publication core 不以 `houses.csv` 为 runtime truth
- Parser 不把 caption/package/cover/review 文本作为事实输入

---

## 9. 测试结果

GitHub Actions authoritative run：

- Workflow: `qiaolian-ui-check`
- Run ID: `34073537146`
- Job: `check`
- Tested head: `c3c9e0fc8dfbca5703e25a15ffba84bf6743541d`
- Result: `success`

### Syntax / import

通过：

- `compileall`
- `import qiaolian_dual.user_bot`
- `build_application()`
- V2 publisher import: `PUBLISHER_IMPORT_OK`

### V3 contracts

```text
10 passed in 0.06s
```

### Existing production regression gate

```text
261 passed, 2 warnings in 5.00s
```

Warnings：

- `tests/test_second_batch_runtime_contract.py::test_build_application_constructs`
- `tests/test_callback_route_repair.py::test_build_application_constructs_with_test_token`

均为现有 `python-telegram-bot` ConversationHandler `per_message=False` warning，不是 V3 test failure。

### 失败项

Authoritative CI：

```text
failed: 0
```

---

## 10. Telegram 安全

测试环境使用：

- `USER_BOT_TOKEN=123456:TESTTOKEN`
- `PUBLISHER_BOT_TOKEN=123456:TESTTOKEN`
- temporary SQLite DB

本阶段未运行 production deploy，未调用真实 Telegram send/edit。

**Did this phase perform any real Telegram mutation? NO**

---

## 11. 删除情况

```text
No production files deleted.
```

仅删除了本阶段临时创建、且未正常生成 job 的 `.github/workflows/v3-phase1-check.yml`，它从未属于生产运行代码。

未删除：

- `drafts`
- `meihua_publisher.py`
- `autopilot_publish_bot.py`
- 旧 Publisher
- CSV/Excel 工具
- `qiaolian_dual`
- User Bot

也未修改频道三按钮合同或频道排版。

---

## 12. 当前兼容路径

Phase 1 完成后的双轨状态：

```text
V2.2 compatibility:
source_posts
  -> canonical facts
  -> drafts
  -> existing Publisher

V3 shadow:
source_posts
  -> canonical_records
  -> pending shadow listing
  -> listing_offers
```

V3 目前尚未成为 Publisher read source。

这符合“先 dual-write / shadow，后切读”的迁移策略。

---

## 13. 当前风险 / 未完成项

### R1 — source update revision 还没有接进 Collector

虽然 migration 已提供：

- `revision`
- `content_hash`
- `ingest_origin`

但当前 `collector_bot._find_existing_post()` 仍优先按 source tuple 直接判 duplicate。

因此：

`same source identity + changed content`

目前还不会自动：

`revision + 1 -> reparse`

这是下一小阶段最优先补齐项。

### R2 — sale boundary 仍有临时 adapter

官方 `canonical_facts.py` 仍会生成 `non_rental_source`。

Phase 1 是在 canonical parser 后通过 `normalize_v3_facts()` 去掉该旧 blocker。

最终应把正确规则正式吸收到 canonical quality 本体：

- sale 是合法 real-estate facts
- publish eligibility 由 publication gate 决定

之后删除这个 normalization shim。

### R3 — legacy listings 仍是 rental schema

sale shadow listing 的 `price=0` 只是兼容占位。

下一阶段应正式重做：

`listings = property identity`

`listing_offers = transaction terms`

### R4 — admin/collector policy 目前仍是 contract-level

Phase 1 已锁定目标行为，但尚未建立统一 `AutoPublishPolicy` service。

不能把当前测试理解为“Collector/Admin 全部入口已经完成统一 policy 重构”。

### R5 — migration 需要正式版本管理

`001_v3_phase1_core.sql` 是一次性 additive migration。

不要人工重复执行同一 ALTER migration。

下一阶段应明确 migration ledger/version runner，使 schema ownership 真正唯一。

### R6 — master 已偏离锁定生产 SHA

V3 基准必须继续使用 `v3/baseline-base / 8e4605cf...` 判断 V2.2 行为。

后续真正合入新的主开发线前，需要专门做 forward-port 差异审计，不能通过 master 反向改写本 V3 基线事实。

---

## 14. 下一 Phase 建议

在 Publisher 全量替换之前，建议先做 **Phase 1.1 / Foundation hardening**：

1. Collector 实现 source identity + content hash revision/upsert。
2. 给 source revision 做真实 Collector integration tests。
3. 建 migration ledger/runner，停止依赖人工 SQL 执行。
4. 把 sale 合法性规则正式移入 canonical parser quality，去掉临时 normalization shim。
5. 建正式 canonical/listing/offer repositories，减少 `collector_db_compat.py` 依赖。
6. 补 mixed -> two offers / needs_review 的完整 materialization contract。
7. 再进入 Publisher Phase：统一 PublishGate / package / approval / delivery。

---

## 15. Phase 1 验收结论

已完成并验证：

```text
source_posts
  -> canonical_records
  -> listings (shadow/pending compatibility projection)
  -> listing_offers
```

Rent：

```text
rent source
-> canonical
-> rent offer
```

Sale：

```text
sale source
-> canonical
-> sale offer
-> publishable=false
-> rent publish gate blocked
```

Mixed：

canonical parser 保持 `mixed`，shadow writer 可以在金额均明确时生成 rent/sale 两个 offers；rent offer不会因 mixed 自动成为 publishable，仍保守阻断。

现有生产回归门禁保持全绿。

**Phase 1 可以作为 V3 shadow 数据模型的可继续开发基线，但尚不应部署为新的生产 Publisher 主链。**
