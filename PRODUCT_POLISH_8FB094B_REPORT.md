# PRODUCT_POLISH_8FB094B_REPORT

## 1. Baseline

- Repository: `Gymmmm/bot123123`
- Base SHA: `8fb094baba9eb7ef350e55b414960708e4d18b38`
- Working branch: `codex/product-polish-8fb094b-20260910`
- Tested implementation SHA before this report-only commit: `7d545cb8f857f026a703a616e2451080ad9e6d64`
- The branch was created directly from the exact production baseline SHA. Final compare shows the merge base remains exactly `8fb094baba9eb7ef350e55b414960708e4d18b38`; no P35, `ccd37b`, `dee937`, or other historical branch was merged or copied over as a replacement.

## 2. Changed

### 2.1 Channel post status and buttons now follow live `inventory_status`

**Problem**

Published channel posts could keep showing `📅 预约看房` even after a listing became pending, rented, inactive, or offline. The reserved status also needed the same human-facing semantics already used by the User Bot.

**Files**

- `v3_core/user_bot/listing_presenter.py`
- `v3_core/publishing/channel_renderer.py`
- `v3_core/publishing/delivery_coordinator.py`
- `v3_core/publishing/telegram_adapter.py`
- `v3_core/user_bot/channel_status_sync.py`
- `v3_core/publishing/publisher_app.py`

**Change**

The existing User Bot inventory status presentation is now the shared source for public status copy and bookability:

- `active` → `🟢 当前可预约`, booking button kept.
- `reserved` → `🟡 已有预约 · 仍可预约`, booking button kept.
- `pending` / unknown → `🔵 房态确认中`, booking button removed.
- `rented` → `🔴 已租出`, booking button removed.
- `inactive` / `offline` → `⚫ 已下架`, booking button removed.

`TelegramSendCommand` carries the live inventory status only so the existing Telegram adapter can decide whether the booking button is valid. Frozen publication facts and the Frozen Publication Package state machine are unchanged.

Existing appointment-driven channel synchronization now updates both caption and keyboard. Publisher-admin status changes also call the same exact-message synchronization path, so an already-published message is edited by its recorded `channel_chat_id` + `message_id`; no Telegram history search or guessed message lookup was introduced.

**Tests**

- New regression matrix for active/reserved/pending/unknown/rented/inactive/offline keyboards.
- Reserved copy regression.
- Existing channel-status synchronization regression updated to the new pending copy.
- Published rented-message regression verifies caption, keyboard, and stored `post_text` are updated together.

### 2.2 `/admin` today appointments accepts legacy date formats

**Problem**

The V3 admin reader could miss real appointments stored as `MM-DD` and incorrectly display `今天暂无预约。`.

**File**

- `v3_core/user_bot/admin_appointments.py`

**Change**

The read query now accepts all three existing formats without rewriting stored data:

- `YYYY-MM-DD`
- `MM-DD`
- `M月D日`

**Tests**

A SQLite-backed regression inserts all three formats for the same day and verifies all are returned.

### 2.3 Advisor notification `source` values are human-readable

**Problem**

Internal slugs such as `hub`, `listing_callback`, and `daily_broadcast` could be shown directly to `ADMIN_IDS`.

**Files**

- `v3_core/user_bot/admin_notification_plans.py`
- `v3_core/user_bot/listing_contact.py`

**Change**

A centralized display-only mapping was added:

- `hub` → `首页联系我们`
- `listing_callback` → `房源咨询`
- `daily_broadcast` → `每日广播咨询`
- `user_search` → `找房咨询`
- `channel` / `channel_deeplink` → `频道房源`
- `search_result` → `找房结果`
- unknown values → `用户咨询`

The original database/domain `source` value is not changed.

**Tests**

Required mappings and unknown fallback are covered. Existing admin-notification tests were updated only where the product requirement intentionally replaces the old slug display.

### 2.4 No-match search page now has `💬 联系我们`

**Problem**

The page said a Chinese advisor could continue watching for a match but only offered `调整条件` and `返回首页`.

**Files**

- `v3_core/user_bot/search_no_match_view.py`
- `v3_core/user_bot/transition_callbacks.py`

**Change**

Added `💬 联系我们`, routed to the existing `v3u:home:contact` flow. No second consultation flow was created.

**Tests**

The no-match view is verified to encode the contact choice into the existing contact callback. Three existing no-match callback regressions were updated to include the new action rather than changing their business flow.

### 2.5 Listing details now show the correct field name

**Problem**

The public listing ID was displayed as `📸 实拍：{public_id}`.

**File**

- `v3_core/user_bot/listing_responses.py`

**Change**

Display-only change:

`🆔 房源编号：{public_id}`

The public ID generation/resolution logic is unchanged.

**Tests**

Regression requires `🆔 房源编号` and explicitly rejects the old `📸 实拍：{public_id}` text.

### 2.6 Admin appointment viewing mode is human-readable

**Problem**

Appointment details could show internal values such as `offline` or `video`.

**File**

- `v3_core/user_bot/admin_appointments.py`

**Change**

The page now reuses the existing `APPOINTMENT_MODE_LABELS` domain mapping:

- `offline` → `实地看房`
- `video` → `实时视频看房`

No duplicate mode mapping was introduced.

**Tests**

Both modes are covered through the actual admin appointment detail callback.

### 2.7 R&F category return path goes back to R&F navigation

**Problem**

A category page used `⬅️ 返回生活服务`, jumping one level too far out.

**File**

- `v3_core/user_bot/service_views.py`

**Change**

- Return label: `⬅️ 返回富力导航`
- Return callback: `v3u:service:rfcity`
- Low-risk title normalization: `R&F City 便民导航` → `富力生活导航`

Merchant body copy was not changed.

**Tests**

The category return label/callback and navigation title are covered.

### 2.8 Admin appointment public listing ID uses an existing real published mapping

**Problem**

An appointment already linked to a published listing could still display `待生成` if the current listing row did not expose its public ID.

**File**

- `v3_core/user_bot/admin_appointments.py`

**Change**

The reader first uses the current listing `public_listing_id`. When that value is missing, it may read the latest actual `published` publication instance's frozen package snapshot and use its already-existing valid public ID. It does not invent or allocate a public ID for historical rows.

**Tests**

One regression verifies a real frozen public ID is recovered; another verifies an empty snapshot remains unresolved rather than fabricating an ID.

### 2.9 `advisor_url` / `CHANNEL_URL` dead-button safety

**Problem checked**

Missing environment URLs must not create clickable-looking dead URL buttons.

**Files reviewed**

- `v3_core/user_bot/home_views.py`
- `v3_core/user_bot/telegram_home_ui.py`
- existing contact callback path

**Change**

No production-code change was needed because the exact baseline already behaves safely:

- non-empty advisor URL → URL button;
- empty advisor URL → existing in-Bot `💬 联系我们` callback;
- non-empty channel URL → `🏠 最新房源` URL button;
- empty channel URL → no `最新房源` URL button.

Regression tests were added to lock both empty-URL cases.

### 2.10 Verification-only GitHub Actions workflow

**File**

- `.github/workflows/product-polish-8fb094b-check.yml`

This workflow is scoped only to `codex/product-polish-8fb094b-20260910`. It performs compile, targeted regressions, complete V3 tests, complete-project pytest, and an exact-baseline full-pytest parity comparison. It contains no deployment, SSH, production-server, database, or Telegram-channel actions.

## 3. Not Changed

The final diff was reviewed against the exact production baseline. The following were not changed:

- Collector: **not modified**.
- Canonical Parser: **not modified**.
- Dedupe main logic: **not modified**.
- Listing identity rules: **not modified**.
- Auto Publish Gate / eligibility thresholds: **not modified**.
- Database core schema: **not modified**.
- Frozen Publication Package design, package freezing rules, package store, approval flow, and delivery state machine: **not modified**.
- No second Collector, Publisher, consultation flow, or publication chain was introduced.
- No production server was modified.
- No production Bot was started/stopped.
- No production channel message was directly edited during this work.
- No database data was deleted or rewritten.
- No merge or production deployment was performed.

The only publication-delivery-layer data-shape change is an additive `inventory_status` value on `TelegramSendCommand`, used solely by the Telegram keyboard renderer to suppress invalid booking buttons.

## 4. Runtime verification required

The following remain **NEEDS_RUNTIME_VERIFY** because GitHub code cannot prove the live production environment/value:

1. **NEEDS_RUNTIME_VERIFY** — whether production `CHANNEL_URL` currently has a value.
2. **NEEDS_RUNTIME_VERIFY** — whether production `advisor_url` / `ADVISOR_URL` currently has a value.
3. **NEEDS_RUNTIME_VERIFY** — whether `/start latest` in the live process is actually returning the latest published inventory in the intended production order.
4. **NEEDS_RUNTIME_VERIFY** — whether anomaly-page `v3edit` is fully registered and reachable in the currently deployed runtime. GitHub code search alone is not treated as live-registration proof.
5. **NEEDS_RUNTIME_VERIFY** — whether the deployed release contains all four assets:
   - `handover.png`
   - `handover.pdf`
   - `deposit.png`
   - `deposit.pdf`
6. **NEEDS_RUNTIME_VERIFY** — whether the current production broadcast setting `button_key` is actually `none`.

The URL fallback behavior itself is covered by code-level regression tests; only the live environment values remain unknown.

## 5. Product decisions deferred

The following were inspected only as scope items and were **not implemented** in this patch:

- Whether `/admin` should restore `新咨询 / 房源 / 服务 / 来源 / 历史`.
- Whether advisor appointment notifications should add confirm/invalid buttons.
- Whether advisor repair notifications should add handling buttons.
- Whether historical appointments should gain a full detail page.
- Whether property coordination should become a structured work-order flow.
- Whether housekeeping should receive an independent flow.
- Whether `停止发布` and `标记已下架` should be merged.
- Whether `repair_power` should get a new menu entry.
- Whether third-party real-estate merchants in the R&F navigation should be removed.

## 6. Tests

GitHub Actions verification:

- Workflow: `product-polish-8fb094b-check`
- Run: `https://github.com/Gymmmm/bot123123/actions/runs/34395955410`
- Tested implementation SHA: `7d545cb8f857f026a703a616e2451080ad9e6d64`
- Overall job result: **SUCCESS**

Results:

- Compile V3 modules/tests/entrypoints: **PASS**.
- Targeted product-polish + Publisher/User Bot regressions: **46 passed**.
- Complete V3 suite: **479 passed, 28 warnings, 0 failed**.
- Production baseline V3 reference before this patch: **453 passed**. Existing tests were not deleted, skipped, or xfailed; complete V3 count increased to 479.
- Complete repository `pytest -q` on patched branch: **collection status 2, 10 collection errors**.
- Complete repository `pytest -q` on exact production baseline `8fb094baba9eb7ef350e55b414960708e4d18b38` in the same GitHub Actions job/environment: **collection status 2, the same 10 collection errors**.
- Full-project baseline parity gate: **PASS** — current and baseline collection-error sets are identical.

The 10 full-repository collection failures are pre-existing legacy/retired-path failures involving modules such as `qiaolian_dual`, `run_pipeline_autopilot`, `scripts.ops`, `qiaolian_pipeline`, and `v2_admin`. They are outside this V3 product-polish scope and were not hidden with skip/xfail or repaired by changing unrelated legacy code.

The GitHub Actions Node.js deprecation warning is non-fatal; the verification job completed successfully.

## 7. Final verdict

No in-scope blocker remains. The V3 suite and targeted product regressions are green, and the repository-wide collection failures were reproduced identically from the exact production baseline.

READY_FOR_REVIEW
