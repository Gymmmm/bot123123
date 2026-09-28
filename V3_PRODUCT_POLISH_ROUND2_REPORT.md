# V3 Product Polish Round 2 — Final Review Report

## Verdict

**READY_FOR_REVIEW**

This branch is a product-flow polish on top of the stable V3 baseline. It has passed compile and the complete `tests/v3_core` suite. It has **not** been merged or deployed.

## Baseline and branch

- Baseline: `5b834f65140ad890b83dacee08e8e8e71836f2bc`
- Branch: `codex/v3-product-polish-round2-5b834f6-20260911`
- Validated code/test SHA: `bf941bb7606f102abdfd4a9a730af75037f96b10`
- GitHub Actions run: `34518523314`
- Workflow job: `103009937190`

The temporary Round 2 push trigger added to `.github/workflows/v3-core-extraction-check.yml` was removed after validation. The workflow file is therefore unchanged from baseline in the final diff.

## Validation result

GitHub Actions on `bf941bb7606f102abdfd4a9a730af75037f96b10`:

- `python -m compileall ...`: **PASS**
- `python -m pytest -q tests/v3_core`: **PASS**
- Tests: **482 passed**
- Failures: **0**
- Warnings: **28**
- Test time: **18.19s**

The warnings are the existing Pillow `Image.Image.getdata` deprecation warnings in media/publisher pipeline tests; no Round 2 test failure is hidden, skipped, or xfailed.

## Product changes in this round

### 1. Listing and contact navigation

- Standardized the active V3 listing/search surface around `租赁详情`, `更多实拍`, `预约看房`, and `联系中文顾问`.
- A configured advisor URL is used as a direct Telegram handoff.
- Listing consultation keeps the current lead/admin-notification effects and carries the public listing context.
- Empty advisor configuration falls back to the existing internal contact flow instead of rendering a dead URL button.
- Relevant listing/start surfaces add useful return paths without reintroducing legacy callback namespaces.

### 2. Appointment customer flow

- Appointment date presentation remains compatible with the existing V3 date formats while showing human-readable dates.
- Appointment success clearly shows the pending-confirmation state and offers `查看我的预约`.
- Existing public listing IDs remain the only listing identity exposed to customers.
- Existing V3 appointment edit/cancel services remain the source of truth; no parallel appointment model was introduced.

### 3. Appointment administrator flow

- New appointment notifications can carry operational actions:
  - confirm appointment
  - mark contacted
  - unable to arrange/cancel
  - contact customer
  - view appointment
- `/admin` has a compact pending-appointment entry/count in addition to today’s appointments.
- Administrator status changes are monotonic: a confirmed appointment cannot accidentally be downgraded to contacted.
- Confirm/cancel can notify the customer through Telegram and expose useful next actions.
- Status changes reuse the existing availability recompute/channel synchronization boundaries.
- Admin public listing display first uses the real `listings_v3.public_listing_id`; when necessary it can read the published Frozen Package snapshot. It never fabricates or backfills a public ID.
- Today’s appointment matching keeps compatibility with `YYYY-MM-DD`, `MM-DD`, and `M月D日`.

### 4. Existing tenant / old-customer service

- Added a real `已入住 / 老租客服务` entry to the active V3 service surface.
- It reuses `tenant_bindings_v3` and `SQLiteTenantServiceRepository.get_active_binding()`.
- A bound customer sees the current property plus repair/property actions.
- An unbound customer gets an explicit advisor/contact path rather than a fake aftercare workflow.
- PR #49’s `rental_cases_v3` / operations schema was **not** imported.

### 5. Assurance material

- Existing handover/deposit assets are reused.
- One customer click sends the guidance text, preview PNG, and complete PDF bundle, avoiding duplicate intermediate decisions.
- Repository verification confirms all four source assets exist:
  - `assets/v2_2/generated/handover.png`
  - `assets/v2_2/generated/handover.pdf`
  - `assets/v2_2/generated/deposit.png`
  - `assets/v2_2/generated/deposit.pdf`

### 6. Publisher manual inventory status → original channel post

- Added `PublisherManualStatusSynchronizer` for explicit Publisher admin status changes.
- It resolves the exact existing Telegram post only from stored `publication_instances.channel_chat_id` and `channel_message_id`.
- It does **not** search Telegram history and does **not** create a replacement post.
- It edits the existing caption to the chosen inventory status and rebuilds the existing public action keyboard.
- A non-bookable status therefore removes `预约看房` from that post through the shared status-aware keyboard rule.
- The edited caption is persisted back to `publication_instances.post_text` after Telegram edit succeeds.
- Telegram edit failure is best-effort and does not roll back the already selected inventory status; the Publisher shows a synchronization warning instead.

## Reuse / architecture boundary

Round 2 deliberately reuses the current V3 components documented in `PRODUCT_POLISH_ROUND2_REUSE_MAP.md`.

Not changed by this round:

- Collector architecture
- Canonical Parser/materializer rules
- dedupe logic
- listing identity rules
- sale hard-isolation policy
- Auto Publish Gate
- Frozen Publication Package design
- durable delivery state machine
- core DB schema
- existing Telegram retry/unknown-state policy

No historical product-polish branch or PR #49 was merged/cherry-picked wholesale.

## Focused Round 2 regression coverage

`tests/v3_core/test_product_polish_round2.py` explicitly covers:

1. administrator appointment operational buttons and customer contact target;
2. existing-tenant entry, bound state, and unbound state;
3. monotonic appointment status transitions;
4. channel caption inventory-status replacement without duplicate public IDs;
5. exact original-post edit using stored Telegram identity, including booking-button removal for a rented listing.

Existing contract tests were changed only where the intended visible product surface changed: the `返回租赁详情` label, the new pending-admin entry, and the new tenant-service entry.

## Runtime verification still required before production deployment

The code/contract suite is green, but these depend on the real Telegram/runtime environment and should be checked during staging or deployment verification:

- configured `ADVISOR_TG` / support URL opens the intended real advisor chat;
- handover/deposit PNG and PDF files are present in the deployed release path, not only in Git;
- Publisher bot has Telegram permission to edit the original channel post;
- stored `publication_instances` chat/message IDs correspond to the actual production channel posts;
- confirmed/cancelled appointment notification can be delivered to a real Telegram customer;
- `CHANNEL_URL` and advisor-empty fallbacks render correctly with the production environment values.

## Production safety statement

During this Round 2 implementation and validation:

- no production deployment was performed;
- no production service was restarted;
- no production channel post was edited;
- no production database was mutated;
- no merge to the production branch was performed.

## Final status

`READY_FOR_REVIEW`

The validated executable source state is `bf941bb7606f102abdfd4a9a730af75037f96b10`; commits after it only restore the temporary CI trigger and add this report. A production/staging runtime verification should occur before merge/deployment.
