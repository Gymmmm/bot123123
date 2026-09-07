# V3 Phase 1 Report — PR #37 Repair

## Status

Phase 1 implementation is complete for independent review. **Do not merge and do not start Phase 2 until an independent reviewer returns `PASS`.**

## Locked baseline

- Repository: `Gymmmm/bot123123`
- Locked V2.2 SHA: `8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- Phase 0 base branch: `v3/phase0-baseline`
- Phase 0 base SHA: `de7c4b651784e101b6a9bde55239129c79801318`
- Phase 1 branch: `v3/refactor-baseline`
- PR: `#37`

The Phase 0 branch adds only an importable `qiaolian_v3` skeleton, `tests/v3`, `FakeTelegramGateway`, and the baseline manifest. Production runtime code remains the locked V2.2 implementation.

## Phase 1 boundary

This repaired PR is DB foundation only.

Production runtime modules are not wired to V3 and were not changed by the repaired Phase 1 diff:

- `ai_parser.py`
- `collector_bot.py`
- `publication_package.py`
- `publication_delivery.py`
- `autopilot_publish_bot.py`
- `qiaolian_dual/user_bot.py`
- V2 publisher runtime

Phase 1 does **not** write legacy production `listings`. V3 listing identity is isolated in `v3_listings`.

No production files were deleted.

## FIX-01 ～ FIX-13 resolution

### FIX-01 — Restore strict Phase 1 boundary

Resolved. Previous Parser/shadow runtime changes were removed by resetting `v3/refactor-baseline` to the Phase 0 baseline before rebuilding Phase 1. Phase 1 contains DB/migration/repository/test infrastructure only.

### FIX-02 — Never write old production `listings`

Resolved. V3 repositories only write `v3_listings`, `listing_sources`, and `listing_offers`. A real DB non-interference test seeds legacy `listings`, performs a V3 sale repository flow, then proves the legacy rows are byte-for-byte unchanged and no `V3SRC_%` rows exist.

### FIX-03 — Establish `qiaolian_v3/db`

Resolved. Added:

- `qiaolian_v3/db/__init__.py`
- `qiaolian_v3/db/connection.py`
- `qiaolian_v3/db/migration_runner.py`
- `qiaolian_v3/db/unit_of_work.py`
- `qiaolian_v3/db/migrations/`
- `qiaolian_v3/db/repositories/`

### FIX-04 — Real repositories

Resolved. Added source, canonical and listing-offer repositories. They execute real SQLite operations and are tested against the migrated schema.

### FIX-05 — Explicit transaction boundary

Resolved. `UnitOfWork` uses a SAVEPOINT-backed transaction boundary so it composes safely even when the caller already has an SQLite transaction. Exiting without explicit `commit()` rolls V3 work back.

### FIX-06 — Immutable source revisions

Resolved. `source_post_revisions` is append-only:

- same source identity + same `content_hash` => existing revision/no-op;
- same source identity + new hash => next `revision_no`;
- UPDATE is rejected by an immutable trigger;
- DELETE is rejected by an immutable trigger.

### FIX-07 — Stable source identity

Resolved. `source_post_identities` has:

- unique `source_identity_key`;
- unique legacy `source_posts(id)` bridge;
- unique `(source_type, source_name, external_post_id)`.

The key is a deterministic SHA-256 over the source identity tuple.

### FIX-08 — `canonical_records` constraints

Resolved. `canonical_records` binds both `source_identity_id` and immutable `source_post_revision_id`.

DB constraints include:

- `deal_type IN ('rent','sale','unknown')` only;
- `mixed` is rejected by SQLite;
- ambiguity belongs in `deal_type_candidates_json` and `processing_status='needs_review'`;
- one current canonical record per source identity via partial unique index;
- `supersedes_id` lineage;
- trigger rejects a canonical record whose revision belongs to another source identity;
- JSON validity checks and facts hash presence.

### FIX-09 — `listing_offers` constraints

Resolved. DB enforces:

- offer type only `rent | sale`;
- rent requires positive `monthly_rent_usd` and NULL sale price;
- sale requires positive `sale_price_usd` and NULL monthly rent;
- sale must have `publication_policy='store_only'`;
- one current offer per `(listing_id, offer_type)`;
- supersedes lineage.

### FIX-10 — Do not compute final publishability in Phase 1

Resolved. Phase 1 has no `publishable` calculation and no Publisher integration. It persists only a storage/publication policy contract. Sale is DB-forced `store_only`; rent is not automatically made publishable.

### FIX-11 — Migration ledger / runner

Resolved. `MigrationRunner` provides:

- versioned SQL discovery;
- SHA-256 migration checksums;
- `schema_migrations` ledger;
- repeat-run idempotency;
- checksum drift rejection;
- transactional forward migration;
- `PRAGMA foreign_key_check`;
- SAVEPOINT-based `simulate_pending()` that proves forward migrations can execute while rolling all simulated schema changes back.

### FIX-12 — Real repository/schema tests

Resolved. `tests/v3` executes real SQLite schema and repository behavior. It covers:

- migration from a locked-baseline structural fixture;
- migration ledger/idempotency;
- forward migration simulation rollback;
- source identity uniqueness;
- same-hash revision no-op;
- new-hash revision increment;
- immutable revision update/delete rejection;
- `mixed` deal type DB rejection;
- canonical source/revision identity consistency;
- one-current canonical invariant;
- canonical supersedes lineage;
- rent/sale price constraints;
- sale DB store-only enforcement;
- one-current offer invariant;
- offer supersedes lineage;
- publication package immutable snapshot fields;
- channel post idempotency key uniqueness;
- `(channel_id,message_id)` uniqueness;
- Phase 1 non-interference with production runtime;
- no legacy `listings` writes;
- UnitOfWork rollback and explicit commit.

The tests assert database failures and repository state transitions; they do not merely assert constants or repeat implementation strings as a substitute for behavior.

### FIX-13 — Phase 0 baseline before Phase 1

Resolved. Same PR #37 is based on `v3/phase0-baseline` at `de7c4b651784e101b6a9bde55239129c79801318`, which is the locked V2.2 SHA plus the isolated V3 package/test skeleton only.

## Phase 1 schema

Migration: `qiaolian_v3/db/migrations/001_phase1_core.sql`

Added V3-owned tables:

- `schema_migrations` (owned by migration runner)
- `source_post_identities`
- `source_post_revisions`
- `source_post_media`
- `canonical_records`
- `canonical_overrides`
- `v3_listings`
- `listing_sources`
- `listing_offers`
- `review_items`
- `listing_media`
- `v3_publication_packages`
- `v3_channel_posts`

The publication tables are persistence contracts only. There is no Publisher runtime wiring in Phase 1.

## Deal type contract

Formal persisted canonical deal type is exactly:

```text
rent | sale | unknown
```

`mixed` is not valid persisted state. If evidence contains both rent and sale intent but Phase 2+ parsing cannot safely resolve it, the DB representation is `deal_type='unknown'` plus candidate evidence/review state. Phase 1 itself does not modify Parser behavior.

## Sale contract

Phase 1 keeps the correct direction without crossing into publishing logic:

```text
sale offer
→ stored in listing_offers
→ publication_policy = store_only
```

SQLite itself rejects a sale offer with `publication_policy='telegram_rent'`.

No Telegram call exists in this path.

## CI verification

Verified workflow: `qiaolian-ui-check` on head `d689f588ef63f2226d4a223bc0e56d6bf791a086` before this report-only commit.

Run: `34103716222`

Results:

```text
Syntax/runtime import check: PASS
V3 Phase 1 DB contracts: 16 passed in 0.11s
Old production regression suite: 261 passed, 2 warnings in 6.17s
Failures: 0
```

The two warnings are existing `python-telegram-bot` `ConversationHandler` warnings from the production regression suite; no V3 failure is associated with them.

The workflow uses dummy test tokens only.

A final CI run is required on the report commit itself; PR #37 is not review-ready until that final head is green.

## Operational safety

- Did this phase perform any real Telegram mutation? **NO**
- Did this phase operate a production server? **NO**
- Did this phase deploy? **NO**
- Was PR #37 merged? **NO**
- Were Parser / Collector / Publisher / User Bot changed by repaired Phase 1? **NO**
- Were legacy production `listings` written by V3 repositories? **NO**

## Stop condition

After final CI on this report commit is green, Phase 1 stops.

Do not merge.
Do not deploy.
Do not start Phase 2.

Wait for an independent review AI to inspect PR #37 and return exactly the Phase 1 disposition. Only an independent `PASS` permits Phase 2 planning/execution.
