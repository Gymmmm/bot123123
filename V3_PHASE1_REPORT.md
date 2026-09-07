# V3 Phase 1 Report — PR #37 Final DB Contract Alignment

## Status

Phase 1 DB foundation and final persistence schema contracts are complete for independent review.

**Do not merge and do not start Phase 2 until an independent reviewer returns `PASS`.**

## Locked baseline

- Repository: `Gymmmm/bot123123`
- Locked V2.2 SHA: `8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- Phase 0 base branch: `v3/phase0-baseline`
- Phase 0 base SHA: `de7c4b651784e101b6a9bde55239129c79801318`
- Phase 1 branch: `v3/refactor-baseline`
- PR: `#37`

Phase 0 remains the isolated V3 skeleton on top of the locked V2.2 production baseline. Phase 1 remains DB/repository/migration/test infrastructure only.

## Phase 1 boundary

The repaired PR does **not** modify or wire V3 into:

- `ai_parser.py`
- `collector_bot.py`
- `publication_package.py`
- `publication_delivery.py`
- `autopilot_publish_bot.py`
- `qiaolian_dual/user_bot.py`
- V2 publisher runtime

Phase 1 does not deploy, does not call Telegram, does not operate a server, and does not write legacy production business tables.

## FIX-01 ～ FIX-13 status

The previous production-boundary fixes remain in force:

- strict Phase 1-only diff;
- no writes to legacy production `listings`;
- `qiaolian_v3/db` owns V3 schema/migrations/repositories/UoW;
- immutable source revisions;
- real repository/schema tests;
- migration ledger/checksum/idempotency/simulation;
- rent/sale offer separation;
- sale DB-forced store-only;
- formal canonical `deal_type = rent | sale | unknown` only;
- no runtime integration.

This final alignment round adds the remaining locked schema contracts described below.

## 1. Independent V3 SourcePost contract

V3 SourcePost is now independent from legacy `source_posts`.

Table: `v3_source_posts`

Key contract:

- `source_identity_key` unique;
- `source_mode` is exactly `collector | admin_import | migration`;
- `source_type`, `source_name`, `external_post_id` identify the logical source post;
- `source_url`, `source_author`, `dedupe_key`, ingest/parse state are V3-owned;
- `current_revision_id` points to the current immutable V3 source revision;
- `legacy_source_post_id` is nullable and unique but has **no foreign key** to legacy `source_posts`;
- creating a V3 SourcePost does not require any legacy row;
- unique `(source_type, source_name, external_post_id)`.

`SourceRepository.register_source_post()` can create a V3 SourcePost when legacy `source_posts` is empty. The compatibility bridge may be absent or may carry a legacy id without making that id a creation precondition.

## 2. Immutable SourcePostRevision contract

Table: `source_post_revisions`

Formal identity is:

```text
source_post_id + revision_no
source_post_id + source_content_hash
```

Revision evidence includes:

- `raw_text`;
- `sanitized_text`;
- `raw_payload_json`;
- raw image/video/contact/meta snapshots;
- `source_content_hash`.

Behavior:

- same source + same hash => no-op/reuse existing revision;
- changed hash => next revision number;
- UPDATE is rejected by SQLite trigger;
- DELETE is rejected by SQLite trigger;
- the SourcePost current revision pointer must reference a revision belonging to that same SourcePost.

## 3. Canonical lifecycle contract

`canonical_records.processing_status` is now exactly the unified persisted lifecycle:

```text
COLLECTED
PARSING
PARSED
STORED
NEEDS_REVIEW
READY_TO_PUBLISH
PUBLISHING
PUBLISHED
REJECTED
FAILED
```

`superseded` is not a lifecycle status.

Canonical history is represented only by:

- `is_current`;
- `supersedes_id`.

When a new canonical version becomes current, the previous record keeps its original lifecycle status and only changes `is_current` to `0`.

The DB also continues to enforce:

```text
deal_type IN ('rent','sale','unknown')
```

`mixed` is rejected.

Each canonical record binds to an immutable revision of the same V3 SourcePost, and only one current canonical record is allowed per V3 SourcePost.

## 4. Formal Listing contract

Table: `v3_listings`

The formal Listing identity/property projection now contains:

- `public_listing_id` UNIQUE;
- `current_canonical_record_id`;
- `property_identity_key` UNIQUE;
- `project_name`;
- `project_alias`;
- `property_type`;
- `property_subtype`;
- `city_key`;
- `project_key`;
- `canonical_area_key`;
- `public_location_key`;
- `public_location_display`;
- `layout`;
- `bedrooms`;
- `living_rooms`;
- `bathrooms`;
- `helper_rooms`;
- `size_sqm`;
- `floor`;
- `listing_status`;
- timestamps.

`semantic_key` is not a formal persisted Listing field; the formal identity key is `property_identity_key`.

Formal Listing status is exactly:

```text
active | reserved | pending | rented | inactive
```

`closed` and `archived` are rejected as Listing statuses.

Commercial transaction terms remain separated in `listing_offers` rather than being folded back into Listing identity.

## 5. ListingOffer contract

The previous Phase 1 offer contract remains intact:

- offer type is `rent | sale`;
- rent requires positive `monthly_rent_usd` and no sale price;
- sale requires positive `sale_price_usd` and no monthly rent;
- one current offer per `(listing_id, offer_type)`;
- history uses `is_current / supersedes_id`;
- sale must have `publication_policy='store_only'`;
- SQLite rejects sale + `telegram_rent`.

Phase 1 still does not compute final runtime publishability.

## 6. ReviewItem contract

Table: `review_items`

Formal persisted fields now include:

- `review_type`;
- `canonical_record_id` nullable FK;
- `listing_id` nullable FK;
- `offer_id` nullable FK;
- `reason_codes_json` valid JSON;
- `source_mode` exactly `collector | admin_import | migration`;
- `operator_id`;
- `resolution_json` valid JSON;
- timestamps.

At least one canonical/listing/offer subject reference must be present.

Formal review status is exactly:

```text
open | approved | rejected | resolved
```

## 7. Final PublicationPackage persistence contract

Table: `v3_publication_packages`

This is a persistence contract only; Publisher runtime is not wired in Phase 1.

Persisted fields now support the locked later-phase needs without a schema redesign:

- `package_id` UNIQUE;
- `idempotency_key` UNIQUE;
- `listing_id`;
- `offer_id`;
- `canonical_record_id`;
- `package_version > 0`;
- `target_kind='telegram_rent'`;
- `target_channel_id`;
- status `prepared | frozen | publishing | published | superseded | failed`;
- approval mode `auto | admin`;
- `approved_by` / `approved_at`;
- `cover_path` / `cover_hash`;
- `gallery_json` / `gallery_hash`;
- `caption_html`;
- `keyboard_json`;
- `content_hash`;
- `canonical_hash`;
- timestamps.

Unique package version/target contract:

```text
(listing_id, offer_id, package_version, target_kind, target_channel_id)
```

Once a package leaves `prepared`, its frozen content identity/snapshot fields cannot be modified in place.

## 8. Final ChannelPost persistence contract

Table: `v3_channel_posts`

Persisted mapping includes:

- `idempotency_key` UNIQUE;
- `channel_id`;
- `message_id`;
- `listing_id`;
- `offer_id`;
- `current_package_id`;
- `publication_kind='telegram_rent'`;
- `content_hash`;
- `post_status`;
- `last_synced_at`;
- `published_at`;
- `updated_at`.

Formal uniqueness includes:

```text
UNIQUE(channel_id, message_id)
UNIQUE(channel_id, listing_id, publication_kind)
```

SQLite triggers additionally reject a ChannelPost whose current package does not match its listing, offer, target channel, and publication kind.

## 9. Migration contract

Migration remains:

`qiaolian_v3/db/migrations/001_phase1_core.sql`

PR #37 is still draft/unmerged/undeployed, so Phase 1 can finalize this first V3 migration before it becomes an applied production migration.

`MigrationRunner` continues to provide:

- version discovery;
- SHA-256 migration checksum ledger;
- idempotent re-run;
- checksum drift rejection;
- transactional forward migration;
- `PRAGMA foreign_key_check`;
- SAVEPOINT-based forward simulation with rollback.

The migration remains additive relative to locked V2.2 production tables.

## 10. Real SQLite contract tests

`tests/v3` now validates behavior against a real SQLite database rather than tautological constant assertions.

Coverage includes:

- independent V3 SourcePost creation with zero legacy source rows;
- no FK/precondition on nullable legacy source bridge;
- strict source modes;
- immutable revisions and current pointer consistency;
- all ten canonical lifecycle states;
- rejection of `superseded` lifecycle state;
- canonical supersedes lineage preserving prior lifecycle;
- rejection of `mixed` deal type;
- source/revision consistency;
- full Listing field persistence;
- exact five Listing statuses and rejection of `closed/archived`;
- public Listing id uniqueness;
- review references, JSON fields, source mode and status constraints;
- rent/sale offer price and store-only constraints;
- package version/target/approval/idempotency contracts;
- frozen package content immutability;
- ChannelPost message/listing uniqueness;
- ChannelPost current-package consistency;
- V3 source operations do not mutate legacy `source_posts`;
- V3 listing/offer operations do not mutate legacy `listings`;
- UnitOfWork rollback/commit behavior;
- migration ledger/simulation behavior.

## CI verification for final schema/code head

Final schema/code head before this report-only update:

- Head: `bf4f1caccd4cfe40f9d308525c9d555fc04f4270`
- Workflow: `qiaolian-ui-check`
- Run: `34130020806`
- Job: `101767631491`
- Result: **SUCCESS**

Results:

```text
Diff whitespace check: PASS
Syntax/runtime import check: PASS
tests/v3: 36 passed in 0.24s
Old production regression suite: 261 passed, 2 warnings in 6.15s
Failures: 0
```

The two warnings are the existing `python-telegram-bot` `ConversationHandler` warnings in the legacy production regression suite; they are not V3 failures.

The workflow uses dummy test tokens only and performs no outbound production mutation.

A final CI run is required on this report-only commit. No code/schema/runtime behavior is changed by the report commit itself.

## Operational safety

- Real Telegram mutation: **NO**
- Production server operation: **NO**
- Deployment: **NO**
- PR #37 merged: **NO**
- Phase 2 started: **NO**
- Parser changed/wired: **NO**
- Collector changed/wired: **NO**
- Publisher changed/wired: **NO**
- User Bot changed/wired: **NO**
- Legacy `source_posts` mutated by V3 repository flow: **NO**
- Legacy `listings` mutated by V3 repository flow: **NO**

## Stop condition

After the report-only commit's CI is green, Phase 1 stops again.

Do not merge.
Do not deploy.
Do not start Phase 2.

Wait for an independent Phase 1 review of PR #37. Only an independent `PASS` permits Phase 2 planning or execution.
