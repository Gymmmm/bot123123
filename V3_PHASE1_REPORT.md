# V3 Phase 1 Report — PR #37 Final Source DB Closure

## Status

Phase 1 DB foundation is complete for independent review.

**Do not merge and do not start Phase 2 until an independent reviewer returns `PASS`.**

## Locked baseline

- Repository: `Gymmmm/bot123123`
- Locked V2.2 SHA: `8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- Phase 0 base branch: `v3/phase0-baseline`
- Phase 0 base SHA: `de7c4b651784e101b6a9bde55239129c79801318`
- Phase 1 branch: `v3/refactor-baseline`
- PR: `#37`

Phase 1 remains migration/schema/repository/test infrastructure only. It is not wired into production runtime.

## Phase 1 boundary

This final Source DB closure did not modify or wire V3 into:

- Parser / `ai_parser.py`
- Collector / `collector_bot.py`
- Publisher
- User Bot
- V2 publisher runtime

It did not write legacy production business tables.

No merge, deployment, production server operation, or Telegram mutation occurred.

## Previous Phase 1 contracts remain unchanged

The already-reviewed Phase 1 contracts remain in force and were not expanded in this closure:

- `qiaolian_v3/db` is the V3 schema owner;
- migration ledger / checksum / forward simulation / UoW remain intact;
- immutable source revisions remain intact;
- `canonical_records` lifecycle and `deal_type = rent | sale | unknown` remain unchanged;
- Listing formal fields/status contract remains unchanged;
- ListingOffer rent/sale separation and sale store-only remain unchanged;
- ReviewItem contract remains unchanged;
- PublicationPackage persistence contract remains unchanged;
- ChannelPost persistence contract remains unchanged;
- no V3 repository writes legacy production `listings`.

From previous final head `3978a8de1a15ef44947fbf858eb69306f28e9765` to the final Source schema/code head, only these files changed:

- `qiaolian_v3/db/migrations/001_phase1_core.sql`
- `qiaolian_v3/db/repositories/sources.py`
- `tests/v3/db/test_source_repository.py`

## 1. V3 Source registry

A V3-owned source registry now exists:

`v3_sources`

Its responsibility is the V3 `Source` domain object described by `V3_EXTRACTION_PLAN.md`:

- `source_type`
- `source_name`
- `external_identity`
- `enabled`
- `collector_config_json`
- timestamps

Uniqueness:

```text
UNIQUE(source_type, external_identity)
```

`SourceRepository.register_source()` and `get_source()` provide the Phase 1 repository interface.

## 2. SourcePost source ownership / FK

`v3_source_posts` now has:

```text
source_id
```

with a formal V3 FK:

```text
v3_source_posts.source_id
→ v3_sources.id
ON DELETE RESTRICT
```

`source_id` is NOT NULL.

A SourcePost can no longer exist in the V3 schema without a V3 Source owner.

The nullable compatibility bridge remains:

```text
legacy_source_post_id
```

It is still **not** a foreign key to legacy `source_posts` and is not a creation precondition.

## 3. SourcePost minimum field contract

`v3_source_posts` now includes the full minimum SourcePost contract:

```text
id
source_id
source_mode
source_type
source_name
external_post_id
source_identity_key
current_revision_id
first_seen_at
last_seen_at
status
```

The existing fields used for evidence routing/compatibility remain available.

Formal source mode is unchanged and DB-enforced:

```text
collector | admin_import | migration
```

`source_identity_key` remains unique.

`current_revision_id` remains the pointer to the current immutable revision.

### first_seen_at / last_seen_at behavior

Repository behavior is now explicit:

- first registration stores `first_seen_at`;
- re-observing the same SourcePost keeps the original `first_seen_at`;
- re-observing updates `last_seen_at`;
- SourcePost identity remains stable;
- SourcePost `status` is persisted and can be updated on re-observation.

## 4. SourcePostRevision time contract

`source_post_revisions` now includes:

```text
source_created_at
fetched_at
```

The formal minimum revision contract is therefore:

```text
id
source_post_id
revision_no
raw_text
sanitized_text
raw_payload_json
source_content_hash
source_created_at
fetched_at
created_at
```

plus the existing raw media/contact/meta snapshot fields.

Uniqueness remains:

```text
UNIQUE(source_post_id, revision_no)
UNIQUE(source_post_id, source_content_hash)
```

Revision immutability remains enforced by SQLite UPDATE/DELETE triggers.

### revision timestamp behavior

`SourceRepository.append_revision()` now accepts and persists:

- `source_created_at`
- `fetched_at`

Same source + same content hash remains a no-op/reuse of the original immutable revision, including the original timestamp values.

A new content hash creates the next revision and advances `current_revision_id`.

## 5. Real SQLite tests added

The Source DB tests now verify actual SQLite behavior, including:

- V3 Source registry persistence;
- `v3_source_posts.source_id` FK points to `v3_sources`;
- invalid/missing Source FK is rejected;
- deleting a Source referenced by a SourcePost is restricted;
- SourcePost creation remains independent from legacy `source_posts`;
- nullable legacy bridge remains optional;
- `source_mode` remains exactly `collector | admin_import | migration`;
- stable SourcePost identity;
- first seen value is preserved;
- last seen value advances on re-observation;
- SourcePost status persists/updates;
- `source_created_at` persists on immutable revisions;
- `fetched_at` persists on immutable revisions;
- same-hash no-op preserves original revision timestamps;
- new hash creates the next revision;
- current revision pointer advances;
- revision UPDATE/DELETE remain rejected.

These tests operate on a real migrated in-memory SQLite database and exercise FK/constraint/repository behavior.

## 6. Migration status

The migration remains:

`qiaolian_v3/db/migrations/001_phase1_core.sql`

PR #37 is still Draft / unmerged / undeployed, so this initial Phase 1 migration is being finalized before any production application.

The migration remains additive relative to the locked V2.2 production schema and does not alter legacy production business tables.

## 7. Final Source schema/code CI evidence

Final Source schema/code head before this report-only update:

- Head: `8019c6a7300074c73396683e83808b7ce49e7a51`
- Workflow: `qiaolian-ui-check`
- Run: `34143642566`
- Job: `101810855739`
- Result: **SUCCESS**

Results:

```text
Diff whitespace check: PASS
Syntax/runtime import check: PASS
tests/v3: 38 passed in 0.28s
Old production regression suite: 261 passed, 2 warnings in 6.33s
Failures: 0
```

The two warnings are the pre-existing `python-telegram-bot` `ConversationHandler` warnings in the old production regression suite and are not V3 failures.

The workflow uses dummy test tokens and performs no production Telegram mutation.

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
- Legacy production business tables written by this closure: **NO**

## Stop condition

Run CI once on this report-only commit. If it is green, Phase 1 stops.

Do not merge.
Do not deploy.
Do not start Phase 2.

Wait for an independent Phase 1 review of PR #37. Only an independent `PASS` permits Phase 2 planning or execution.
