# PHASE_2_RESULT.md

## Baseline

- Repository: `Gymmmm/bot123123`
- Locked V2.2 production source baseline: `8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- Phase 1 PASS head / Phase 2 parent: `cee9e567d7681c1a81f900922031c3c7ab5bbede`
- Phase 2 branch: `v3/phase2-ingest`
- Phase 2 PR: `#42`
- Phase 2 must remain Draft/unmerged until independent review returns `PASS`.

## Scope

Phase 2 implements only Collector / Source Revision / Ingest Dedupe extraction.

Allowed implementation scope used:

- `qiaolian_v3/ingest/**`
- `qiaolian_v3/media/source_media.py`
- ingest-only DB repository additions
- `tests/v3/ingest/**`
- this result report

Explicitly not modified or wired:

- Parser / Canonical behavior
- Publisher / publication runtime
- User Bot
- Admin Bot
- legacy production source files
- production services
- Telegram write paths

## Files changed

Relative to Phase 1 PASS head, Phase 2 adds/changes only:

- `qiaolian_v3/ingest/__init__.py`
- `qiaolian_v3/ingest/sanitizer.py`
- `qiaolian_v3/ingest/source_identity.py`
- `qiaolian_v3/ingest/source_service.py`
- `qiaolian_v3/ingest/telegram_collector.py`
- `qiaolian_v3/media/source_media.py`
- `qiaolian_v3/db/repositories/source_media.py`
- `qiaolian_v3/db/repositories/sources.py`
- `tests/v3/ingest/__init__.py`
- `tests/v3/ingest/test_source_identity_and_dedupe.py`
- `tests/v3/ingest/test_sanitizer_and_media.py`
- `tests/v3/ingest/test_telegram_collector_boundary.py`
- `PHASE_2_RESULT.md`

## Functions / contracts changed

### Source identity

- `build_single_external_post_id(message_id)`
- `build_album_external_post_id(grouped_id, anchor_message_id)`
- `make_source_content_hash(sanitized_text, media_hashes)`

Single-message identity remains the Telegram message id.
Album identity follows the locked V2.2 rule: `album_{grouped_id}` with `album_{anchor_message_id}` fallback.

Source content hash is computed from sanitized fact text plus normalized media content hashes, not fetch timestamps or removed source contacts.

### Sanitizer

The verified behavior from locked V2.2 `source_sanitizer.py` is extracted into `qiaolian_v3/ingest/sanitizer.py`:

- strip Unicode formatting/private-use noise;
- isolate URL/handle/phone/contact lines;
- remove source promotion text;
- preserve factual prefix when contact text is appended to a fact line;
- keep source contacts outside canonical input text.

### Source media

`SourceMedia` provides stable SHA-256 media identity from bytes/files and retains ingest evidence metadata such as sort order, Telegram file ids and message id.

Phase 2 does not rank photos, select covers, render media, or decide listing quality.

### Ingest repository

Phase 1 SourceRepository contracts remain authoritative. Phase 2 only adds ingest lookups:

- source post lookup by stable identity;
- current revision lookup for a SourcePost.

`SourceMediaRepository` only persists immutable revision-to-media-hash links using the Phase 1 `source_post_media` table.

### SourceIngestService

Formal outcomes:

```text
NEW_SOURCE_POST
DUPLICATE_IGNORE
SOURCE_UPDATED
```

Behavior:

```text
same source identity + same source content hash
→ DUPLICATE_IGNORE
→ no new revision
→ last_seen_at advances

same source identity + changed source content hash
→ SOURCE_UPDATED
→ next immutable SourcePostRevision
→ current_revision_id moves to new revision
```

A contact/promotion-only source edit that sanitizes to the same factual text and has the same media does not create a false factual revision.

### TelegramCollector

Phase 2 provides the collector core boundary only:

```text
observed Telegram evidence
→ source identity
→ sanitize
→ content/media hash
→ SourcePost / SourcePostRevision
→ source media links
→ END
```

It is deliberately not connected to production Telethon sessions/services in this phase.

Collector Phase 2 code has:

- zero publication imports;
- zero publisher imports;
- zero package build calls;
- zero Telegram channel writes.

## DB migrations

**None.**

Phase 2 uses the schema frozen and approved by Phase 1. No migration or schema contract was changed in Phase 2.

## Tests added

13 Phase 2 ingest tests were added, covering:

- single source identity;
- album/grouped identity;
- content hash normalization;
- exact duplicate -> `DUPLICATE_IGNORE`;
- contact-only edit remains duplicate/no new fact revision;
- factual edit -> `SOURCE_UPDATED` revision 2;
- revision `source_created_at` / `fetched_at` persistence;
- sanitizer fact-prefix preservation;
- Unicode noise removal;
- stable media SHA-256 identity;
- fewer than 4 photos still preserved;
- album edit becomes revision, not duplicate skip;
- static collector boundary: no publication/publisher/package-build dependency.

## Tests passed

CI verification for implementation head `232ae9d99c1871cf2ec37f3860c7882f159f45a7`:

- Workflow: `qiaolian-ui-check`
- Run: `34148794641`
- Job: `101826454123`
- Syntax/import check: PASS
- `tests/v3`: **51 passed in 0.41s**
- old production regression suite: **261 passed, 2 existing warnings in 6.37s**
- failures: **0**

The two warnings are the pre-existing `python-telegram-bot ConversationHandler` warnings from legacy production tests.

The existing workflow does not listen to PRs whose base is `v3/refactor-baseline`. To obtain the required CI without modifying the workflow file outside Phase 2 scope, PR #42 was temporarily retargeted to `v3/phase0-baseline` only for the CI trigger. After final report CI, the PR base is restored to `v3/refactor-baseline`. No code was borrowed from the temporary base change and no workflow code was modified.

## Diff summary

Phase 2 extracts only the stable collector/source-evidence behavior from locked V2.2 `collector_bot.py` and `source_sanitizer.py`.

The critical legacy bug is not carried forward: legacy collection treated an existing source tuple as a duplicate before comparing content. V3 Phase 2 distinguishes source identity from immutable content revision, so editing the same source post produces a new revision when facts/media actually change.

## Behavior changes

New V3-only behavior:

- exact factual duplicate: no second revision;
- same logical source post with changed facts/media: new revision;
- contact-only changes do not manufacture fact updates;
- album identity remains stable across edits;
- media identity uses content SHA-256;
- fewer than 4 photos are preserved as source evidence and flagged insufficient rather than discarded;
- source evidence persistence stops before Parser/publication.

There is **no production runtime switchover** in Phase 2.

## Compatibility kept

- locked V2.2 production files remain unchanged;
- Phase 1 database contracts remain unchanged;
- legacy collector remains production runtime until a later approved migration phase;
- V2.2 sanitizer semantics used here are preserved by regression-style tests;
- current production regression suite remains green.

## Compatibility removed

None from production runtime.

Phase 2 does not delete or disable any legacy code.

## Known blockers

No Phase 2 functional blocker is known after current tests.

Intentional later-phase work, not Phase 2 blockers:

- no Parser invocation/enqueue wiring yet;
- no production Telethon/session/service cutover;
- no Listing/Offer materialization;
- no Quality Gate;
- no publication/publisher behavior.

These belong to later phases and must not be pulled into Phase 2.

## Rollback notes

Phase 2 is branch/PR-only and has:

- no DB migration;
- no deployment;
- no server state change;
- no Telegram mutation;
- no legacy production file edit.

Rollback is therefore code-only: discard/revert the Phase 2 branch/PR commits and retain the Phase 1 PASS head `cee9e567d7681c1a81f900922031c3c7ab5bbede`.

## Safety

- Real Telegram mutation: **NO**
- Production server operation: **NO**
- Deployment: **NO**
- Legacy production runtime modification: **NO**
- Parser modification: **NO**
- Publisher modification: **NO**
- User Bot modification: **NO**
- Admin Bot modification: **NO**
- Phase 3 started: **NO**

## Next phase prerequisites

Stop after Phase 2.

Do not merge or start Phase 3 until independent review of PR #42 returns `PASS` for:

- exact duplicate vs source update behavior;
- album identity;
- sanitizer contract;
- media hash and <4-photo preservation;
- no publication/publisher/package-build dependency;
- diff remains inside the Phase 2 allowed boundary;
- final CI remains green.
