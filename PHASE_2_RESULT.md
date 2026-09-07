# PHASE_2_RESULT.md

## Baseline

- Repository: `Gymmmm/bot123123`
- Locked V2.2 production source baseline: `8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- Phase 1 PASS head / Phase 2 parent: `cee9e567d7681c1a81f900922031c3c7ab5bbede`
- Phase 2 branch: `v3/phase2-ingest`
- Phase 2 PR: `#42`
- Phase 2 remains Draft/unmerged pending independent re-review.

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

## Independent review corrections

The first independent Phase 2 review returned `CHANGES REQUIRED` with two blockers. Both are addressed in the same Draft PR #42 without widening scope.

### 1. Ordered media identity in source_content_hash

Correct contract:

```text
source_content_hash = sha256(sanitized_text + normalized ordered media identity)
```

`SourceIngestService` normalizes media by `sort_order`. `make_source_content_hash()` now preserves that incoming order and preserves duplicate media identities. It does not sort media hashes/identities internally.

Therefore:

```text
same text + [A, B] != same text + [B, A]
```

and a same-source album reorder produces:

```text
SOURCE_UPDATED
revision + 1
```

rather than `DUPLICATE_IGNORE`.

### 2. Telegram media read/download adapter and identity fallback

`qiaolian_v3/media/source_media.py` now contains `TelegramSourceMedia.download()` as a Phase-2-only read adapter equivalent to the stable read/download responsibility from locked V2.2 `collector_bot.download_media`.

Boundary:

```text
Telegram observed media
→ client.download_media(...)
→ local file hash when available
→ Telegram evidence metadata
→ SourceMedia
→ ingest
```

It does not start a Telegram client/session, register listeners, publish, send, edit, or perform any Telegram write.

Stable media identity contract is now:

```text
file SHA-256
↓ if unavailable
Telegram unique identity
↓ final defensive fallback
Telegram file id
```

The required contract is satisfied by the first two layers: file hash is preferred and Telegram unique identity is the official fallback. Telegram file id is retained only as a last-resort evidence fallback rather than replacing the required unique identity behavior.

Evidence metadata retained:

- `local_path`
- `content_hash`
- `telegram_file_id`
- `telegram_file_unique_id`
- `message_id`
- `sort_order`
- `media_type`

Tests use fake Telegram client/message objects only; no real network or Telegram session is used.

## Files changed

Relative to Phase 1 PASS head, Phase 2 remains confined to:

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

## Core contracts

### Source identity

- `build_single_external_post_id(message_id)`
- `build_album_external_post_id(grouped_id, anchor_message_id)`
- `make_source_content_hash(sanitized_text, ordered_media_identities)`

Single-message identity remains Telegram message id.
Album identity follows locked V2.2: `album_{grouped_id}`, with `album_{anchor_message_id}` fallback.

### Sanitizer

Extracted from locked V2.2 `source_sanitizer.py`:

- strips Unicode formatting/private-use noise;
- isolates URL/handle/phone/contact lines;
- removes source promotion text;
- preserves factual prefix when contact text follows facts on the same line;
- keeps source contacts outside sanitized canonical input text.

### Source media

`SourceMedia` provides immutable source evidence metadata and `media_identity`.

`TelegramSourceMedia.download()` provides the read/download adapter using a supplied client only. It is intentionally dependency-light and imports no Publisher or publication code.

Phase 2 does not rank photos, select covers, render media, or decide listing quality.

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

same source identity + changed facts/media/media order
→ SOURCE_UPDATED
→ next immutable SourcePostRevision
→ current_revision_id moves to new revision
```

A contact/promotion-only edit that sanitizes to the same factual text and has the same ordered media identities does not create a false factual revision.

## DB migrations

**None.**

Phase 2 uses the Phase 1 approved schema. No schema contract was changed.

## Tests

Phase 2 ingest coverage now includes:

- single source identity;
- album/grouped identity;
- ordered source content hash;
- duplicate media identities preserved in hash input;
- exact duplicate -> `DUPLICATE_IGNORE`;
- contact-only edit remains duplicate/no new fact revision;
- factual edit -> `SOURCE_UPDATED` revision 2;
- same text + same media identities in different order -> different hash -> `SOURCE_UPDATED` -> revision 2;
- revision `source_created_at` / `fetched_at` persistence;
- sanitizer fact-prefix preservation;
- Unicode noise removal;
- stable file SHA-256 identity;
- Telegram fake-client download -> file hash preferred;
- fake download unavailable -> Telegram unique identity fallback;
- Telegram evidence metadata preservation;
- fewer than 4 photos still preserved;
- album edit becomes revision, not duplicate skip;
- static collector boundary: no publication/publisher/package-build dependency.

## CI

Post-review-fix implementation CI:

- Workflow: `qiaolian-ui-check`
- Run: `34152093453` (#652)
- Tested head: `372c63a924db028cc6650d05abdc9e8b13bcfc1f`
- Syntax/import checks: PASS
- `tests/v3`: **54 passed in 0.36s**
- production regression: **261 passed, 2 existing warnings in 6.14s**
- failures: **0**

The two warnings are the same pre-existing `python-telegram-bot ConversationHandler` warnings from the legacy production suite.

This report commit is the only change after that implementation CI and is followed by the same final CI gate before re-review.

The existing workflow only listens to PRs targeting `v3/phase0-baseline`. As previously independently validated, the PR is temporarily retargeted only to trigger the synthetic CI merge, then restored to the formal Phase 1 PASS base `v3/refactor-baseline`. No workflow file is modified and no code is borrowed from the temporary base.

## Compatibility kept

- locked V2.2 production files remain unchanged;
- Phase 1 database contracts remain unchanged;
- legacy collector remains production runtime;
- no V3 production cutover;
- current production regression suite remains green.

## Compatibility removed

None from production runtime.

Phase 2 deletes or disables no legacy code.

## Known blockers

The two blockers from the first independent Phase 2 review are fixed and covered by passing tests. No known Phase 2 implementation blocker remains; independent re-review is still required before PASS/merge.

No Phase 3 work has started.

Intentional later-phase work remains outside Phase 2:

- no Parser invocation/enqueue wiring;
- no production Telethon/session/service cutover;
- no Listing/Offer materialization;
- no Quality Gate;
- no publication/publisher behavior.

## Rollback notes

Phase 2 is branch/PR-only and has:

- no DB migration;
- no deployment;
- no server state change;
- no Telegram mutation;
- no legacy production file edit.

Rollback is code-only: revert/discard Phase 2 and retain Phase 1 PASS head `cee9e567d7681c1a81f900922031c3c7ab5bbede`.

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

## Next phase prerequisite

Stop after Phase 2. Do not merge or start Phase 3 until independent re-review of PR #42 returns `PASS`.