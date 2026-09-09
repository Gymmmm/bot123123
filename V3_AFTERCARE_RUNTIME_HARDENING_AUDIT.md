# V3 aftercare + runtime hardening audit

Base reviewed: `codex/v3-core-extraction-aftercare-ops` at `11da97cb81bb8af9ad7bc87f3ba77fca7e48f409`.

## What was already good

The aftercare patch correctly keeps V3 inventory/publication truth in `listings_v3`, `listing_offers`, and `publication_instances`, while using `draft_id` only as provenance. It adds `rental_cases_v3`, `operations_tasks_v3`, and task event history without modifying parser/materializer/publisher semantics.

## Remaining gaps found

1. **No isolated operational runtime.** The patch had storage/business code but no candidate runtime root, candidate DB/media/session isolation, side-by-side systemd template, or candidate-safe startup wrapper.
2. **No production-safe empty-DB recovery path for the combined feature set.** V3 has explicit bootstrap, but there was no candidate command tying path isolation + backup + V3 bootstrap + integrity check together.
3. **The previous runtime port used legacy truth tables.** Its bootstrap created `drafts`, `listings`, `posts`, and legacy publication tables. That is not appropriate for this V3 branch and was deliberately not copied.
4. **Tenant binding was not enforced when creating the rental case.** `tenant_binding_id` could be supplied without verifying that the binding exists, is active, or belongs to the case user.
5. **Repair task linkage was incomplete.** A repair ticket only had to exist; it was not required to belong to the same tenant/binding as the rental case.
6. **Rental case lifecycle had no event history.** Task changes were audited, but case transitions (`pending_handover -> active -> ending/closed`) were not.
7. **Open-case idempotency was not defined.** Repeating a handover/case creation flow could create multiple active cases for the same tenant binding and listing.
8. **Closed/cancelled cases could still be used as a source for new operational work if callers only used the generic repository.
9. **No database integrity check before service start.** Presence of tables alone does not catch SQLite corruption.
10. **No backup before additive initialization of a non-empty candidate database.** Although DDL is additive, a pre-init snapshot is operationally safer for repeatable testing.

## Hardening added on this branch

- isolated `QIAOLIAN_RUNTIME_ROOT` with escape protection for DB/media/session/backup paths;
- V3-native bootstrap only: `listings_v3`, `listing_offers`, `publication_instances`, aftercare tables; no legacy `drafts/listings/posts` creation;
- pre-init backup for an existing candidate DB;
- SQLite `PRAGMA quick_check` in candidate preflight;
- one runtime dispatcher for collector / canonical worker / publisher / user;
- side-by-side hardened systemd template that writes only under `/opt/qiaolian_v3_candidate`;
- rental-case event table and controlled case transitions;
- idempotent `create_case_from_binding` for the same active binding/listing;
- repair-task creation validates case user + tenant binding against the repair ticket;
- closed/cancelled rental cases cannot accept new repair tasks through the lifecycle service;
- CI coverage for native empty-DB recovery, path isolation, backup preservation, case lifecycle, repair lineage, and existing V3 tests.

## Deliberately not changed

- parser/canonicalization rules;
- `listings_v3` / `listing_offers` truth model;
- Telegram publication protocol;
- User Bot UI;
- production systemd units;
- automatic implicit V3 schema mutation in normal production entrypoints.
