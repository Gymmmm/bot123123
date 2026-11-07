# PR114 release evidence and remaining gate (2026-11-07, Phnom Penh UTC+7)

**Offline acceptance is not production sign-off. PR stays Draft. Do not merge,
deploy, restart services, clear queues or resend unknown sends from this report.**

## Reproducible offline collection

`tests/v3_core/release_acceptance_files.txt` is the explicit release-path manifest.
CI installs Tesseract with `eng` and `chi_sim`, then runs this exact collection
with sockets disabled and an isolated temporary DB. The JUnit artifact is
`pr114-offline-release-path-results`; the full suite still runs even on failures.
No tests are deleted or deselected from the full-suite command.

Coverage: one-use/owner/expiry/status-revision confirmation, cancel/replay,
persistent sync intent before I/O, stale-edit compensation after publisher ACK,
expired in-flight lease, no-op success/no alert, current post identity, legacy
appointments and takeover, cancellation/rescheduling side effects, search/query/
results/navigation, manual pending preview, canonical worker, unknown delivery
blocking and explicit reconciliation, and villa real-photo repair/re-review.

The villa integration uses the **final production controller MRO**, real intake,
canonical worker, SQLite source/inventory, recorded vision fixtures, real OCR,
media preparation, cover/package building and a fake reply sink. It verifies no
publication instance is created. It is not a live Gemini/Telegram/mobile test.
Without Tesseract this one integration test skips locally; CI must execute it.

## Independently established CI baseline

Run: https://github.com/Gymmmm/bot123123/actions/runs/42358578247
Python 3.12.16 / Ubuntu 24.04, Tesseract `eng` + `chi_sim` verified.
Base `1a7c4c44cb47478aaf9d30aa89aa5ee012eafacf`: 1623 passed, 114 failed,
9 collection errors. Candidate `3a31e0f3f170767cce8dadee7540e484eebe9de8`:
1654 passed, 114 failed, 9 collection errors; identical failing **node IDs**.
Targeted CI: 56 passed, including all 15 cancel/race regression tests.
Both previously failing local cover-service fixtures pass in this OCR-equipped
CI environment. Local missing-OCR rejections are not evidence of a product bug.
Comparison artifacts contain complete logs, not a historical-failure assumption.

111 named failed tests plus failed subtests produce the reported 114 failures:
14 FileNotFoundError, 6 SQLite fixture/schema failures, 9 AttributeError mocks,
80 assertion/contract failures, 1 KeyError, 1 TypeError. Nine collection errors:
`run_pipeline_autopilot` (4 files), `qiaolian_pipeline` (2), `scripts.ops` (1),
`v2_admin` (1), removed `_draw_new_cover` import (1).

## Historical failures: disposition is NOT blanket exemption

* Missing `schema_core.sql`, old bootstrap/direct-publisher scripts and retired
  imports: inherited, outside the four V3 entrypoints. Keep the tests and history;
  do not invent stub modules or revive direct senders merely to turn them green.
  Their legacy functional guarantees require a migration/test-ownership decision.
* V3 search-card mock missing `deposit_terms`: a safe test-only fixture repair;
  all approved product/UI assertions remain intact and its complete file passes.
* Six other V3 failures assert older home/service/album button layouts. Visual and
  album behavior is frozen, so no product change was made to satisfy them. A
  contract-owner comparison is required before explicit waiver; not auto-waived.
* Remaining legacy assertion/schema/mock errors are retained as full-suite gate
  failures. Passing release paths narrows the risk; it does not prove them safe.

## Still-blocking evidence gaps / product finding

* Appointment-history calendar gap is now repaired without schema migration or
  bulk rewrites. New date selection/confirmation stores ISO calendar dates with
  Phnom Penh date/time interpretation (existing created_at storage stays unchanged);
  relative input is frozen before midnight can move it. Existing slots retain their fixed time windows; custom times are
  concrete clock values. Both legacy and V3 repositories reconcile known date/
  time aliases for duplicate checks. Cancellation and rescheduling stay intact.
  Legacy yearless dates use the creation context only inside the existing quick
  picker's 0/1/2-day horizon, including Dec/Jan rollover. Once a yearless row is
  past, its lost selection/year context must also be confirmed: creation proximity is not proof of completion. Other unknown years,
  invalid dates and old relative strings remain visible as date-needs-confirmation,
  never guessed completed. Known ISO past dates retain completed semantics.
* The existing V3 readonly runtime audit workflow has been narrowed to fixed
  SHA/service/ExecStart metadata, two fixed DB identities, persisted boolean
  switches and aggregate queue counts. It has no command inputs, repository code
  execution, runtime/proc environment reads, raw logs or user/source identifiers.
  It reuses only its original host pin and original Actions-held SSH secret.
  Successful real audit: https://github.com/Gymmmm/bot123123/actions/runs/42360408689
  Snapshot 2026-11-07 16:35:53 Phnom Penh UTC+7: current
  `1a7c4c44cb47478aaf9d30aa89aa5ee012eafacf`; four services active, expected
  run_component entrypoints; both fixed DB paths resolve to the same file and
  device/inode (2049/390582). All three auto-publish/broadcast switches are present
  and true. Queued/sending/sync-outbox counts are zero; unknown_delivery is 21
  (untouched). The feature's in-flight table is absent on this old production SHA.
* Publisher restart schedules autopilot; deployment can publish backlog. The
  switches are now verified enabled; backup/integrity and rollback readiness
  still need release-time inspection. Restart must not be presumed message-free.
  Unknown sends require individual reconciliation, never automatic replay.
* Test-channel business closure, physical phone and production behavior are not
  accepted by this offline collection. No real messages or production DB writes
  were performed. Overall release gate is **not passed**.

## Subsequent complete CI evidence

Run https://github.com/Gymmmm/bot123123/actions/runs/42359341990 on
`ec7aaa38cc4acc9a3e732821f17753505582036c` completed: targeted 56 passed;
release-path collection 317 passed, no skips; full 1657 passed, 113 failed,
9 collection errors. Same-run base remained 1623 passed / 114 failed / 9 errors.
One inherited search-card mock failure was fixed, no tests removed. Current date
and readonly-audit changes must pass their own latest-SHA CI; this is not a waiver
of remaining legacy/UI failures or a production release approval.

Run https://github.com/Gymmmm/bot123123/actions/runs/42360408687 on
`8987c039fa3c7563ecbd095a4a9d1df1019d1081` completed: targeted 56 passed;
release-path collection 433 passed with real OCR and no skips; full 1712 passed,
110 failed, 9 collection errors. Same environment/base remained 1623 passed,
114 failed, 9 errors. Full failing-node comparison finds no newly failing IDs.
The four resolved IDs are the search-card fixture plus three legacy simplified
appointment tests whose mocks missed the real inventory adapter. All product
assertions remain. The conservative past-yearless guard added afterward must
pass its own latest-SHA CI; remaining failures are not waived.
