# V3 Phase 5 — 100 real-source dry-run

Formal parent: `af83ccb42983bbfc7b32ecf444d696725671b2a8`.

Scope is limited to existing real-source fixtures, dry-run report tooling, and bugs in Phase 2-4 exposed by those samples. No Publisher/Admin/User Bot/server/Telegram writes.

## Real-derived sample set

104 independent historical production-derived group rows are retained from commit `7f9865a73a28e7953211f6d16436791f866b734d`. Aggregate `总条数` values are retained only as evidence and are never expanded into synthetic Telegram messages. Historical media is not invented; absent media evidence is explicitly `historical_export_has_no_media_evidence` and remains fail-closed.

## Real duplicate / revision evidence

`tests/v3/fixtures/phase5_real_history_evidence.json` records the audit result.

A real exact same-source duplicate is available: source `zufang555`, source_post_id `728`, source URL `https://t.me/c/2498584369/6443`. The same exported historical row exists unchanged in repository snapshots `d70e8efa2bac6059e97b65cf45a9507a081aff66` and `7f9865a73a28e7953211f6d16436791f866b734d`. Both snapshots preserve the same source identity and raw historical export text. This is classified `DUPLICATE_IGNORE` and must not create a second listing/publication target.

A traceable real changed-revision pair is **not available** in the existing repository history/exports/fixtures searched for Phase 5. The report therefore emits `REAL_REVISION_EVIDENCE_UNAVAILABLE`. No aggregate count is split, no historical message is invented, and no fake `月租：$999/月` edit is presented as real evidence. `_scenario_metrics()` remains only a synthetic unit regression proving the existing dedupe classifier returns `UPDATE_EXISTING` for a changed same-source canonical hash; it is explicitly excluded from real-history acceptance evidence.

## Complete deterministic report

`python tools/v3_phase5_dry_run.py` prints the complete deterministic JSON report. It includes every real-derived row rather than stripping `results` from CLI output.

Every row includes source identity, provenance, canonical facts, deal type, dedupe result, quality result, publication policy, listing/offer projection, blocking reasons, warnings, and media evidence status.

The same report contains `anomaly_review`. Anomalies are recorded row-by-row with source identity, anomaly type, actual classification, reason for review, and a non-empty `reviewed_disposition`. Covered categories include sale, mixed rent/sale conflict when present, unknown, reject/non-property when present, incomplete/insufficient evidence, unexpected parser classification when present, and duplicate/update evidence.

## Acceptance

- sale AUTO_PUBLISH = 0
- detected sale publication policy = `store_only`
- real exact same-source duplicate = `DUPLICATE_IGNORE`
- exact duplicate creates no second listing/publication target
- real changed revision: `REAL_REVISION_EVIDENCE_UNAVAILABLE`; no fabricated evidence
- synthetic changed-source regression remains `UPDATE_EXISTING` but is not claimed as real history
- sale/deposit/electric/utility-like numbers do not create false rent
- historical media absence remains fail-closed
- no Publisher/Admin/User Bot/DB migration/server/Telegram/deployment/Phase 6+7 changes
