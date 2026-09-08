# V3 Phase 5 — 100 real-source dry-run

Formal parent: `af83ccb42983bbfc7b32ecf444d696725671b2a8`.

Scope is limited to existing real-source fixtures, dry-run report tooling, and bugs in Phase 2-4 exposed by those samples. No Publisher/Admin/User Bot/server/Telegram writes.

Required sample coverage: rent, sale, unknown, insufficient photos, conflicting facts, duplicates, source price updates.

Required per-sample output: source identity, canonical facts, deal type, dedupe result, quality result, listing/offer projection, blocking reasons, warnings.

Acceptance: sale 100% store-only; zero sale AUTO_PUBLISH; exact duplicate creates no second listing target; source updates are not duplicate-skipped; no obvious false-rent caused by sale price, deposit, or utility fees.

Fixture provenance: 104 independent historical production-derived group rows from commit `7f9865a73a28e7953211f6d16436791f866b734d`; aggregate `总条数` values are retained as evidence and are never expanded into synthetic source messages.
