# PHASE_3_RESULT.md

## Baseline

- Repository: `Gymmmm/bot123123`
- Locked V2.2 extraction source: `8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- Phase 2 PASS head / Phase 3 parent: `60e3fbe838acaeeebf0f27ec25f8901bcdb825de`
- Phase 3 branch: `v3/phase3-parser-listing`
- Phase 3 PR: `#43`
- PR remains Draft and unmerged.

## Phase 3 scope

Phase 3 is limited to Parser / Canonical / Listing / Offer / Public ID extraction.

Allowed files remain within:

- `qiaolian_v3/parser/**`
- `qiaolian_v3/listing/materializer.py`
- `qiaolian_v3/listing/public_id.py`
- `qiaolian_v3/legacy/public_id_compat.py`
- Phase 3 parser/listing tests
- this report

No Publisher, User Bot, Admin Bot, Telegram mutation, deployment, server operation, DB migration, production runtime switch, or Phase 4 quality/media/dedupe work is included.

## Independent review remediation status

The first Phase 3 independent review reported four blockers. All four remain closed:

1. unresolved rent + sale intent is represented only as `deal_type=unknown`, `deal_type_candidates=["rent","sale"]`, review flag `conflicting_deal_type`, and stops before Listing/Offer materialization;
2. `canonical_facts_hash` uses an explicit stable business-facts projection and excludes source/revision IDs and operational evidence;
3. locked V2.2 canonical capability is preserved by the exact locked parser blob plus V3-only contract normalization;
4. Listing identity is SourcePost-anchored in Phase 3 so separate sources do not weakly merge.

Sale remains valid storage data and produces a `sale` ListingOffer with `publication_policy=store_only` and `publish_block_reason=sale_not_enabled_for_rent_channel`.

QL Public ID behavior remains unchanged.

## Locked V2.2 canonical implementation

The exact locked V2.2 canonical implementation remains in:

```text
qiaolian_v3/parser/canonical_v22_locked.py
```

Its Git blob SHA remains exactly:

```text
6b0616efeda2dd9db34e6aae937cdf8a68461350
```

The file remains byte-identical to the locked `qiaolian_dual/canonical_facts.py` source blob. It is intentionally not cleaned up internally because its purpose is to preserve locked extraction behavior exactly.

The formal V3 wrapper is `qiaolian_v3/parser/canonical.py`. It delegates extraction to the locked blob and applies only V3 contracts: V3 taxonomy binding, three-state deal type normalization, conflict review semantics, safe enrichment, and stable canonical business hash.

## Canonical public API surface

The second independent Phase 3 review confirmed the four prior blockers as PASS and identified one small public-surface residue.

The formal `qiaolian_v3.parser.canonical` API no longer exposes the legacy decision/projection helpers:

```text
draft_projection
is_buildable
has_confirmed_physical_area
```

They have been removed from the formal wrapper implementation and from its `__all__`.

This establishes the Phase 3 authority boundary:

```text
Parser / Canonical
-> facts only

Listing projection/materialization
-> CanonicalListingMaterializer

Eligibility / Quality Gate
-> Phase 4 authority, not a Phase 3 Parser public API
```

`qiaolian_v3/parser/__init__.py` already did not expose these names and remains unchanged.

The private locked blob is not inspected or modified by the public-surface contract test. Legacy helper definitions may remain inside the private locked implementation solely to preserve its original bytes; they are not part of the formal V3 parser API.

## Canonical deal-type contract

Persisted deal type is exactly:

```text
rent | sale | unknown
```

Routing remains:

```text
rent evidence only -> rent
sale evidence only -> sale
rent + sale intent -> unknown + [rent, sale] + conflicting_deal_type
no reliable intent -> unknown
```

`mixed` and `ambiguous_deal_type` are not formal persisted V3 deal states.

An unresolved `unknown` CanonicalRecord cannot materialize ListingOffers in Phase 3. Resolution/override routing belongs to later work.

## Stable canonical facts hash

`canonical_facts_hash` is calculated from an explicit business-facts allowlist.

Operational/audit-only evidence does not affect the hash, including source/revision DB IDs, source identity keys, timestamps, raw/sanitized text hashes, local paths, Telegram transport identifiers, media transport snapshots, evidence excerpts/spans, review notes, captions, parser/schema metadata and other runtime evidence.

Full evidence remains in `facts_json` for audit.

Tests prove that the same business facts with different source/revision/local-path/transport metadata produce the same canonical hash, while an actual business fact change changes the hash.

## Locked canonical regression coverage

V3 regression tests directly call `qiaolian_v3.parser.canonicalize_source` and cover locked V2.2 behavior including:

- complex layout, office and helper-room rules;
- English bedrooms/bathrooms;
- floor extraction;
- current/original rent;
- rent/sale numeric separation;
- generic payment terms;
- contract term;
- available date;
- management/internet/water/electric/parking facts;
- viewing time and video viewing;
- primary size;
- land/building dimensions and area;
- unlabelled dimension separation;
- locked taxonomy and public-location behavior;
- safe enrichment.

Locked semantic behavior is preserved even where imperfect; tests are aligned to the locked behavior rather than silently improving parser semantics in Phase 3.

## Listing identity boundary

Phase 3 does not perform weak cross-source dedupe.

```text
same SourcePost + later revision
-> same property_identity_key
-> same listing_id
-> same QL public_listing_id
```

```text
different SourcePosts
+ same project/location/property_type/layout/size/floor
+ no strong media/unit identity
-> different property_identity_key
-> different listing_id
```

Cross-source strong dedupe is reserved for Phase 4.

## Persistence path

Resolved rent or sale path:

```text
SourcePostRevision
-> CanonicalParserService
-> canonical_records
-> CanonicalListingMaterializer
-> v3_listings
-> listing_sources
-> listing_offers
```

Unresolved rent+sale path:

```text
SourcePostRevision
-> canonical_records (deal_type=unknown, conflicting_deal_type)
-> STOP before Listing / Offer materialization
```

No drafts or publication packages are part of Phase 3.

## DB schema

**No Phase 3 DB migration.**

The approved Phase 1 schema remains unchanged.

## Tests

Current V3 coverage includes all Phase 1/2 contracts plus Phase 3 parser/listing contracts and the final public-surface guard.

The new static/public-surface test verifies that formal `qiaolian_v3.parser.canonical` does not expose:

```text
draft_projection
is_buildable
has_confirmed_physical_area
```

It does not inspect or modify `canonical_v22_locked.py` internals.

## Previous CI evidence

Corrected implementation CI before the final public-surface cleanup:

- Run `34226265498` (#658)
- `tests/v3`: **94 passed**
- production regression: **261 passed, 2 existing warnings, 0 failures**

Report-only CI before the final public-surface cleanup:

- Run `34226571232` (#659)
- `tests/v3`: **94 passed**
- production regression: **261 passed, 2 existing warnings, 0 failures**

The two warnings are the pre-existing `python-telegram-bot ConversationHandler` warnings in the legacy production regression suite.

A new final CI run after this public-surface cleanup must be green before Phase 3 stops again.

## Safety

- PR merge: **NO**
- Deployment: **NO**
- Production server operation: **NO**
- Telegram mutation: **NO**
- Publisher modification/wiring: **NO**
- User Bot modification/wiring: **NO**
- Admin Bot modification/wiring: **NO**
- DB schema migration: **NO**
- Production legacy file modification: **NO**
- Parser business-fact rule modification: **NO**
- Materializer behavior modification: **NO**
- Phase 4 started: **NO**

## Stop condition

After final CI is green and PR #43 is confirmed back on formal base `v3/phase2-ingest` at Phase 2 PASS head `60e3fbe838acaeeebf0f27ec25f8901bcdb825de`, Phase 3 stops again for independent review.

Do not merge, deploy, operate production systems, operate Telegram, or start Phase 4 until explicitly instructed after a Phase 3 PASS review.
