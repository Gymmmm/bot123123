# PHASE_3_RESULT.md

## Baseline

- Repository: `Gymmmm/bot123123`
- Locked V2.2 extraction source: `8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- Phase 2 PASS head / Phase 3 parent: `60e3fbe838acaeeebf0f27ec25f8901bcdb825de`
- Phase 3 branch: `v3/phase3-parser-listing`
- Phase 3 PR: `#43`
- PR remains Draft/unmerged pending independent Phase 3 review.

## Scope

Phase 3 implements only Parser / Canonical / Listing / Offer / Public ID extraction required by `V3_EXTRACTION_PLAN.md`.

Allowed scope used:

- `qiaolian_v3/parser/**`
- `qiaolian_v3/listing/materializer.py`
- `qiaolian_v3/listing/public_id.py`
- `qiaolian_v3/legacy/public_id_compat.py`
- Phase 3 parser/listing tests
- this report

No publishing, Publisher, User Bot, Admin Bot, Telegram write, deployment, server operation, DB schema migration, or Phase 4 quality/media-gate work was introduced.

## Locked behavior extracted

The Phase 3 implementation was derived only from locked V2.2 sources, including:

- `qiaolian_dual/canonical_facts.py`
- `qiaolian_dual/listing_taxonomy.py`
- `parser_v2_safe.py`
- `qiaolian_dual/canonical_listing_materializer.py`
- `qiaolian_dual/public_listing_id.py`
- parser regression tests at the locked SHA

`qiaolian_v3/parser/taxonomy.py` uses the locked taxonomy blob. The safe-enrichment implementation used by V3 is pinned from the locked V2.2 safe-enrichment blob and exposed through a V3-only entry point.

## Canonical deal-type contract

Persisted deal type is exactly:

```text
rent | sale | unknown
```

Routing:

```text
rent evidence only -> rent
sale evidence only -> sale
rent + sale intent -> unknown + candidates [rent, sale]
no reliable intent -> unknown
```

`mixed` is never emitted as a V3 canonical deal type.

When both rent and sale numeric evidence are explicit, the facts remain independently preserved while deal type remains `unknown`; downstream storage may create independent rent and sale offers without pretending the property is rent-only.

## Price separation

The V3 parser preserves the locked V2.2 money boundary:

- rent is extracted only from explicit rent/monthly contexts;
- sale price is extracted only from explicit sale contexts;
- deposit amounts do not become rent;
- utility rates do not become rent;
- sale prices do not become rent;
- conflicting rental values are not guessed.

Locked rental regressions covered include `$1,500/月`, `$850/月`, `租金520$包物业`, `特价出租600$`, `出租情况：850$`, and `7000美元每月`.

## Sale storage

Sale is valid canonical real-estate data in Phase 3.

```text
sale source
-> canonical_records deal_type=sale
-> v3_listings
-> listing_offers offer_type=sale
-> publication_policy=store_only
-> publish_block_reason=sale_not_enabled_for_rent_channel
```

There is no `skipped_non_rental` path in V3 Phase 3.

## Safe enrichment and taxonomy

Safe enrichment remains additive and does not parse or override money. Covered stable rules include services, furniture/appliances, decoration, amenities, explicit included items, lease wording, pending project candidates and unlabelled dimensions.

Taxonomy remains conservative:

- inventory/menu property types do not manufacture the current property type;
- unapproved project tokens remain review candidates;
- roads/market concepts stay separate from physical canonical area;
- explicit project/brand/location relationships remain separate.

## Canonical persistence

`CanonicalParserService` consumes only an immutable `SourcePostRevision` plus its V3 SourcePost identity and writes `canonical_records` using the approved Phase 1 repository/schema.

It does not read or write drafts and does not build publication packages.

## Listing / Offer materialization

`CanonicalListingMaterializer` consumes a persisted CanonicalRecord and writes only:

```text
v3_listings
listing_sources
listing_offers
```

The required Phase 3 flow is therefore:

```text
SourcePostRevision
-> CanonicalParserService
-> canonical_records
-> CanonicalListingMaterializer
-> v3_listings
-> listing_offers
```

It does not pass through `drafts` or `v3_publication_packages`.

Rent and sale are materialized as separate offers. A canonical record with explicit rent and sale evidence can materialize both offers while the canonical deal type remains `unknown`.

## Public listing ID

V3 Public ID is owned directly by `v3_listings.public_listing_id`; no second public-ID truth table is introduced.

Contract:

```text
QL-<location-code>-<A2B3-style code>
```

The ID is assigned once and remains stable for the Listing. Known location-aware codes are extracted from locked V2.2 behavior, with `PP` fallback.

Legacy `QC/QJ/L` normalization is isolated in `qiaolian_v3/legacy/public_id_compat.py` and does not become V3 identity truth.

## DB schema

**No Phase 3 DB migration.**

Phase 3 uses the approved Phase 1 persistence schema unchanged.

## Tests

Phase 3 adds real parser/listing contract tests covering:

- rent / sale / unknown routing;
- rent+sale intent -> unknown + candidates, never mixed;
- sale is valid canonical data;
- locked rent regressions;
- sale price extraction;
- deposit/utility/sale amounts != rent;
- conflicting rent values are not guessed;
- safe enrichment does not overwrite money;
- services/furniture/appliances/amenities/included enrichment;
- pending project and unlabelled dimension review behavior;
- inventory property-type conservatism;
- market/project taxonomy conservatism;
- source -> canonical -> listing -> rent offer;
- sale -> listing -> store-only sale offer;
- unknown with explicit rent+sale -> two independent offers;
- no drafts table introduced by the V3 flow;
- no publication package produced;
- stable location-aware QL Public ID;
- legacy ID compatibility remains separate.

## CI verification

Implementation head verified after pinning the exact locked safe-enrichment implementation:

- Head: `315c2de0e0b77a8126ff1922864ee03818db2421`
- Workflow: `qiaolian-ui-check`
- Run: `34208599249` (#655)
- Job: `102003940814`
- syntax/import: PASS
- `tests/v3`: **74 passed in 0.48s**
- old production regression: **261 passed, 2 existing warnings in 6.12s**
- failures: **0**

The two warnings are the pre-existing `python-telegram-bot ConversationHandler` warnings in the legacy production suite.

The repository workflow currently listens to PRs targeting `v3/phase0-baseline`. As in the independently accepted Phase 2 procedure, PR #43 is temporarily retargeted only to trigger CI, then restored to its formal Phase 2 PASS base. No workflow file is modified and no code is borrowed from the temporary base.

A report-only final CI run must remain green before Phase 3 stops.

## Safety

- PR merge: **NO**
- Deployment: **NO**
- Production server operation: **NO**
- Telegram mutation: **NO**
- Publisher modification/wiring: **NO**
- User Bot modification/wiring: **NO**
- Admin Bot modification/wiring: **NO**
- DB schema migration: **NO**
- Phase 4 started: **NO**

## Stop condition

After the report-only CI is green and PR #43 is restored to base `v3/phase2-ingest`, Phase 3 stops for independent review.

Do not merge, deploy, operate production systems, or start Phase 4 until independent Phase 3 review returns `PASS`.
