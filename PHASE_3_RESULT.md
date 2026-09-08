# PHASE_3_RESULT.md

## Baseline

- Repository: `Gymmmm/bot123123`
- Locked V2.2 extraction source: `8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- Phase 2 PASS head / Phase 3 parent: `60e3fbe838acaeeebf0f27ec25f8901bcdb825de`
- Phase 3 branch: `v3/phase3-parser-listing`
- Phase 3 PR: `#43`
- PR remains Draft/unmerged pending the second independent Phase 3 review.

## Scope

Phase 3 implements only Parser / Canonical / Listing / Offer / Public ID extraction required by `V3_EXTRACTION_PLAN.md`.

Allowed scope used:

- `qiaolian_v3/parser/**`
- `qiaolian_v3/listing/materializer.py`
- `qiaolian_v3/listing/public_id.py`
- `qiaolian_v3/legacy/public_id_compat.py`
- Phase 3 parser/listing tests
- this report

No publishing, Publisher, User Bot, Admin Bot, Telegram write, deployment, server operation, DB schema migration, or Phase 4 quality/media/dedupe work was introduced.

## Independent review remediation

The first independent Phase 3 review returned `CHANGES REQUIRED`. This revision keeps the accepted Phase 3 boundaries, sale storage, QL Public ID, taxonomy and safe enrichment, and closes only the four reported blockers.

### 1. Unresolved rent + sale conflict

Persisted deal type remains exactly:

```text
rent | sale | unknown
```

The formal unresolved conflict contract is now:

```text
rent intent + sale intent
-> deal_type = unknown
-> deal_type_candidates = ["rent", "sale"]
-> review flag = conflicting_deal_type
```

`ambiguous_deal_type` is not part of the formal V3 contract. V2-only `mixed_sale_rent_terms` is also normalized away from V3 deal review semantics.

Both explicit numeric facts may remain in `facts_json` for audit:

```text
monthly_rent_usd = <explicit rent>
sale_price_usd   = <explicit sale price>
```

However an unresolved `unknown` CanonicalRecord is **not materializable** in Phase 3. `CanonicalListingMaterializer` raises `unresolved_deal_type` before any Listing or ListingOffer write. Phase 3 does not implement Admin Bot resolution and does not write `routing_decision`.

A later resolved canonical/override flow may explicitly establish the allowed transaction state. That later resolution behavior is outside this Phase 3 PR.

### 2. Stable canonical business-facts hash

`canonical_facts_hash` no longer hashes the entire enriched facts dictionary.

`qiaolian_v3.parser.canonical_business_projection()` defines an explicit allowlist of stable business facts, including transaction facts, property identity/taxonomy facts, layout, price, size, lease/cost details and safe business enrichment.

The hash excludes operational/audit evidence such as:

- SourcePost / SourcePostRevision identity and DB IDs;
- source identity keys;
- timestamps;
- raw/sanitized text hashes;
- local file paths;
- Telegram message/file/transport identifiers;
- media transport snapshots;
- evidence spans/excerpts;
- review/quality notes;
- manual override audit records;
- captions/display copy;
- parser/schema revision metadata;
- the hash itself.

`facts_json` still retains full source/evidence/audit information. Only the hash projection is restricted.

Regression tests prove:

```text
same business facts
+ different source_post_id / revision_id
+ different local_path / Telegram transport metadata
-> same canonical_facts_hash
```

and:

```text
business fact changes (for example rent 800 -> 850)
-> different canonical_facts_hash
```

### 3. Locked V2.2 canonical capability restored

The exact locked V2.2 `qiaolian_dual/canonical_facts.py` blob was copied into the isolated V3 parser as:

```text
qiaolian_v3/parser/canonical_v22_locked.py
```

Locked source blob SHA:

```text
6b0616efeda2dd9db34e6aae937cdf8a68461350
```

The production legacy file itself is unchanged.

The V3 canonical wrapper executes the locked extraction implementation against the V3-extracted taxonomy, then applies only V3 contract normalization (three-state deal type, conflict review, stable business hash and safe enrichment). This avoids the earlier simplified Phase 3 parser losing existing capabilities.

Restored locked capabilities include:

- complex layout patterns;
- office/helper-room layout handling;
- English bedroom/bathroom form;
- floor extraction;
- current/original rent;
- sale price separation;
- generic payment terms without inventing deposit month fields;
- explicit contract term;
- available date;
- management fee;
- internet fee;
- water rate;
- electric rate;
- parking fee;
- viewing time;
- video viewing availability;
- primary size;
- land dimension / land area;
- building dimension / building area;
- unlabelled dimension separation;
- highlights/tags;
- locked taxonomy/public-location behavior.

Locked semantic behavior is preserved even where it is imperfect. During the first remediation CI, one newly written regression expected the compound layout `2+1房+1佣人房 1厅 3卫` to be parsed more intelligently than V2.2 actually does. The locked regex ordering returns `1房+1佣人房`; the test was corrected to the locked result rather than changing parser semantics.

`tests/v3/parser/test_locked_v22_canonical_regressions.py` directly calls `qiaolian_v3.parser.canonicalize_source`, so old production parser tests are no longer used as a substitute for V3 parser regression coverage.

### 4. Listing property identity boundary

Phase 3 no longer performs weak cross-source property dedupe.

Listing identity is SourcePost-anchored for Phase 3:

```text
same SourcePost
+ later SourcePostRevision
-> same property_identity_key
-> same listing_id
-> same QL public_listing_id
```

Different SourcePosts remain independent by default:

```text
different SourcePosts
+ same project/location/property_type/layout/size/floor
+ no strong unit/media identity
-> different property_identity_key
-> different listing_id
```

Cross-source automatic merging based on media overlap or strong unit identity belongs to the later Phase 4 `listing/dedupe.py` work and is not implemented here.

## Canonical deal-type and price contract

Routing:

```text
rent evidence only -> rent
sale evidence only -> sale
rent + sale intent -> unknown + [rent, sale] + conflicting_deal_type
no reliable intent -> unknown
```

The parser preserves the locked money boundary:

- rent is extracted only from explicit rent/monthly contexts;
- sale price is extracted only from explicit sale contexts;
- deposit amounts do not become rent;
- utility rates do not become rent;
- sale prices do not become rent;
- conflicting rental values are not guessed.

Locked rental regressions covered include `$1,500/月`, `$850/月`, `租金520$包物业`, `特价出租600$`, `出租情况：850$`, and `7000美元每月`.

## Sale storage

Sale remains valid canonical real-estate data:

```text
sale source
-> canonical_records deal_type=sale
-> v3_listings
-> listing_offers offer_type=sale
-> publication_policy=store_only
-> publish_block_reason=sale_not_enabled_for_rent_channel
```

There is no V3 `skipped_non_rental` path.

## Safe enrichment and taxonomy

Safe enrichment remains additive and does not parse or override money. Stable rules include services, furniture/appliances, decoration, amenities, explicit included items, lease wording, pending project candidates and unlabelled dimensions.

Taxonomy remains conservative:

- inventory/menu property types do not manufacture the current property type;
- unapproved project tokens remain review candidates;
- roads/market concepts stay separate from physical canonical area;
- explicit project/brand/location relationships remain separate.

## Persistence path

`CanonicalParserService` consumes an immutable SourcePostRevision plus V3 SourcePost identity and writes `canonical_records` using the approved Phase 1 repository/schema.

`CanonicalListingMaterializer` consumes a resolved rent or sale CanonicalRecord and writes only:

```text
v3_listings
listing_sources
listing_offers
```

The Phase 3 resolved path is:

```text
SourcePostRevision
-> CanonicalParserService
-> canonical_records
-> CanonicalListingMaterializer
-> v3_listings
-> listing_offers
```

The unresolved conflict path is:

```text
SourcePostRevision
-> canonical_records (deal_type=unknown, conflicting_deal_type)
-> STOP before Listing / Offer materialization
```

No drafts or publication packages participate in either path.

## Public listing ID

V3 Public ID remains directly owned by `v3_listings.public_listing_id`.

Contract:

```text
QL-<location-code>-<A2B3-style code>
```

It is assigned once and remains stable for the Listing. Known location-aware codes are extracted from locked V2.2 behavior, with `PP` fallback.

Legacy `QC/QJ/L` normalization remains isolated in `qiaolian_v3/legacy/public_id_compat.py` and does not become V3 identity truth.

## DB schema

**No Phase 3 DB migration.**

The approved Phase 1 schema is sufficient to express all four corrected Phase 3 contracts.

## Tests

Current V3 tests directly cover:

- rent / sale / unknown routing;
- unresolved rent+sale -> unknown + candidates + `conflicting_deal_type`;
- removal of `ambiguous_deal_type` from formal deal review;
- unresolved unknown stops before Listing/Offer writes;
- sale -> store-only sale Offer;
- canonical business-hash stability across source/revision/media transport changes;
- canonical hash change on business-fact change;
- complex locked layouts and helper-room/office behavior;
- generic payment terms;
- available date and management/internet/water/electric/parking fields;
- viewing/video fields;
- land/building/primary/unlabelled size semantics;
- locked price regressions and price separation;
- safe enrichment;
- taxonomy conservatism;
- different SourcePosts with identical weak facts remain different Listings;
- same SourcePost later revision remains the same Listing;
- QL Public ID stability;
- no drafts table introduced;
- zero publication package creation.

## CI verification

### First remediation CI

- Run: `34226078385` (#657)
- Head: `d23e70095de2327ba09699de9c8d5e82ac43b98c`
- Syntax/import: PASS
- `tests/v3`: **93 passed, 1 failed**
- Failure: only the newly added compound-layout expectation exceeded locked V2.2 behavior.
- Production regression: skipped because the V3 step failed.
- Resolution: corrected the test to the exact locked V2.2 result; parser semantics were not changed.

### Corrected implementation CI

- Head: `5603b907ccb489fdff5ff5b00df043a4558ac686`
- Workflow: `qiaolian-ui-check`
- Run: `34226265498` (#658)
- Job: `102061036270`
- Diff whitespace: PASS
- Syntax/runtime import: PASS
- `tests/v3`: **94 passed in 0.46s**
- old production regression: **261 passed, 2 existing warnings in 4.41s**
- failures: **0**

The two warnings are the existing `python-telegram-bot ConversationHandler` warnings in the legacy production regression suite.

The repository workflow currently listens to PRs targeting `v3/phase0-baseline`. As in the independently accepted Phase 2 procedure, PR #43 is temporarily retargeted only to trigger CI, then restored to the formal Phase 2 PASS base. No workflow file is modified and no code is borrowed from the temporary base.

A final report-only CI must be green before Phase 3 stops.

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
- Phase 4 started: **NO**

## Stop condition

After the report-only CI is green and PR #43 is restored to base `v3/phase2-ingest` at Phase 2 PASS head `60e3fbe838acaeeebf0f27ec25f8901bcdb825de`, Phase 3 stops for the second independent review.

Do not merge, deploy, operate production systems, operate Telegram, or start Phase 4 until that independent Phase 3 review returns `PASS`.
