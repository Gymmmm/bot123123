"""Production adapter for the additive V2 SAFE enrichment layer.

The authoritative parser output is immutable: this adapter delegates to
``parser_v2_safe.enrich_v2`` and verifies that every pre-existing non-empty
business value is preserved. It owns no parsing rules.

Production canonical facts are hash-bound. Additive enrichment changes the
serialized payload, so after preservation is verified we refresh only the
integrity hash; no parsed business fact is replaced.
"""
from __future__ import annotations

from typing import Any

from parser_v2_safe import assert_v1_1_preserved, enrich_v2
from qiaolian_dual.canonical_fact_projection import facts_hash


def enrich_authoritative_facts(raw_text: str, authoritative: dict[str, Any]) -> dict[str, Any]:
    """Return authoritative facts plus additive V2 SAFE enrichment.

    ``enrich_v2`` deep-copies its input. Preservation is asserted before the
    canonical integrity hash is refreshed for the expanded payload.
    """
    enriched = enrich_v2(str(raw_text or ""), authoritative)
    assert_v1_1_preserved(authoritative, enriched)
    if authoritative.get("canonical_facts_hash"):
        enriched["canonical_facts_hash"] = facts_hash(enriched)
    return enriched


__all__ = ["enrich_authoritative_facts"]
