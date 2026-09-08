"""Authoritative canonical enrichment stage.

This is the V3 replacement for ``qiaolian_dual.v2_safe_adapter``.  The SAFE
rules remain unchanged; this adapter only enforces preservation and refreshes
the canonical integrity hash after additive enrichment.
"""
from __future__ import annotations

from typing import Any

from v3_core.inventory.hashing import facts_hash
from .safe_enrichment import assert_v1_1_preserved, enrich_v2


def enrich_authoritative_facts(
    raw_text: str, authoritative: dict[str, Any]
) -> dict[str, Any]:
    enriched = enrich_v2(str(raw_text or ""), authoritative)
    assert_v1_1_preserved(authoritative, enriched)
    if authoritative.get("canonical_facts_hash"):
        enriched["canonical_facts_hash"] = facts_hash(enriched)
    return enriched


__all__ = ["enrich_authoritative_facts"]
