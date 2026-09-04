"""Production adapter for the additive V2 SAFE enrichment layer.

The authoritative parser output is immutable: this adapter only delegates to
``parser_v2_safe.enrich_v2`` and verifies that every pre-existing non-empty
value is preserved.  It deliberately owns no parsing rules of its own.
"""
from __future__ import annotations

from typing import Any

from parser_v2_safe import assert_v1_1_preserved, enrich_v2


def enrich_authoritative_facts(raw_text: str, authoritative: dict[str, Any]) -> dict[str, Any]:
    """Return authoritative facts plus additive V2 SAFE enrichment.

    ``enrich_v2`` deep-copies its input.  The explicit preservation assertion
    here is intentional defense-in-depth for the production wiring contract.
    """
    enriched = enrich_v2(str(raw_text or ""), authoritative)
    assert_v1_1_preserved(authoritative, enriched)
    return enriched


__all__ = ["enrich_authoritative_facts"]
