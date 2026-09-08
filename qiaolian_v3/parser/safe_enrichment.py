"""V3 safe enrichment entry point extracted from locked V2.2 rules."""
from __future__ import annotations

from typing import Any

from .safe_enrichment_v2 import enrich_v2


def enrich_safe(original_text: str, parsed_base: dict[str, Any]) -> dict[str, Any]:
    """Apply additive safe enrichment without reparsing monetary facts."""
    return enrich_v2(str(original_text or ""), parsed_base)


__all__ = ["enrich_safe"]
