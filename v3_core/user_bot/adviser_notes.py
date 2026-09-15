"""Rental-detail adapter for the single Production V1 adviser engine.

Only frozen canonical evidence is consumed. Old frozen prose is not trusted:
it can contain removed management/internet, numeric-floor or fallback claims.
No live facts, filler, headings, emoji or bullets are produced here.
"""
from __future__ import annotations
from typing import Any

from v3_core.adviser_copy import generate_adviser_text, verified_canonical_adviser_facts
from .public_inventory import PublishedListingView


def generate_adviser_notes(listing: dict[str, Any], max_points: int = 2,
                           allow_empty: bool = True) -> str:
    """Compatibility adapter; never revive the previous parallel copy generator."""
    canonical = listing.get("canonical_facts")
    facts = canonical if isinstance(canonical, dict) else listing
    seed = str(listing.get("public_listing_id") or listing.get("listing_id") or "")
    return generate_adviser_text(verified_canonical_adviser_facts(facts), seed=seed,
                                 max_points=max_points, allow_fallback=False)


def frozen_adviser_evidence(view: PublishedListingView) -> dict[str, Any]:
    snapshot = view.snapshot
    if str(snapshot.get("schema") or "") != "v3_publication_snapshot.v1":
        raise ValueError("frozen_public_snapshot_missing")
    canonical = snapshot.get("canonical_facts")
    if not isinstance(canonical, dict):
        raise ValueError("frozen_canonical_facts_missing")
    return {"listing_id": str(snapshot.get("listing_id") or ""),
            "public_listing_id": str(snapshot.get("public_listing_id") or ""),
            "canonical_facts": dict(canonical)}


def adviser_notes_for_view(view: PublishedListingView, *, max_points: int = 2,
                           allow_empty: bool = True) -> str:
    return generate_adviser_notes(frozen_adviser_evidence(view), max_points=max_points)


__all__ = ["adviser_notes_for_view", "frozen_adviser_evidence", "generate_adviser_notes"]
