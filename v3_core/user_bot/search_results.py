"""Explicit search-result augmentation extracted from the production UX patch.

Production currently monkey-patches ``results_admin.send_find_results_as_cards``
when a strict search returns exactly one listing.  V3 keeps that behavior as a
pure rule: strict result first, then real unique similar listings, capped at 5.
No database search or Telegram send happens in this module.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class SearchResultSet:
    items: tuple[dict[str, Any], ...]
    has_similar: bool


def augment_strict_single_result(
    matches: Iterable[dict[str, Any]] | None,
    similar: Iterable[dict[str, Any]] | None,
    *,
    match_mode: str = "strict",
    limit: int = 5,
) -> SearchResultSet:
    """Preserve production UX without runtime monkey patching."""
    items = [dict(item) for item in (matches or [])]
    if len(items) != 1 or str(match_mode or "strict") != "strict":
        return SearchResultSet(items=tuple(items), has_similar=False)

    cap = max(1, int(limit or 5))
    seen = {
        str(item.get("listing_id") or "").strip()
        for item in items
        if str(item.get("listing_id") or "").strip()
    }
    for raw in similar or []:
        item = dict(raw)
        listing_id = str(item.get("listing_id") or "").strip()
        if not listing_id or listing_id in seen:
            continue
        items.append(item)
        seen.add(listing_id)
        if len(items) >= cap:
            break

    return SearchResultSet(items=tuple(items), has_similar=len(items) > 1)


__all__ = ["SearchResultSet", "augment_strict_single_result"]
