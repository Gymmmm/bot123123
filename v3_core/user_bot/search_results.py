"""Explicit search-result augmentation extracted from the production UX patch.

Production monkey-patches ``results_admin.send_find_results_as_cards`` when a
strict search returns a single listing. V3 keeps that behavior as a pure rule:
strict result first, then real unique similar listings, capped at five.

The helper accepts either mapping-like legacy rows or V3 public view objects so
search policy stays separate from database and Telegram adapters.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class SearchResultSet:
    items: tuple[Any, ...]
    has_similar: bool


def _listing_id(item: object) -> str:
    if isinstance(item, Mapping):
        return str(item.get("listing_id") or "").strip()
    return str(getattr(item, "listing_id", "") or "").strip()


def _stable_item(item: Any) -> Any:
    # Preserve the old immutability guarantee for dict rows while leaving V3
    # frozen dataclass/view objects untouched.
    return dict(item) if isinstance(item, Mapping) else item


def augment_strict_single_result(
    matches: Iterable[Any] | None,
    similar: Iterable[Any] | None,
    *,
    match_mode: str = "strict",
    limit: int = 5,
) -> SearchResultSet:
    """Preserve production one-result carousel UX without widening no-match."""
    items = [_stable_item(item) for item in (matches or [])]
    if len(items) != 1 or str(match_mode or "strict") != "strict":
        return SearchResultSet(items=tuple(items), has_similar=False)

    cap = max(1, int(limit or 5))
    seen = {value for value in (_listing_id(item) for item in items) if value}
    for raw in similar or []:
        item = _stable_item(raw)
        listing_id = _listing_id(item)
        if not listing_id or listing_id in seen:
            continue
        items.append(item)
        seen.add(listing_id)
        if len(items) >= cap:
            break

    return SearchResultSet(items=tuple(items), has_similar=len(items) > 1)


__all__ = ["SearchResultSet", "augment_strict_single_result"]
