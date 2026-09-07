"""Small public User Bot UX fixes that are safe to install at process start.

This module does not change lead attribution/admin flows.  It only keeps the
search carousel useful when a strict search returns a single listing.
"""
from __future__ import annotations


_INSTALLED = False


def install_user_ux_patch() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    from . import results_admin
    from .search import search_similar_listings

    original_send_results = results_admin.send_find_results_as_cards

    async def send_find_results_as_cards(update, context, matches: list[dict], match_mode: str = 'strict') -> None:
        """Keep previous/next navigation alive by appending real similar listings.

        Strict matches stay first.  We only append already-published, currently
        available listings returned by the existing public search layer, and we
        never fabricate listing data.
        """
        items = list(matches or [])
        if len(items) == 1 and str(match_mode or 'strict') == 'strict':
            pref = context.user_data.get('last_search_pref') or {}
            try:
                similar, _ = search_similar_listings(
                    property_type=pref.get('property_type'),
                    area=pref.get('area'),
                    budget_min=pref.get('budget_min'),
                    budget_max=pref.get('budget_max'),
                    limit=5,
                )
            except Exception:
                similar = []

            seen = {
                str(item.get('listing_id') or '').strip()
                for item in items
                if str(item.get('listing_id') or '').strip()
            }
            for item in similar:
                listing_id = str(item.get('listing_id') or '').strip()
                if not listing_id or listing_id in seen:
                    continue
                items.append(item)
                seen.add(listing_id)
                if len(items) >= 5:
                    break

            if len(items) > 1:
                context.user_data['find_card_has_similar'] = True

        await original_send_results(update, context, items, match_mode)

    results_admin.send_find_results_as_cards = send_find_results_as_cards
    _INSTALLED = True


__all__ = ['install_user_ux_patch']
