from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ListingActionPolicy:
    details: bool
    photos: bool
    book: bool
    contact: bool


def action_policy(listing_status: str) -> ListingActionPolicy:
    status = str(listing_status or '').strip()
    if status == 'active':
        return ListingActionPolicy(True, True, True, True)
    if status in {'pending', 'rented', 'inactive'}:
        return ListingActionPolicy(True, True, False, True)
    return ListingActionPolicy(False, False, False, False)


__all__ = ['ListingActionPolicy', 'action_policy']
