from __future__ import annotations

from enum import Enum
from typing import Iterable


class DedupeDecision(str, Enum):
    NEW = 'NEW'
    UPDATE = 'UPDATE'
    DUPLICATE = 'DUPLICATE'
    REVIEW = 'REVIEW'


def _normalized(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(str(v) for v in values if str(v))


def _strong_media_overlap(current: tuple[str, ...], existing: tuple[str, ...]) -> bool:
    a, b = set(current), set(existing)
    if not a or not b:
        return False
    shared = len(a & b)
    return shared >= 3 and shared / len(a) >= 0.75 and shared / len(b) >= 0.75


def classify_listing_candidate(
    *,
    source_post_id: int,
    existing_source_post_id: int | None,
    canonical_hash: str,
    existing_canonical_hash: str | None,
    media_identities: Iterable[str] = (),
    existing_media_identities: Iterable[str] = (),
    unit_identity: str | None = None,
    existing_unit_identity: str | None = None,
) -> DedupeDecision:
    """Conservative listing relation classifier.

    Same SourcePost revisions remain one listing. Different SourcePosts never
    auto-merge from weak semantic similarity alone; only strong unit identity or
    strong media overlap can establish the same physical listing in Phase 4.
    """
    if existing_source_post_id is None:
        return DedupeDecision.NEW

    current_media = _normalized(media_identities)
    existing_media = _normalized(existing_media_identities)
    if int(source_post_id) == int(existing_source_post_id):
        if str(canonical_hash) == str(existing_canonical_hash or '') and current_media == existing_media:
            return DedupeDecision.DUPLICATE
        return DedupeDecision.UPDATE

    strong_unit = bool(unit_identity and existing_unit_identity and str(unit_identity) == str(existing_unit_identity))
    strong_media = _strong_media_overlap(current_media, existing_media)
    if not (strong_unit or strong_media):
        return DedupeDecision.REVIEW

    if str(canonical_hash) == str(existing_canonical_hash or '') and set(current_media) == set(existing_media):
        return DedupeDecision.DUPLICATE
    return DedupeDecision.UPDATE


__all__ = ['DedupeDecision', 'classify_listing_candidate']
