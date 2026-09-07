"""Stable external-post identity and revision-content hashing for V3 ingest."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable


def build_single_external_post_id(message_id: int | str) -> str:
    return str(message_id)


def build_album_external_post_id(*, grouped_id: int | str | None, anchor_message_id: int | str) -> str:
    """Keep the locked V2.2 album identity rule: grouped id, anchor fallback."""
    anchor = str(anchor_message_id)
    if grouped_id is None:
        return f"album_{anchor}"
    return f"album_{grouped_id}"


def make_source_content_hash(sanitized_text: str, media_hashes: Iterable[str]) -> str:
    """Hash semantic source evidence, not fetch timestamps or source contacts.

    Media identity is normalized as a sorted multiset so transport/order-only
    differences do not manufacture a new SourcePostRevision.
    """
    payload = {
        "sanitized_text": str(sanitized_text or ""),
        "media_hashes": sorted(str(value) for value in media_hashes if str(value)),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
