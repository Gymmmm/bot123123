"""Legacy listing reference normalization without making legacy IDs V3 truth."""
from __future__ import annotations

import re

LEGACY_RE = re.compile(r"^(?:QC|QJ)[_-]?(\d+)$", re.I)
INTERNAL_RE = re.compile(r"^L[_-]?(\d+)$", re.I)


def normalize_legacy_listing_reference(value: object) -> str | None:
    raw = str(value or "").strip()
    match = LEGACY_RE.fullmatch(raw) or INTERNAL_RE.fullmatch(raw)
    return f"l_{int(match.group(1))}" if match else None


__all__ = ["normalize_legacy_listing_reference"]
