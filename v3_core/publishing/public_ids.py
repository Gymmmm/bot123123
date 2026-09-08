"""Pure public listing-id rules extracted from qiaolian_dual.public_listing_id.

V3 keeps identifier syntax separate from database lookup/generation.  Storage
repositories may assign IDs, but channel rendering only consumes normalized
public IDs and never queries legacy drafts/listings to discover one.
"""
from __future__ import annotations

import re
import secrets

LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ"
DIGITS = "23456789"
RANDOM_RE = r"[A-HJ-NP-Z][2-9][A-HJ-NP-Z][2-9]"
NEW_PUBLIC_RE = re.compile(rf"^QL-([A-HJ-NP-Z2-9]{{2,4}})-({RANDOM_RE})$", re.I)
LEGACY_PUBLIC_RE = re.compile(r"^QL-([A-HJ-NP-Z2-9]{6})$", re.I)
LEGACY_ALTERNATING_RE = re.compile(r"^QL-([A-HJ-NP-Z][2-9]){3}$", re.I)
LEGACY_RE = re.compile(r"^(?:QC|QJ)[_-]?(\d+)$", re.I)
INTERNAL_RE = re.compile(r"^L[_-]?(\d+)$", re.I)

KNOWN_LOCATION_CODES = frozenset(
    {
        "RF",
        "BK",
        "B2",
        "B3",
        "DD",
        "AE",
        "A2",
        "TK",
        "SS",
        "HS",
        "CH",
        "PP",
    }
)


def normalize_public_id(value: object) -> str | None:
    raw = re.sub(r"\s+", "", str(value or "")).upper()
    if not raw.startswith("QL"):
        return None
    tail = raw[2:].lstrip("-")
    if not tail:
        return None
    with_hyphens = "QL-" + tail
    if NEW_PUBLIC_RE.fullmatch(with_hyphens):
        return with_hyphens
    compact = tail.replace("-", "")
    for location in sorted(KNOWN_LOCATION_CODES, key=len, reverse=True):
        if compact.startswith(location) and re.fullmatch(
            RANDOM_RE, compact[len(location) :]
        ):
            return f"QL-{location}-{compact[len(location):]}"
    legacy = "QL-" + compact
    if LEGACY_ALTERNATING_RE.fullmatch(legacy):
        return legacy
    if 6 <= len(compact) <= 8:
        location, random_code = compact[:-4], compact[-4:]
        candidate = f"QL-{location}-{random_code}"
        if NEW_PUBLIC_RE.fullmatch(candidate):
            return candidate
    if LEGACY_PUBLIC_RE.fullmatch(legacy):
        return legacy
    return None


def legacy_to_internal(value: object) -> str | None:
    raw = str(value or "").strip()
    match = LEGACY_RE.fullmatch(raw) or INTERNAL_RE.fullmatch(raw)
    return f"l_{int(match.group(1))}" if match else None


def generate_public_id(location_code: str = "PP") -> str:
    code = re.sub(r"[^A-Z2-9]", "", str(location_code or "PP").upper())
    if not 2 <= len(code) <= 4:
        code = "PP"
    random_code = "".join(
        (
            secrets.choice(LETTERS),
            secrets.choice(DIGITS),
            secrets.choice(LETTERS),
            secrets.choice(DIGITS),
        )
    )
    return f"QL-{code}-{random_code}"


__all__ = [
    "generate_public_id",
    "legacy_to_internal",
    "normalize_public_id",
]
