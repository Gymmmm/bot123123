"""Read-only adapter for Publisher-authoritative adviser copy.

Publisher freezes the final public text in the publication snapshot. The User
Bot must never infer, regenerate, polish, or fall back to listing facts.
"""
from __future__ import annotations

from .public_inventory import PublishedListingView


def adviser_notes_for_view(
    view: PublishedListingView,
    *,
    max_points: int = 2,
    allow_empty: bool = True,
) -> str:
    """Return Publisher's frozen copy verbatim, or an empty string.

    ``max_points`` and ``allow_empty`` remain accepted only for call-site
    compatibility. They intentionally have no effect on frozen Publisher text.
    """
    del max_points, allow_empty
    snapshot = view.snapshot
    source = str(snapshot.get("adviser_copy_source") or "").strip().lower()
    if source == "hidden":
        return ""
    return str(snapshot.get("adviser_copy") or "")


__all__ = ["adviser_notes_for_view"]
