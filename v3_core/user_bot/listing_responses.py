"""Telegram-neutral public listing responses for the V3 User Bot.

The renderer preserves the locked production details/photos presentation while
consuming only a durably published V3 view. Telegram adapters later translate
semantic actions/media groups into framework objects; they do not re-decide
business state.
"""
from __future__ import annotations

from dataclasses import dataclass
from html import escape as he
from pathlib import Path
from typing import Literal
import re

from v3_core.publishing.formatting import display_floor

from .adviser_notes import adviser_notes_for_view
from .listing_presenter import build_public_listing_details
from .public_inventory import PublishedListingView


InternalListingAction = Literal[
    "details",
    "photos",
    "book",
    "consult",
    "similar",
    "previous",
    "next",
    "change_search",
]

_INTERNAL_ID = re.compile(
    r"(?i)\b(?:lst_[0-9a-z]+|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b"
)


@dataclass(frozen=True)
class SemanticAction:
    label: str
    action: InternalListingAction
    target_public_listing_id: str = ""
    target_index: int | None = None


@dataclass(frozen=True)
class PublicDetailsResponse:
    text: str
    action_rows: tuple[tuple[SemanticAction, ...], ...]


@dataclass(frozen=True)
class PublicPhotosResponse:
    media_groups: tuple[tuple[str, ...], ...]
    text: str
    action_rows: tuple[tuple[SemanticAction, ...], ...]

    @property
    def has_media(self) -> bool:
        return bool(self.media_groups)


def _format_price(value: int | None) -> str:
    return f"${int(value):,}/\u6708" if value is not None and int(value) > 0 else ""


def _format_size(value: float | None) -> str:
    if value is None or value <= 0:
        return ""
    numeric = int(value) if float(value).is_integer() else value
    return f"{numeric}\u33a1"


def _safe_line(text: str) -> str:
    clean = str(text or "").strip()
    if not clean or _INTERNAL_ID.search(clean):
        return ""
    return clean


def _details_actions(
    *,
    bookable: bool,
    public_listing_id: str,
) -> tuple[tuple[SemanticAction, ...], ...]:
    target = str(public_listing_id or "").strip()
    if bookable:
        return (
            (
                SemanticAction("\ud83d\udcc5 \u9884\u7ea6\u770b\u623f", "book", target),
                SemanticAction("\ud83d\udcf8 \u66f4\u591a\u5b9e\u62cd", "photos", target),
            ),
            (SemanticAction("\ud83d\udcac \u8054\u7cfb\u4e2d\u6587\u987e\u95ee", "consult", target),),
        )
    return (
        (
            SemanticAction("\ud83d\udcf8 \u66f4\u591a\u5b9e\u62cd", "photos", target),
            SemanticAction("\ud83d\udcac \u8054\u7cfb\u4e2d\u6587\u987e\u95ee", "consult", target),
        ),
        (SemanticAction("\ud83c\udee9 \u770b\u76f8\u8fd1\u623f\u6e90", "similar", target),),
    )


def _append(lines: list[str], value: str) -> None:
    clean = _safe_line(value)
    if clean:
        lines.append(clean)


def build_details_response(view: PublishedListingView) -> PublicDetailsResponse:
    details = build_public_listing_details(view)
    price = _format_price(details.monthly_rent_usd)
    size = _format_size(details.size_sqm)
    floor = display_floor(details.floor)

    lines: list[str] = ["\ud83c\udfe0 <b>\u79df\u8d41\u8be6\u60c5</b>", ""]
    if details.subject:
        _append(lines, f"\ud83c\udfe0 <b>{he(details.subject)}</b>")
    elif details.location:
        _append(lines, f"\ud83c\udfe0 <b>{he(details.location)}</b>")
    if details.location and details.location != details.project_name:
        _append(lines, f"<b>\u533a\u57df\uff1a</b> {he(details.location)}")
    if details.property_type and details.property_type not in (details.layout, details.subject):
        _append(lines, f"\ud83c\udff7 \u7c7b\u578b\uff1a{he(details.property_type)}")
    if price:
        _append(lines, f"<b>\u79df\u91d1\uff1a</b> <b>{he(price)}</b>")
    if size:
        _append(lines, f"\ud83d\udcd0 \u9762\u79ef\uff1a{he(size)}")
    if floor:
        _append(lines, f"\ud83c\udfe2 \u697c\u5c42\uff1a{he(floor)}")
    if details.lease_summary:
        _append(lines, f"\ud83d\udd11 \u79df\u7ea6\uff1a{he(details.lease_summary)}")
    for fee in details.fee_lines:
        _append(lines, f"\ud83d\udcb8 {he(fee)}")
    if details.amenity_line:
        _append(lines, f"\ud83e\udde3 \u914d\u5957\uff1a{he(details.amenity_line)}")
    if details.highlight_line:
        _append(lines, f"\u2728 \u4eae\u70b9\uff1a{he(details.highlight_line)}")
    _append(lines, f"{details.status_icon} \u623f\u6001\uff1a{he(details.status_label)}")
    if details.public_listing_id:
        _append(lines, f"\ud83c\udd94 \u623f\u6e90\u7f16\u53f7\uff1a{he(details.public_listing_id)}")

    notes = adviser_notes_for_view(view, max_points=2, allow_empty=True).strip()
    if notes:
        safe_notes = "\n".join(he(line) for line in notes.splitlines() if line.strip())
        if safe_notes:
            lines.extend(["", "\ud83d\udcac <b>\u4fa8\u8054\u8bf4</b>", "", safe_notes])

    text = "\n".join(lines).rstrip()
    if details.listing_id and details.listing_id in text:
        text = text.replace(details.listing_id, "")
    text = _INTERNAL_ID.sub("", text)

    return PublicDetailsResponse(
        text=text,
        action_rows=_details_actions(
            bookable=details.bookable,
            public_listing_id=details.public_listing_id,
        ),
    )


def _existing_gallery(paths: tuple[str, ...]) -> tuple[str, ...]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in paths:
        path = str(raw or "").strip()
        if not path or path in seen:
            continue
        if Path(path).name.lower() in {"cover.jpg", "cover.jpeg", "cover.png"}:
            continue
        if not Path(path).is_file():
            continue
        output.append(path)
        seen.add(path)
    return tuple(output)


def build_photos_response(view: PublishedListingView) -> PublicPhotosResponse:
    from .listing_photos_panel import build_photos_end_actions, build_photos_end_text

    details = build_public_listing_details(view)
    photos = _existing_gallery(details.gallery)
    groups = tuple(tuple(photos[offset : offset + 10]) for offset in range(0, len(photos), 10))
    return PublicPhotosResponse(
        media_groups=groups,
        text=build_photos_end_text(
            has_media=bool(groups),
            bookable=details.bookable,
            public_listing_id=details.public_listing_id,
        ),
        action_rows=build_photos_end_actions(
            bookable=details.bookable,
            public_listing_id=details.public_listing_id,
        ),
    )


__all__ = [
    "InternalListingAction",
    "PublicDetailsResponse",
    "PublicPhotosResponse",
    "SemanticAction",
    "build_details_response",
    "build_photos_response",
]
