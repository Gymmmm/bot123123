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
    return f"${int(value):,}/月" if value is not None and int(value) > 0 else ""


def _format_size(value: float | None) -> str:
    if value is None or value <= 0:
        return ""
    numeric = int(value) if float(value).is_integer() else value
    return f"{numeric}㎡"


def _details_actions(
    *,
    bookable: bool,
    public_listing_id: str,
) -> tuple[tuple[SemanticAction, ...], ...]:
    target = str(public_listing_id or "").strip()
    if bookable:
        return (
            (
                SemanticAction("📅 预约看房", "book", target),
                SemanticAction("📸 更多实拍", "photos", target),
            ),
            (SemanticAction("💬 联系我们", "consult", target),),
        )
    return (
        (
            SemanticAction("📸 更多实拍", "photos", target),
            SemanticAction("💬 联系我们", "consult", target),
        ),
        (SemanticAction("🏘 看相近房源", "similar", target),),
    )


def _photo_actions(
    *,
    bookable: bool,
    public_listing_id: str,
) -> tuple[tuple[SemanticAction, ...], ...]:
    target = str(public_listing_id or "").strip()
    first = [SemanticAction("🏠 房源详情", "details", target)]
    if bookable:
        first.append(SemanticAction("📅 预约看房", "book", target))
    return (
        tuple(first),
        (SemanticAction("💬 联系我们", "consult", target),),
    )


def build_details_response(view: PublishedListingView) -> PublicDetailsResponse:
    details = build_public_listing_details(view)
    price = _format_price(details.monthly_rent_usd)
    size = _format_size(details.size_sqm)
    floor = display_floor(details.floor)

    lines = ["🏠 <b>房源详情</b>", ""]
    if details.subject:
        lines.append(f"🏠 <b>{he(details.subject)}</b>")
    elif details.location:
        lines.append(f"🏠 <b>{he(details.location)}</b>")
    if details.location and details.location != details.project_name:
        lines.append(f"<b>区域：</b> {he(details.location)}")
    if price:
        lines.append(f"<b>租金：</b> <b>{he(price)}</b>")
    if size:
        lines.append(f"📐 面积：{he(size)}")
    if floor:
        lines.append(f"🏢 楼层：{he(floor)}")
    if details.lease_summary:
        lines.append(f"🔑 租约：{he(details.lease_summary)}")
    lines.append(f"{details.status_icon} 房态：{he(details.status_label)}")
    if details.public_listing_id:
        lines.append(f"📸 实拍：{he(details.public_listing_id)}")

    notes = adviser_notes_for_view(view, max_points=2, allow_empty=True).strip()
    if notes:
        safe_notes = "\n".join(he(line) for line in notes.splitlines() if line.strip())
        lines.extend(["", "💬 <b>侨联说</b>", "", safe_notes])

    return PublicDetailsResponse(
        text="\n".join(lines),
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
    details = build_public_listing_details(view)
    photos = _existing_gallery(details.gallery)
    groups = tuple(tuple(photos[offset : offset + 10]) for offset in range(0, len(photos), 10))
    if groups:
        text = (
            "📸 <b>以上是这套房目前保存的现场实拍。</b>\n\n"
            "想进一步了解，可以继续看详情，或直接预约。"
        )
    else:
        text = (
            f"📸 <b>更多实拍｜{he(details.public_listing_id)}</b>\n\n"
            "这套房的实拍暂时没有加载出来。\n\n"
            "可以稍后再试，或直接联系我们。"
        )
    return PublicPhotosResponse(
        media_groups=groups,
        text=text,
        action_rows=_photo_actions(
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
