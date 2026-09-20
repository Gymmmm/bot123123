"""Telegram-neutral public listing responses for the V3 User Bot.

Merged listing view: one entry opens a single-photo flipper (上一张 / 下一张)
with a SHORT photo caption. Full sectioned detail is available as a separate
text payload — never dumped onto the photo caption (keeps image + buttons close).
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
    listing_summary: str = ""


@dataclass(frozen=True)
class PublicPhotosResponse:
    """Single-photo flipper payload (short caption + one current frame)."""

    media_groups: tuple[tuple[str, ...], ...]
    text: str
    action_rows: tuple[tuple[SemanticAction, ...], ...]
    listing_summary: str = ""
    photo_path: str = ""
    photo_index: int = 0
    photo_total: int = 0
    detail_text: str = ""

    @property
    def has_media(self) -> bool:
        return bool(self.photo_path) or bool(self.media_groups)


def _format_price(value: int | None) -> str:
    return f"${int(value):,}/月" if value is not None and int(value) > 0 else ""


def listing_summary_bits(
    *,
    project_name: object = "",
    layout: object = "",
    monthly_rent_usd: int | None = None,
    location: object = "",
) -> str:
    """Compact identity line for advisor handoff / consult prefill."""
    return "｜".join(
        part
        for part in (
            str(project_name or "").strip(),
            str(layout or "").strip(),
            _format_price(monthly_rent_usd),
            str(location or "").strip(),
        )
        if part
    )


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
    """Text-only fallback actions (no separate 更多实拍 — merged into primary CTA)."""
    target = str(public_listing_id or "").strip()
    if bookable:
        return (
            (
                SemanticAction("📅 预约看房", "book", target),
                SemanticAction("💬 中文顾问", "consult", target),
            ),
            (SemanticAction("🔍 看相近房源", "similar", target),),
        )
    return (
        (SemanticAction("💬 中文顾问", "consult", target),),
        (SemanticAction("🔍 看相近房源", "similar", target),),
    )


def _photo_actions(
    *,
    bookable: bool,
    public_listing_id: str,
    photo_index: int = 0,
    photo_total: int = 0,
) -> tuple[tuple[SemanticAction, ...], ...]:
    target = str(public_listing_id or "").strip()
    rows: list[tuple[SemanticAction, ...]] = []
    total = max(0, int(photo_total or 0))
    index = max(0, int(photo_index or 0))
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            (
                SemanticAction(
                    "⬅️ 上一张",
                    "photos",
                    target,
                    target_index=prev_index,
                ),
                SemanticAction(
                    "下一张 ➡️",
                    "photos",
                    target,
                    target_index=next_index,
                ),
            )
        )
    if bookable:
        rows.append(
            (
                SemanticAction("📅 预约看房", "book", target),
                SemanticAction("💬 中文顾问", "consult", target),
            )
        )
    else:
        rows.append((SemanticAction("💬 中文顾问", "consult", target),))
    rows.append((SemanticAction("🔍 看相近房源", "similar", target),))
    return tuple(rows)


_DETAIL_DIVIDER = "━━━━━━━━━━━━━━━"
_DETAIL_TITLE = "🏢 金边优质房源出租"


def _bullet(label: str, value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return f"・{label}：{he(text)}"


def _section(title: str, bullets: list[str]) -> list[str]:
    if not bullets:
        return []
    return [title, *bullets]


def _utilities_line(*, water: str, electric: str) -> str:
    parts: list[str] = []
    if water:
        parts.append(f"水 {water}")
    if electric:
        parts.append(f"电 {electric}")
    return " / ".join(parts)


def _project_area(details) -> str:
    project = str(details.project_name or "").strip()
    location = str(details.location or "").strip()
    if project and location and project != location:
        return f"{project} · {location}"
    return project or location


def _detail_rent_line(value: int | None) -> str:
    if value is None or int(value) <= 0:
        return ""
    return f"${int(value):,} / 月"


def _detail_status_line(details) -> str:
    if details.bookable:
        return "🟢 房源状态：随时可预约看房"
    status = str(details.inventory_status or "").strip().lower()
    if status in {"rented", "leased"}:
        return "🔴 房源状态：已租出"
    if status in {"inactive", "offline"}:
        return "⚫ 房源状态：已下架"
    label = str(details.status_label or "").strip() or "待确认"
    return f"{details.status_icon} 房源状态：{he(label)}"


def _adviser_copy_for_view(view: PublishedListingView) -> str:
    details = build_public_listing_details(view)
    frozen = str(details.adviser_copy or "").strip()
    if frozen:
        return frozen
    return adviser_notes_for_view(view, max_points=2, allow_empty=True).strip()


def build_detail_text(view: PublishedListingView) -> str:
    """Full sectioned listing detail (separate text message; NOT photo caption)."""
    details = build_public_listing_details(view)
    floor = display_floor(details.floor)
    size = _format_size(details.size_sqm)
    rent = _detail_rent_line(details.monthly_rent_usd)
    utilities = _utilities_line(
        water=str(details.water_rate or "").strip(),
        electric=str(details.electric_rate or "").strip(),
    )

    lines: list[str] = [_DETAIL_DIVIDER, _DETAIL_TITLE, _DETAIL_DIVIDER]

    basic_header = "📌基本信息"
    if details.public_listing_id:
        basic_header = f"📌基本信息  房源编号：{he(details.public_listing_id)}"
    basic_bullets = [
        bullet
        for bullet in (
            _bullet("项目区域", _project_area(details)),
            _bullet("户型格局", details.layout),
            _bullet("楼层类型", floor),
            _bullet("房屋面积", size),
        )
        if bullet
    ]
    if details.public_listing_id or basic_bullets:
        section = _section(basic_header, basic_bullets)
        lines.extend(section if section else [basic_header])

    rent_bullets = [
        bullet
        for bullet in (
            _bullet("月租金额", rent),
            _bullet("押付方式", details.deposit_terms),
            _bullet("起租租期", details.contract_term),
        )
        if bullet
    ]
    lines.extend(_section("💰 租金与付款", rent_bullets))

    fee_bullets = [
        bullet
        for bullet in (
            _bullet("物业管理", details.management_fee),
            _bullet("水电费用", utilities),
            _bullet("大楼配套", details.building_amenities),
        )
        if bullet
    ]
    lines.extend(_section("🧾 费用与配套", fee_bullets))

    notes = _adviser_copy_for_view(view)
    if notes:
        safe_notes = "\n".join(he(line) for line in notes.splitlines() if line.strip())
        lines.extend(["", "💬 侨联说", safe_notes])

    lines.extend(["", _detail_status_line(details)])
    return "\n".join(lines).strip()


def build_photo_caption(
    view: PublishedListingView,
    *,
    photo_index: int = 0,
    photo_total: int = 0,
) -> str:
    """SHORT photo caption: project · layout · rent · 📸 N/M."""
    details = build_public_listing_details(view)
    project = str(details.project_name or "").strip()
    layout = str(details.layout or "").strip()
    rent = _format_price(details.monthly_rent_usd)
    head = " · ".join(part for part in (project, layout, rent) if part)
    if not head:
        head = str(details.location or details.public_listing_id or "房源").strip()
    total = max(0, int(photo_total or 0))
    if total > 0:
        index = max(0, int(photo_index or 0)) % total
        return f"{he(head)} · 📸 {index + 1}/{total}"
    return he(head)


def build_details_response(view: PublishedListingView) -> PublicDetailsResponse:
    details = build_public_listing_details(view)
    return PublicDetailsResponse(
        text=build_detail_text(view),
        listing_summary=listing_summary_bits(
            project_name=details.project_name,
            layout=details.layout,
            monthly_rent_usd=details.monthly_rent_usd,
            location=details.location,
        ),
        action_rows=_details_actions(
            bookable=details.bookable,
            public_listing_id=details.public_listing_id,
        ),
    )


def _frozen_cover_path(view: PublishedListingView) -> str:
    """Resolve the listing cover file for flipper index 1/N."""
    package = getattr(view, "package", {}) or {}
    candidate = str(package.get("cover_path") or "").strip()
    if candidate and Path(candidate).is_file():
        return candidate
    # Fall back to a gallery asset named cover.* if present.
    for raw in getattr(view, "gallery", ()) or ():
        path = str(raw or "").strip()
        if not path:
            continue
        if Path(path).name.lower() in {"cover.jpg", "cover.jpeg", "cover.png"}:
            if Path(path).is_file():
                return path
    return ""


def _flipper_photo_paths(view: PublishedListingView) -> tuple[str, ...]:
    """Cover first, then remaining real/gallery photos (deduped).

    Product lock: 📷 房源详情 opens at 📸 1/N = cover, then gallery shots.
    """
    output: list[str] = []
    seen: set[str] = set()

    cover = _frozen_cover_path(view)
    if cover:
        output.append(cover)
        seen.add(cover)

    for raw in getattr(view, "gallery", ()) or ():
        path = str(raw or "").strip()
        if not path or path in seen:
            continue
        # Skip cover-named assets once cover is already first.
        if cover and Path(path).name.lower() in {"cover.jpg", "cover.jpeg", "cover.png"}:
            continue
        if not Path(path).is_file():
            continue
        output.append(path)
        seen.add(path)
    return tuple(output)


def build_photos_response(
    view: PublishedListingView,
    *,
    offset: int = 0,
    page_size: int | None = None,
) -> PublicPhotosResponse:
    """Merged one-photo flipper with SHORT caption (上一张 / 下一张).

    Photo 1/N is always the listing cover when available; gallery follows.
    ``page_size`` is ignored (kept for call-site compatibility).
    """
    del page_size
    details = build_public_listing_details(view)
    photos = _flipper_photo_paths(view)
    total = len(photos)
    if total:
        index = max(0, int(offset or 0)) % total
        current = photos[index]
        groups = ((current,),)
    else:
        index = 0
        current = ""
        groups = ()

    return PublicPhotosResponse(
        media_groups=groups,
        text=build_photo_caption(view, photo_index=index, photo_total=total),
        detail_text=build_detail_text(view),
        photo_path=current,
        photo_index=index,
        photo_total=total,
        listing_summary=listing_summary_bits(
            project_name=details.project_name,
            layout=details.layout,
            monthly_rent_usd=details.monthly_rent_usd,
            location=details.location,
        ),
        action_rows=_photo_actions(
            bookable=details.bookable,
            public_listing_id=details.public_listing_id,
            photo_index=index,
            photo_total=total,
        ),
    )


__all__ = [
    "InternalListingAction",
    "PublicDetailsResponse",
    "PublicPhotosResponse",
    "SemanticAction",
    "build_detail_text",
    "build_details_response",
    "build_photo_caption",
    "build_photos_response",
    "listing_summary_bits",
]
