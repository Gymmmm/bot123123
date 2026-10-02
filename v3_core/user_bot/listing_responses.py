"""Telegram-neutral public listing responses for the V3 User Bot.

Photos entry sends a native Telegram album (first-batch curated frames) plus a
separate status/action message. Remaining frames expand on demand — no Bot-side
上一张 / 下一张 flipper state.
"""
from __future__ import annotations

from dataclasses import dataclass
from html import escape as he
from pathlib import Path
from typing import Literal

from v3_core.publishing.formatting import display_floor
from v3_core.inventory.listing_taxonomy import location_display_overlaps_project

from .adviser_notes import adviser_notes_for_view
from .listing_presenter import build_public_listing_details
from .public_inventory import PublishedListingView


# First screen album size; remaining frames (if any) expand on demand.
# Preview selection count is property-aware (apartment 3 / villa 4).
PHOTOS_FIRST_BATCH = 4
PHOTOS_PAGE_SIZE = 4
# Hard cap for one listing photos view (first batch + expand).
PHOTOS_MAX_TOTAL = 10


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
    """Native album payload + separate status/action message.

    ``expand_only`` means the user already saw the first batch and only remaining
    frames should be sent (no second action keyboard).
    """

    media_groups: tuple[tuple[str, ...], ...]
    text: str
    action_rows: tuple[tuple[SemanticAction, ...], ...]
    listing_summary: str = ""
    media_caption: str = ""
    photo_path: str = ""
    photo_index: int = 0
    photo_total: int = 0
    detail_text: str = ""
    expand_only: bool = False

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
    """Details page actions: book/consult/similar — no photos button (photos is a separate deep link)."""
    target = str(public_listing_id or "").strip()
    if bookable:
        return (
            (
                SemanticAction("📅 预约看房", "book", target),
                SemanticAction("💬 咨询这套", "consult", target),
            ),
            (SemanticAction("🔍 找相似", "similar", target),),
        )
    return (
        (SemanticAction("💬 咨询这套", "consult", target),),
        (SemanticAction("🔍 找相似", "similar", target),),
    )


def _photo_actions(
    *,
    bookable: bool,
    inventory_status: str,
    public_listing_id: str,
    has_more: bool,
) -> tuple[tuple[SemanticAction, ...], ...]:
    target = str(public_listing_id or "").strip()
    status = str(inventory_status or "").strip().lower()
    first: list[SemanticAction] = []
    if has_more:
        first.append(SemanticAction("📷 查看全部实拍", "photos", target, target_index=1))
    else:
        first.append(SemanticAction("📷 房源详情", "details", target))
    if bookable:
        first.append(SemanticAction("📅 预约看房", "book", target))
    if status == "pending" and not bookable:
        first.append(SemanticAction("💬 中文顾问", "consult", target))
        return (tuple(first), (SemanticAction("⬅️ 返回房源", "details", target),))
    if bookable or status in {"active", "reserved"}:
        return (
            tuple(first),
            (SemanticAction("💬 咨询这套", "consult", target),),
            (SemanticAction("⬅️ 返回房源", "details", target),),
        )
    return (
        (SemanticAction("💬 咨询这套", "consult", target),),
        (SemanticAction("⬅️ 返回房源", "details", target),),
    )


def _utilities_line(*, water: str, electric: str) -> str:
    parts: list[str] = []
    if water:
        parts.append(f"水 {water}")
    if electric:
        parts.append(f"电 {electric}")
    return " / ".join(parts)


def _detail_status_line(details) -> str:
    status = str(details.inventory_status or "").strip().lower()
    if status == "reserved":
        return "🟡 已有预约，仍可预约"
    if status == "active":
        return "🟢 当前可预约"
    if status == "pending":
        return "🔵 房态待确认"
    if status in {"rented", "leased"}:
        return "🔴 已租出"
    if status in {"inactive", "offline"}:
        return "⚫ 已下架"
    label = str(details.status_label or "").strip() or "待确认"
    return f"{details.status_icon} {he(label)}"


def _adviser_copy_for_view(view: PublishedListingView) -> str:
    """Publisher-frozen copy only; respects adviser_copy_source=hidden."""
    return adviser_notes_for_view(view, max_points=2, allow_empty=True).strip()



def _listing_fact_lines(details) -> list[str]:
    """Consumer-facing listing facts plus live availability."""
    project = str(details.project_name or "").strip()
    location = str(details.location or "").strip()
    layout = str(details.layout or "").strip()
    property_type = str(details.property_type or "").strip()
    identity = project or location or property_type
    title = "｜".join(part for part in (identity, layout) if part) or "房源"

    lines: list[str] = [f"🏠 <b>{he(title)}</b>"]
    if details.monthly_rent_usd:
        lines.append(f"💰 {_format_price(details.monthly_rent_usd)}")

    if project and location and not location_display_overlaps_project(project, location):
        lines.append(f"📍 {he(location)}")

    size = _format_size(details.size_sqm)
    floor = display_floor(details.floor)
    meta = [part for part in (property_type, size, floor) if part]
    if meta:
        lines.append("🏢 " + "｜".join(he(part) for part in meta))

    terms = [he(part) for part in (details.deposit_terms, details.contract_term) if part]
    if terms:
        lines.append("🔑 " + "｜".join(terms))

    lines.append(_detail_status_line(details))
    # NOTE: public_id is NOT exposed in user-visible details text (privacy)
    return lines


def _adviser_lines(view: PublishedListingView, *, caption: bool) -> list[str]:
    notes = _adviser_copy_for_view(view)
    if not notes:
        return []
    if caption and len(notes) > 360:
        notes = notes[:359].rstrip() + "…"
    return ["", "💬 <b>侨联说</b>", *(he(line) for line in notes.splitlines())]


def build_detail_text(view: PublishedListingView) -> str:
    """Text fallback / details surface for listings."""
    details = build_public_listing_details(view)
    utilities = _utilities_line(
        water=str(details.water_rate or "").strip(),
        electric=str(details.electric_rate or "").strip(),
    )
    lines = _listing_fact_lines(details)
    for label, value in (
        ("物业费", details.management_fee),
        ("水电", utilities),
        ("配套", details.building_amenities),
    ):
        if value:
            lines.append(f"{label}｜{he(value)}")
    lines.extend(_adviser_lines(view, caption=False))
    return chr(10).join(lines).strip()

def build_photo_caption(
    view: PublishedListingView,
    *,
    photo_index: int = 0,
    photo_total: int = 0,
) -> str:
    """Legacy caption helper kept for detail-adjacent copy locks / tests."""
    details = build_public_listing_details(view)
    lines = _listing_fact_lines(details)
    adviser = _adviser_lines(view, caption=True)
    if len(chr(10).join([*lines, *adviser])) < 850:
        lines.extend(adviser)
    total = max(0, int(photo_total or 0))
    if total > 0:
        index = max(0, int(photo_index or 0)) % total
        lines.extend(["", f"📸 {index + 1}/{total}"])
    return chr(10).join(lines)


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


def _existing_file(path: object) -> str:
    """Return resolved path string when ``path`` points at a real file."""
    raw = str(path or "").strip()
    if not raw:
        return ""
    candidate = Path(raw).expanduser()
    try:
        if candidate.is_file():
            return str(candidate.resolve())
    except OSError:
        return ""
    return ""


def _cover_search_roots(view: PublishedListingView) -> list[Path]:
    """Infer media roots (covers_v3 / media) from package + gallery paths."""
    roots: list[Path] = []
    seen: set[str] = set()

    def _add(path: Path) -> None:
        try:
            resolved = path.expanduser().resolve()
        except OSError:
            return
        key = str(resolved)
        if key in seen:
            return
        seen.add(key)
        roots.append(resolved)

    package = getattr(view, "package", {}) or {}
    explicit = str(package.get("cover_path") or "").strip()
    if explicit:
        parent = Path(explicit).expanduser().parent
        _add(parent)
        _add(parent.parent)

    for raw in getattr(view, "gallery", ()) or ():
        gallery_path = Path(str(raw or "").strip())
        if not str(gallery_path):
            continue
        parent = gallery_path.expanduser().parent
        _add(parent)
        # prepared_v3/<id>/gallery -> prepared_v3/<id> -> media -> covers_v3
        for up in list(parent.parents)[:5]:
            _add(up)
            _add(up / "covers_v3")
            if up.name in {"gallery", "prepared_v3", "media"}:
                _add(up.parent / "covers_v3")
                _add(up.parent.parent / "covers_v3")
    return roots


def _looks_like_rendered_cover(path: Path, *, public_id: str, style: str) -> bool:
    name = path.name.lower()
    pid = str(public_id or "").strip().lower()
    if not pid or pid not in name:
        return False
    if path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp"}:
        return False
    if style and style.lower() in name:
        return True
    return name.startswith(pid + "_") or name.startswith(pid + ".")


def _frozen_cover_path(view: PublishedListingView) -> str:
    """Resolve the listing COVER RENDER file for album frame 1.

    Prefer the frozen package ``cover_path`` when the file exists. If that path is
    empty/stale at runtime (common when gallery derivatives are mounted but the
    recorded cover absolute path is not), rediscover the rendered cover under
    nearby ``covers_v3`` roots using public id + cover_style. Never fall back to
    a random gallery room shot here — gallery ordering is handled by the
    album builder after cover resolution.
    """
    package = getattr(view, "package", {}) or {}
    public_id = str(
        getattr(view, "public_listing_id", "")
        or package.get("public_listing_id")
        or ""
    ).strip()
    style = str(package.get("cover_style") or "").strip()

    explicit = _existing_file(package.get("cover_path"))
    if explicit:
        return explicit

    candidates: list[str] = []
    if public_id and style:
        for root in _cover_search_roots(view):
            for suffix in (".png", ".jpg", ".jpeg", ".webp"):
                candidates.append(str(root / f"{public_id}_{style}{suffix}"))
    if public_id:
        for root in _cover_search_roots(view):
            try:
                if not root.is_dir():
                    continue
                for child in root.iterdir():
                    if not child.is_file():
                        continue
                    if _looks_like_rendered_cover(child, public_id=public_id, style=style):
                        candidates.append(str(child))
            except OSError:
                continue

    for candidate in candidates:
        found = _existing_file(candidate)
        if found and _looks_like_rendered_cover(
            Path(found), public_id=public_id, style=style
        ):
            return found

    return ""


def _gallery_photo_paths(view: PublishedListingView) -> tuple[str, ...]:
    """Real original gallery only; generated Channel Cover is a separate template."""
    output: list[str] = []
    seen: set[str] = set()
    public_id = str(getattr(view, "public_listing_id", "") or "").strip()
    style = str((getattr(view, "package", {}) or {}).get("cover_style") or "").strip()
    for raw in getattr(view, "gallery", ()) or ():
        existing = _existing_file(raw)
        if not existing or existing in seen:
            continue
        path = Path(existing)
        if path.name.lower() in {"cover.jpg", "cover.jpeg", "cover.png", "cover.webp"}:
            continue
        if _looks_like_rendered_cover(path, public_id=public_id, style=style):
            continue
        seen.add(existing)
        output.append(existing)
    return tuple(output)


def _as_media_groups(paths: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    if not paths:
        return ()
    return (tuple(paths),)


def _album_media_caption(
    *,
    public_listing_id: str,
    shown: int,
    total: int,
    expand: bool,
) -> str:
    if expand:
        return f"📷 继续看实拍{chr(10)}剩余 {shown} 张{chr(10)}{public_listing_id}"
    if total > shown:
        return f"📷 实拍精选{chr(10)}共 {total} 张 · 先看 {shown} 张{chr(10)}{public_listing_id}"
    return f"📷 实拍相册{chr(10)}共 {shown} 张{chr(10)}{public_listing_id}"


def _photos_action_text(details) -> str:
    """Compact status bar under the album — no explanatory fluff."""
    lines = [_detail_status_line(details)]
    subject = " · ".join(
        part
        for part in (
            str(details.property_type or details.project_name or "").strip(),
            str(details.layout or "").strip(),
        )
        if part
    )
    if subject:
        lines.append(f"🏠 {he(subject)}")
    location = str(details.location or "").strip()
    if location:
        lines.append(f"📍 {he(location)}")
    price = _format_price(details.monthly_rent_usd)
    if price:
        lines.append(f"💰 {he(price)}")
    return chr(10).join(lines)


def _try_listing_preview(
    photos: tuple[str, ...],
    *,
    property_type: str,
    public_listing_id: str,
) -> str:
    """Property-aware Bot preview: apartment 3 frames, villa/other 4."""
    needed = 3 if "公寓" in str(property_type or "") else 4
    if len(photos) < needed:
        return ""
    try:
        from .photos_collage import render_listing_preview_collage

        return render_listing_preview_collage(
            photos[:needed],
            property_type=property_type,
            public_listing_id=public_listing_id,
        )
    except Exception:
        return ""


def _try_photo_page(
    photos: tuple[str, ...],
    *,
    public_listing_id: str,
    page: int,
) -> str:
    if not photos:
        return ""
    try:
        from .photos_collage import render_photo_page_collage

        return render_photo_page_collage(
            photos[:PHOTOS_PAGE_SIZE],
            public_listing_id=public_listing_id,
            page=page,
        )
    except Exception:
        return ""


def _photo_page_actions(
    *,
    page: int,
    total_pages: int,
    bookable: bool,
    public_listing_id: str,
) -> tuple[tuple[SemanticAction, ...], ...]:
    target = str(public_listing_id or "").strip()
    nav: list[SemanticAction] = []
    if page > 1:
        nav.append(SemanticAction("⬅️ 上一页", "photos", target, target_index=page - 1))
    if page < total_pages:
        nav.append(SemanticAction("下一页 ➡️", "photos", target, target_index=page + 1))
    rows: list[tuple[SemanticAction, ...]] = []
    if nav:
        rows.append(tuple(nav))
    action_row: list[SemanticAction] = []
    if bookable:
        action_row.append(SemanticAction("📅 预约看房", "book", target))
    action_row.append(SemanticAction("💬 咨询这套", "consult", target))
    rows.append(tuple(action_row))
    rows.append((SemanticAction("⬅️ 返回房源", "details", target),))
    return tuple(rows)


def _photos_preview_text(details, *, total: int) -> str:
    lines = [f"📷 <b>实拍｜共{max(0, int(total))}张</b>"]
    subject = "｜".join(part for part in (
        str(details.property_type or "").strip(),
        str(details.layout or "").strip(),
    ) if part)
    if subject:
        lines.append(f"🏠 {he(subject)}")
    location = str(details.location or details.project_name or "").strip()
    if location:
        lines.append(f"📍 {he(location)}")
    price = _format_price(details.monthly_rent_usd)
    if price:
        lines.append(f"💵 {he(price)}")
    status = _detail_status_line(details)
    status = status.replace("当前可预约", "可预约").replace("房态待确认", "待确认")
    lines.append(status)
    return chr(10).join(lines)


def build_photos_response(
    view: PublishedListingView,
    *,
    offset: int = 0,
    page_size: int | None = None,
) -> PublicPhotosResponse:
    del page_size
    details = build_public_listing_details(view)
    all_photos = _gallery_photo_paths(view)[:PHOTOS_MAX_TOTAL]
    total = len(all_photos)
    summary = listing_summary_bits(
        project_name=details.project_name,
        layout=details.layout,
        monthly_rent_usd=details.monthly_rent_usd,
        location=details.location,
    )
    page = max(0, int(offset or 0))
    if page > 0:
        total_pages = max(1, (total + PHOTOS_PAGE_SIZE - 1) // PHOTOS_PAGE_SIZE)
        page = min(page, total_pages)
        start_index = (page - 1) * PHOTOS_PAGE_SIZE
        page_photos = all_photos[start_index : start_index + PHOTOS_PAGE_SIZE]
        page_image = _try_photo_page(
            page_photos,
            public_listing_id=details.public_listing_id,
            page=page,
        )
        text = f"📷 <b>实拍 {page}/{total_pages}｜共{total}张</b>"
        return PublicPhotosResponse(
            media_groups=(),
            text=text,
            media_caption="",
            detail_text="",
            photo_path=page_image or (page_photos[0] if page_photos else ""),
            photo_index=start_index,
            photo_total=total,
            listing_summary=summary,
            action_rows=_photo_page_actions(
                page=page,
                total_pages=total_pages,
                bookable=details.bookable,
                public_listing_id=details.public_listing_id,
            ),
            expand_only=False,
        )

    preview = _try_listing_preview(
        all_photos,
        property_type=details.property_type,
        public_listing_id=details.public_listing_id,
    )
    first = preview or (all_photos[0] if all_photos else "")
    if all_photos:
        text = _photos_preview_text(details, total=total)
    else:
        text = (
            f"{_detail_status_line(details)}{chr(10)}{chr(10)}"
            f"这套房的实拍暂时没有加载出来。{chr(10)}"
            f"可以稍后再试，或直接联系顾问。"
        )
    return PublicPhotosResponse(
        media_groups=(),
        text=text,
        media_caption="",
        detail_text="",
        photo_path=first,
        photo_index=0,
        photo_total=total,
        listing_summary=summary,
        action_rows=_photo_actions(
            bookable=details.bookable,
            inventory_status=details.inventory_status,
            public_listing_id=details.public_listing_id,
            has_more=bool(all_photos),
        ),
        expand_only=False,
    )


# Back-compat alias used by older call sites / tests.
_flipper_photo_paths = _gallery_photo_paths

build_detail_caption = build_detail_text  # product alias used by open-details copy locks

__all__ = [
    "InternalListingAction",
    "PHOTOS_FIRST_BATCH",
    "PHOTOS_MAX_TOTAL",
    "PHOTOS_PAGE_SIZE",
    "PublicDetailsResponse",
    "PublicPhotosResponse",
    "SemanticAction",
    "build_detail_caption",
    "build_detail_text",
    "build_details_response",
    "build_photo_caption",
    "build_photos_response",
    "listing_summary_bits",
]