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
PHOTOS_FIRST_BATCH = 4
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
    """Text details actions (photos opens the album surface separately)."""
    target = str(public_listing_id or "").strip()
    if bookable:
        return (
            (
                SemanticAction("📅 预约看房", "book", target),
                SemanticAction("💬 中文顾问", "consult", target),
            ),
            (
                SemanticAction("📷 更多实拍", "photos", target),
                SemanticAction("🔍 继续找房", "similar", target),
            ),
        )
    return (
        (SemanticAction("💬 中文顾问", "consult", target),),
        (
            SemanticAction("📷 更多实拍", "photos", target),
            SemanticAction("🔍 继续找房", "similar", target),
        ),
    )


def _photo_actions(
    *,
    bookable: bool,
    inventory_status: str,
    public_listing_id: str,
    has_more: bool,
) -> tuple[tuple[SemanticAction, ...], ...]:
    """FINAL LOCK status matrix for the album action bar.

    Bookability still comes from the unified ``bookable`` flag (active/reserved
    via inventory_status_bookable / PublishedListingView.bookable). Never invent
    a second bookable rule here.
    """
    target = str(public_listing_id or "").strip()
    status = str(inventory_status or "").strip().lower()

    def _expand_or_details() -> SemanticAction:
        if has_more:
            return SemanticAction(
                "📷 查看全部实拍",
                "photos",
                target,
                target_index=PHOTOS_FIRST_BATCH,
            )
        return SemanticAction("📷 房源详情", "details", target)

    if bookable or status in {"active", "reserved"}:
        # Book button only when unified bookable is true.
        first: list[SemanticAction] = [_expand_or_details()]
        if bookable:
            first.append(SemanticAction("📅 预约看房", "book", target))
        return (
            tuple(first),
            (SemanticAction("💬 中文顾问", "consult", target),),
        )

    if status == "pending":
        first_row: list[SemanticAction] = []
        if has_more:
            first_row.append(
                SemanticAction(
                    "📷 查看全部实拍",
                    "photos",
                    target,
                    target_index=PHOTOS_FIRST_BATCH,
                )
            )
        first_row.append(SemanticAction("💬 中文顾问", "consult", target))
        return (
            tuple(first_row),
            (
                SemanticAction("🏠 帮我找房", "change_search", target),
                SemanticAction("🔎 看相近房源", "similar", target),
            ),
        )

    # rented / offline / inactive / withdrawn / unknown non-bookable
    return (
        (
            SemanticAction("🏠 帮我找房", "change_search", target),
            SemanticAction("🔎 看相近房源", "similar", target),
        ),
        (SemanticAction("💬 中文顾问", "consult", target),),
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
    """Public snapshot facts plus live availability, omitting unknown fields."""
    project = str(details.project_name or "").strip()
    location = str(details.location or "").strip()
    layout = str(details.layout or "").strip()
    lines: list[str] = []
    if project:
        lines.append(f"🏡 项目：{he(project)}")
    elif details.property_type:
        lines.append(f"🏡 类型：{he(details.property_type)}")
    if location and not location_display_overlaps_project(project, location):
        lines.append(f"📍 区域：{he(location)}")
    if layout:
        lines.append(f"🛏 户型：{he(layout)}")
    if details.monthly_rent_usd:
        lines.append(f"💵 租金：{_format_price(details.monthly_rent_usd)}")
    size = _format_size(details.size_sqm)
    floor = display_floor(details.floor)
    if size and floor:
        lines.append(f"📐 面积/楼层：{he(size)} · {he(floor)}")
    elif size:
        lines.append(f"📐 面积：{he(size)}")
    elif floor:
        lines.append(f"🏙 楼层：{he(floor)}")
    terms = [he(part) for part in (details.deposit_terms, details.contract_term) if part]
    if terms:
        lines.append("🗝 租约：" + " · ".join(terms))
    lines.append(_detail_status_line(details))
    if details.public_listing_id:
        lines.append(f"🪧 编号：{he(details.public_listing_id)}")
    return lines


def _adviser_lines(view: PublishedListingView, *, caption: bool) -> list[str]:
    notes = _adviser_copy_for_view(view)
    if not notes:
        return []
    if caption and len(notes) > 360:
        notes = notes[:359].rstrip() + "…"
    return ["", "💬 侨联说", *(he(line) for line in notes.splitlines())]


def build_detail_text(view: PublishedListingView) -> str:
    """Text fallback / details surface for listings."""
    details = build_public_listing_details(view)
    utilities = _utilities_line(
        water=str(details.water_rate or "").strip(),
        electric=str(details.electric_rate or "").strip(),
    )
    lines = _listing_fact_lines(details)
    for label, value in (("物业费", details.management_fee), ("水电", utilities), ("配套", details.building_amenities)):
        if value:
            lines.append(f"🧾 {label}：{he(value)}")
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
    """Cover first, then remaining gallery photos (deduped by path / near-dup).

    Product lock: album frame 1 is the cover render when available, then other
    gallery shots — never the same room again as a logo-only frame.
    """
    output: list[str] = []
    seen: set[str] = set()
    cover_hash: int | None = None

    def _append(path: str, *, skip_near_cover: bool = False) -> None:
        nonlocal cover_hash
        existing = _existing_file(path)
        if not existing:
            return
        key = existing
        if key in seen:
            return
        if skip_near_cover and cover_hash is not None:
            try:
                from v3_core.media.ranker import NEAR_DUPLICATE_HAMMING, _dhash, _hamming

                gallery_hash = _dhash(Path(existing))
                # Cover has template overlays; allow a looser match than gallery dedupe.
                if _hamming(cover_hash, gallery_hash) <= max(NEAR_DUPLICATE_HAMMING + 10, 12):
                    return
            except Exception:
                pass
        seen.add(key)
        output.append(existing)

    cover = _frozen_cover_path(view)
    if cover:
        _append(cover)
        try:
            from v3_core.media.ranker import _dhash

            cover_hash = _dhash(Path(cover))
        except Exception:
            cover_hash = None

    for raw in getattr(view, "gallery", ()) or ():
        path = str(raw or "").strip()
        if not path:
            continue
        # Skip cover-named assets once cover is already first.
        if cover and Path(path).name.lower() in {
            "cover.jpg",
            "cover.jpeg",
            "cover.png",
            "cover.webp",
        }:
            continue
        _append(path, skip_near_cover=bool(cover))
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
    public_id = he(str(public_listing_id or "").strip() or "实拍")
    if expand:
        return f"📷 {public_id}｜全部实拍{chr(10)}共 {shown} 张"
    if total > shown:
        return f"📷 {public_id}｜实拍精选{chr(10)}共 {total} 张 · 先看 {shown} 张"
    return f"📷 {public_id}｜实拍相册{chr(10)}共 {shown} 张"


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
    if details.public_listing_id:
        lines.append(f"🆔 {he(details.public_listing_id)}")
    return chr(10).join(lines)


def _try_side_collage(
    photos: tuple[str, ...],
    *,
    public_listing_id: str,
) -> str:
    """Build left-large + right-3 collage.

    Collector policy: listings with fewer than 4 usable photos are not kept, so
    collage is only attempted when ≥4 frames are available. Short galleries fall
    back to native send without inventing duplicate thumbs.
    """
    if len(photos) < 4:
        return ""
    try:
        from .photos_collage import render_side_stack_collage

        return render_side_stack_collage(
            photos[:4],
            public_listing_id=public_listing_id,
        )
    except Exception:
        return ""


def build_photos_response(
    view: PublishedListingView,
    *,
    offset: int = 0,
    page_size: int | None = None,
) -> PublicPhotosResponse:
    """First screen: side-stack collage (1 JPG); expand: native MediaGroup originals.

    ``offset`` > 0 means 「查看全部实拍」→ send original frames (cap 10).
    ``page_size`` ignored (call-site compatibility).
    """
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

    start = max(0, int(offset or 0))
    if start > 0:
        # Full original album (collage was only a preview card).
        originals = all_photos[:PHOTOS_MAX_TOTAL]
        groups = _as_media_groups(originals)
        first = originals[0] if originals else ""
        caption = (
            _album_media_caption(
                public_listing_id=details.public_listing_id,
                shown=len(originals),
                total=total,
                expand=True,
            )
            if originals
            else ""
        )
        return PublicPhotosResponse(
            media_groups=groups,
            text="",
            media_caption=caption,
            detail_text="",
            photo_path=first,
            photo_index=start,
            photo_total=total,
            listing_summary=summary,
            action_rows=(),
            expand_only=True,
        )

    collage = _try_side_collage(
        all_photos,
        public_listing_id=details.public_listing_id,
    )
    if collage:
        return PublicPhotosResponse(
            media_groups=((collage,),),
            text=_photos_action_text(details),
            media_caption="",
            detail_text="",
            photo_path=collage,
            photo_index=0,
            photo_total=total,
            listing_summary=summary,
            action_rows=_photo_actions(
                bookable=details.bookable,
                inventory_status=details.inventory_status,
                public_listing_id=details.public_listing_id,
                # Collage uses 4 frames; expand always offers the native originals.
                has_more=True,
            ),
            expand_only=False,
        )

    # Fallback: native first-batch album (0/1 photo, or collage render failed).
    if total > PHOTOS_FIRST_BATCH:
        batch = all_photos[:PHOTOS_FIRST_BATCH]
        has_more = True
    else:
        batch = all_photos
        has_more = False

    groups = _as_media_groups(batch)
    first = batch[0] if batch else ""
    if batch:
        caption = _album_media_caption(
            public_listing_id=details.public_listing_id,
            shown=len(batch),
            total=total,
            expand=False,
        )
        text = _photos_action_text(details)
    else:
        caption = ""
        text = (
            f"{_detail_status_line(details)}{chr(10)}{chr(10)}"
            f"这套房的实拍暂时没有加载出来。{chr(10)}"
            f"可以稍后再试，或直接联系顾问。"
        )

    return PublicPhotosResponse(
        media_groups=groups,
        text=text,
        media_caption=caption,
        detail_text="",
        photo_path=first,
        photo_index=0,
        photo_total=total,
        listing_summary=summary,
        action_rows=_photo_actions(
            bookable=details.bookable,
            inventory_status=details.inventory_status,
            public_listing_id=details.public_listing_id,
            has_more=has_more,
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
