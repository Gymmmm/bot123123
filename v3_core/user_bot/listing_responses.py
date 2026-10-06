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
    """Details page actions: keep the current listing primary; similar is only offered when it is not bookable."""
    target = str(public_listing_id or "").strip()
    if bookable:
        return ((
            SemanticAction("📅 预约看房", "book", target),
            SemanticAction("💬 咨询这套", "consult", target),
        ),)
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
    """Album entry bar.

    The 「全部实拍」button opens the thumbnail overview first. Explicit
    ``:pg:N`` callbacks are reserved for the raw-original album pages. When
    the listing has no extra photos the button collapses to 「房源详情」.

    Bookability still comes from the unified ``bookable`` flag (active/reserved
    via inventory_status_bookable / PublishedListingView.bookable). Never invent
    a second bookable rule here.
    """
    target = str(public_listing_id or "").strip()
    status = str(inventory_status or "").strip().lower()

    def _more_or_details() -> SemanticAction:
        if has_more:
            return SemanticAction(
                "📸 全部实拍",
                "photos",
                target
            )
        return SemanticAction("📋 房源详情", "details", target)

    if bookable or status in {"active", "reserved"}:
        # Book button only when unified bookable is true.
        first: list[SemanticAction] = [_more_or_details()]
        if bookable:
            first.append(SemanticAction("📅 预约看房", "book", target))
        return (
            tuple(first),
            (SemanticAction("💬 咨询这套", "consult", target),),
        )

    if status == "pending":
        first_row: list[SemanticAction] = []
        if has_more:
            first_row.append(
                SemanticAction(
                    "📸 全部实拍",
                    "photos",
                    target,
                )
            )
        first_row.append(SemanticAction("💬 咨询这套", "consult", target))
        return (
            tuple(first_row),
            (SemanticAction("🔍 找相似", "similar", target),),
        )

    # rented / offline / inactive / withdrawn / unknown non-bookable
    return (
        (SemanticAction("🔍 找相似", "similar", target),),
        (SemanticAction("💬 咨询这套", "consult", target),),
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


def _raw_photo_paths_for_pages(view: PublishedListingView) -> tuple[str, ...]:
    """Raw original photos for the 「更多实拍」paged album.

    Product lock (Paging 2026-10-02 fix): the paged album never starts with the
    frozen cover render, the side collage, or any thumbnail derivative. It must
    show only the listing's original photos so every page (1/3, 2/3, 3/3) is a
    real photo that opens in Telegram's native viewer. The cover is reserved for
    the search-card entry and the channel post — those surfaces are unchanged.
    """
    output: list[str] = []
    seen: set[str] = set()
    cover_path = str((getattr(view, "package", {}) or {}).get("cover_path") or "").strip()
    cover_name = Path(cover_path).name.lower() if cover_path else ""

    for raw in getattr(view, "gallery", ()) or ():
        path = _existing_file(raw)
        if not path:
            continue
        name = Path(path).name.lower()
        # Defensive: any cover-named asset is excluded even if it slips into
        # the gallery_json (e.g. legacy packages with cover.jpg included).
        if name in {"cover.jpg", "cover.jpeg", "cover.png", "cover.webp"}:
            continue
        if cover_name and name == cover_name:
            continue
        if path in seen:
            continue
        seen.add(path)
        output.append(path)
    return tuple(output)


# Back-compat alias used by older call sites / tests (pointed at the
# cover-first ordering before the paged-album fix split the two surfaces).
_flipper_photo_paths = None  # placeholder, replaced at end of module

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
    if expand:
        return f"📷 继续看实拍{chr(10)}剩余 {shown} 张{chr(10)}{public_listing_id}"
    if total > shown:
        return f"📷 实拍精选{chr(10)}共 {total} 张 · 先看 {shown} 张{chr(10)}{public_listing_id}"
    return f"📷 实拍相册{chr(10)}共 {shown} 张{chr(10)}{public_listing_id}"


def _photos_action_text(details) -> str:
    """Compact status bar under the first-screen album (no paged entry)."""
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


def _status_label_for(details) -> str:
    """Return the real inventory status line — empty when status is unknown.

    Paged-album card spec: only show the status badge when we have a
    Publisher-frozen status. Never fabricate a placeholder like 「待确认」.
    """
    status = str(details.inventory_status or "").strip().lower()
    return {
        "active": "🟢 当前可预约",
        "reserved": "🟡 已有预约，仍可预约",
        "pending": "🔵 房态待确认",
        "rented": "🔴 已租出",
        "leased": "🔴 已租出",
        "inactive": "⚫ 已下架",
        "offline": "⚫ 已下架",
    }.get(status, "")


def _photos_page_action_text(
    view: PublishedListingView,
    details,
    *,
    page: int,
    total_pages: int,
    total: int,
) -> str:
    """Final paged-photo card: real listing facts only; no placeholder values."""
    public_id = str(details.public_listing_id or "").strip()
    lines = [f"📷 <b>实拍房源 {he(public_id)}</b>"] if public_id else ["📷 <b>实拍房源</b>"]
    if total > 0 and total_pages > 0:
        lines.append(f"第 {page + 1}/{total_pages} 页 · 共 {total} 张")

    project = str(details.project_name or "").strip()
    location = str(details.location or "").strip()
    layout = str(details.layout or "").strip()
    property_type = str(details.property_type or "").strip()
    status = _status_label_for(details)
    identity = project or location or property_type
    headline = "｜".join(part for part in (identity, layout) if part)
    if headline or status:
        lines.extend(["", "  ".join(part for part in (he(headline), status) if part)])

    price = _format_price(details.monthly_rent_usd)
    if price:
        lines.extend(["", f"💵 <b>{he(price)}</b>"])

    facts: list[str] = []
    if project:
        facts.append(f"📍 {he(project)}")
    elif location:
        facts.append(f"📍 {he(location)}")
    if layout:
        facts.append(f"🏠 {he(layout)}")
    size = _format_size(details.size_sqm)
    if size:
        facts.append(f"📐 {he(size)}")
    floor = display_floor(details.floor)
    if floor:
        facts.append(f"🏢 {he(floor)}")
    terms = "｜".join(he(part) for part in (details.deposit_terms, details.contract_term) if part)
    if terms:
        facts.append(f"💰 {terms}")
    utilities = _utilities_line(
        water=str(details.water_rate or "").strip(),
        electric=str(details.electric_rate or "").strip(),
    )
    fee_bits = [he(part) for part in (details.management_fee, utilities) if part]
    if fee_bits:
        facts.append("💡 " + "｜".join(fee_bits))
    if facts:
        lines.extend(["", *facts])

    adviser = _adviser_lines(view, caption=False)
    if adviser:
        lines.extend(adviser)
    return chr(10).join(lines).strip()

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
    """First screen: native Telegram originals; expand: remaining originals.

    ``offset`` > 0 means 「查看全部实拍」→ send only frames not already shown.
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
        # The first batch is already visible; only append unseen original frames.
        originals = (
            all_photos[start:PHOTOS_MAX_TOTAL]
            if start < total
            else all_photos[:PHOTOS_MAX_TOTAL]
        )
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

    # Public photos open as native Telegram originals. Keep collage rendering
    # available for other surfaces, but never hide the real-photo entry point.
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


# ---- Paged raw album ----
# Telegram allows at most 10 frames per sendMediaGroup.
PHOTOS_PAGE_SIZE = 10
PHOTOS_ALBUM_MAX_TOTAL = 30


def _album_photo_paths(view: PublishedListingView) -> tuple[str, ...]:
    """Cover-source hero first, then de-duplicated raw gallery originals."""
    raw = _raw_photo_paths_for_pages(view)
    hero = ""
    try:
        from v3_core.media.album_hero import album_hero_for_package
        hero = album_hero_for_package(getattr(view, "package", {}) or {}, raw)
    except Exception:
        hero = ""
    ordered = [hero, *raw] if hero else list(raw)
    return tuple(dict.fromkeys(path for path in ordered if path))[:PHOTOS_ALBUM_MAX_TOTAL]



def _photos_page_count(total: int) -> int:
    if total <= 0:
        return 0
    return (total + PHOTOS_PAGE_SIZE - 1) // PHOTOS_PAGE_SIZE


def _photos_page_slice(total: int, page: int) -> tuple[int, int]:
    """Clamp ``page`` into [0, page_count) and return (start, end)."""
    pages = _photos_page_count(total)
    if pages <= 0:
        return 0, 0
    safe_page = max(0, min(int(page or 0), pages - 1))
    start = safe_page * PHOTOS_PAGE_SIZE
    end = min(start + PHOTOS_PAGE_SIZE, total)
    return start, end


def _album_title(details) -> str:
    project = str(details.project_name or "").strip()
    location = str(details.location or "").strip()
    property_type = str(details.property_type or "").strip()
    layout = str(details.layout or "").strip().replace(" ", "")
    identity = project or location or property_type
    return " · ".join(part for part in (identity, layout) if part)


def _photos_page_caption(details, *, page: int, total_pages: int, total: int) -> str:
    if total <= 0:
        return ""
    title = _album_title(details)
    parts = [f"📷 {he(title)}" if title else "📷 实拍", f"共 {total} 张"]
    if total_pages > 1:
        parts.append(f"第 {page + 1}/{total_pages} 页")
    return " · ".join(parts)


def _photos_page_action_text(details) -> str:
    project = str(details.project_name or "").strip()
    location = str(details.location or "").strip()
    property_type = str(details.property_type or "").strip()
    layout = str(details.layout or "").strip()
    identity = project or location or property_type
    title = "｜".join(part for part in (identity, layout) if part) or "这套房"
    lines = [f"🏠 <b>{he(title)}</b>"]
    price = _format_price(details.monthly_rent_usd)
    if price:
        lines.append(f"💵 {he(price)}")
    status = _status_label_for(details)
    if status:
        lines.append(status)
    public_id = str(details.public_listing_id or "").strip()
    if public_id:
        lines.append(f"🆔 {he(public_id)}")
    return chr(10).join(lines)


def build_photos_overview_response(view: PublishedListingView) -> PublicPhotosResponse:
    """Thumbnail overview shown before the raw Telegram album."""
    details = build_public_listing_details(view)
    all_photos = _album_photo_paths(view)
    total = len(all_photos)
    summary = listing_summary_bits(project_name=details.project_name, layout=details.layout, monthly_rent_usd=details.monthly_rent_usd, location=details.location)
    target = str(details.public_listing_id or "").strip()
    if not all_photos:
        return PublicPhotosResponse(media_groups=(), text=f"{_detail_status_line(details)}\n\n这套房的实拍还没传上来，可以直接问顾问要更多图/视频。", media_caption="", detail_text="", photo_path="", photo_index=0, photo_total=0, listing_summary=summary, action_rows=_details_actions(bookable=details.bookable, public_listing_id=target), expand_only=False)
    from .photo_overview import MAX_PREVIEW, render_photo_overview
    import tempfile
    overview = Path(tempfile.gettempdir()) / "qiaolian_photo_overviews" / f"{target or 'listing'}.jpg"
    rendered = render_photo_overview(all_photos, output_path=overview)
    lines = _listing_fact_lines(details)
    adviser = _adviser_copy_for_view(view)
    if adviser:
        lines.extend(["", "💬 <b>侨联说</b>", he(adviser)])
    lines.extend(["", f"📸 共 {total} 张实拍 · 当前预览 {min(total, MAX_PREVIEW)} 张"])
    actions = [(SemanticAction("📸 查看全部原图", "photos", target, target_index=0),)]
    if details.bookable:
        actions.append((SemanticAction("📅 预约看房", "book", target), SemanticAction("💬 咨询这套", "consult", target)))
    else:
        actions.append((SemanticAction("💬 咨询这套", "consult", target), SemanticAction("🔍 找相似", "similar", target)))
    actions.append((SemanticAction("⬅️ 返回房源", "details", target),))
    return PublicPhotosResponse(media_groups=((rendered,),) if rendered else (), text="\n".join(lines), media_caption="", detail_text="", photo_path=rendered, photo_index=0, photo_total=total, listing_summary=summary, action_rows=tuple(actions), expand_only=False)


def build_photos_page_response(
    view: PublishedListingView,
    *,
    page: int = 0,
) -> PublicPhotosResponse:
    """Raw Telegram album page (up to 10 originals per page).


    Edge cases honored:
      * ``0`` photos → status bar + advisor button, no media.
      * ``1–3`` photos → single page, no paging buttons.
      * ``4``/``5``/``8``/``9+`` photos → page through ``PHOTOS_PAGE_SIZE`` chunks
        with prev/next/page buttons; ``next`` on last page falls back to
        "房源详情" so the album always has a usable exit.
      * Broken / duplicate paths are filtered before paging (see
        ``_gallery_photo_paths`` dedupe + ``_existing_file``). Re-running the same
        page callback is idempotent (deterministic slice + deterministic caption).
    """
    details = build_public_listing_details(view)
    # Paged 2026-10-02 fix: only raw originals — cover render is excluded so
    # every page (1/3, 2/3, 3/3) is a real photo, not the channel thumbnail.
    all_photos = _album_photo_paths(view)
    total = len(all_photos)
    total_pages = _photos_page_count(total)
    summary = listing_summary_bits(
        project_name=details.project_name,
        layout=details.layout,
        monthly_rent_usd=details.monthly_rent_usd,
        location=details.location,
    )

    if total <= 0 or total_pages <= 0:
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
            photo_path="",
            photo_index=0,
            photo_total=0,
            listing_summary=summary,
            action_rows=_photo_actions(
                bookable=details.bookable,
                inventory_status=details.inventory_status,
                public_listing_id=details.public_listing_id,
                has_more=False,
            ),
            expand_only=False,
        )

    start, end = _photos_page_slice(total, page)
    page_rows = all_photos[start:end]
    pages = total_pages
    safe_page = max(0, min(int(page or 0), pages - 1))
    caption = _photos_page_caption(
        details, page=safe_page, total_pages=pages, total=total
    )
    groups = _as_media_groups(page_rows)
    first = page_rows[0] if page_rows else ""

    # 2026-10-03 fix: the paged album always uses the paged keyboard contract
    # so the button row layout (⬅️ 返回房源 + book/consult) is consistent
    # regardless of how many pages the listing has. Single-page listings
    # only render a no-op "1/1" counter (no prev / next buttons) — the same
    # controls that multi-page listings use on the same N/M cell.
    action_rows = _photo_page_actions(
        public_listing_id=details.public_listing_id,
        page=safe_page,
        total_pages=pages,
        bookable=details.bookable,
        inventory_status=details.inventory_status,
    )

    return PublicPhotosResponse(
        media_groups=groups,
        text=_photos_page_action_text(details),
        media_caption=caption,
        detail_text="",
        photo_path=first,
        photo_index=start,
        photo_total=total,
        listing_summary=summary,
        action_rows=action_rows,
        expand_only=False,
    )


def _photo_page_actions(
    *,
    public_listing_id: str,
    page: int,
    total_pages: int,
    bookable: bool,
    inventory_status: str,
) -> tuple[tuple[SemanticAction, ...], ...]:
    """Final photo UX: page counter/navigation, booking/consult, return to listing."""
    target = str(public_listing_id or "").strip()
    safe_page = max(0, min(int(page or 0), max(total_pages - 1, 0)))
    del inventory_status  # availability comes from the unified bookable flag only
    rows: list[tuple[SemanticAction, ...]] = []
    if total_pages > 1:
        paging: list[SemanticAction] = []
        if safe_page > 0:
            paging.append(SemanticAction("⬅️ 上一页", "photos", target, target_index=safe_page - 1))
        paging.append(SemanticAction(f"{safe_page + 1}/{total_pages}", "photos", target, target_index=safe_page))
        if safe_page + 1 < total_pages:
            paging.append(SemanticAction("下一页 ➡️", "photos", target, target_index=safe_page + 1))
        rows.append(tuple(paging))

    if bookable:
        rows.append((
            SemanticAction("📅 预约看房", "book", target),
            SemanticAction("💬 咨询这套", "consult", target),
        ))
    else:
        rows.append((SemanticAction("💬 咨询这套", "consult", target),))
    rows.append((SemanticAction("⬅️ 返回房源", "details", target),))
    return tuple(rows)



# Back-compat alias used by older call sites / tests (cover-first ordering
# before the 2026-10-02 paged-album fix split the two surfaces).
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
    "build_photos_overview_response",
    "build_photos_page_response",
    "build_photos_response",
    "listing_summary_bits",
]
