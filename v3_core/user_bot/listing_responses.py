"""Telegram-neutral public listing responses for the V3 User Bot.

Merged listing view: one entry opens a single-photo flipper (上一张 / 下一张)
with the rental essentials in its caption and no extra text bubble.
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
    """Single-photo flipper payload (rental caption + one current frame)."""

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
    target = str(public_listing_id or "").strip()
    if bookable:
        return (
            (
                SemanticAction("📅 预约看房", "book", target),
                SemanticAction("💬 咨询这套", "consult", target),
            ),
            (SemanticAction("🔍 继续找房", "similar", target),),
        )
    return (
        (SemanticAction("💬 咨询这套", "consult", target),),
        (SemanticAction("🔍 继续找房", "similar", target),),
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
                SemanticAction("⬅️ 上一张", "photos", target, target_index=prev_index),
                SemanticAction("下一张 ➡️", "photos", target, target_index=next_index),
            )
        )
    if bookable:
        rows.append(
            (
                SemanticAction("📅 预约看房", "book", target),
                SemanticAction("💬 咨询这套", "consult", target),
            )
        )
    else:
        rows.append((SemanticAction("💬 咨询这套", "consult", target),))
    rows.append((SemanticAction("🔍 继续找房", "similar", target),))
    return tuple(rows)

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
    return lines


def _adviser_lines(view: PublishedListingView, *, caption: bool) -> list[str]:
    notes = _adviser_copy_for_view(view)
    if not notes:
        return []
    if caption and len(notes) > 360:
        notes = notes[:359].rstrip() + "…"
    return ["", "💬 <b>侨联说</b>", *(he(line) for line in notes.splitlines())]


def build_detail_text(view: PublishedListingView) -> str:
    """Text fallback for listings without a usable photo."""
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
    return "\n".join(lines).strip()

def build_photo_caption(
    view: PublishedListingView,
    *,
    photo_index: int = 0,
    photo_total: int = 0,
) -> str:
    """Scan-friendly facts stay with every photo in the listing gallery."""
    details = build_public_listing_details(view)
    lines = _listing_fact_lines(details)
    # Keep enough space for the photo index even with unusually long copy.
    adviser = _adviser_lines(view, caption=True)
    if len("\n".join([*lines, *adviser])) < 850:
        lines.extend(adviser)
    total = max(0, int(photo_total or 0))
    if total > 0:
        index = max(0, int(photo_index or 0)) % total
        lines.extend(["", f"📸 {index + 1}/{total}"])
    return "\n".join(lines)


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
    """Resolve the listing COVER RENDER file for flipper index 1/N.

    Prefer the frozen package ``cover_path`` when the file exists. If that path is
    empty/stale at runtime (common when gallery derivatives are mounted but the
    recorded cover absolute path is not), rediscover the rendered cover under
    nearby ``covers_v3`` roots using public id + cover_style. Never fall back to
    a random gallery room shot here — gallery ordering is handled by the
    flipper builder after cover resolution.
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


def _flipper_photo_paths(view: PublishedListingView) -> tuple[str, ...]:
    """Cover first, then remaining real/gallery photos (deduped by path / near-dup).

    Product lock: 📷 更多实拍 opens at 📸 1/N = cover render, then *other* gallery
    shots — never the same room again as a logo-only frame.
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


def build_photos_response(
    view: PublishedListingView,
    *,
    offset: int = 0,
    page_size: int | None = None,
) -> PublicPhotosResponse:
    """Merged one-photo flipper with rental essentials (上一张 / 下一张).

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
        detail_text="",
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


build_detail_caption = build_detail_text  # product alias used by open-details copy locks

__all__ = [
    "InternalListingAction",
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