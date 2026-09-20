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
    """Publisher-frozen copy only; respects adviser_copy_source=hidden."""
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
    if explicit and _looks_like_rendered_cover(
        Path(explicit), public_id=public_id, style=style
    ):
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
    """Cover first, then remaining real/gallery photos (deduped by resolved path).

    Product lock: 📷 房源详情 opens at 📸 1/N = cover render, then gallery shots.
    """
    output: list[str] = []
    seen: set[str] = set()

    def _append(path: str) -> None:
        existing = _existing_file(path)
        if not existing:
            return
        key = existing
        if key in seen:
            return
        seen.add(key)
        output.append(existing)

    cover = _frozen_cover_path(view)
    if cover:
        _append(cover)

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
        _append(path)
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
