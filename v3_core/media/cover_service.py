"""Resolve V3 inventory facts into one final rendered cover.

The service owns cover output naming and the inventory -> renderer contract.
It does not read drafts or mutate publication packages. ``renderer`` is
injectable so rendering contracts can be tested independently. Pillow is the production renderer.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from v3_core.publishing.formatting import display_floor, display_layout, display_property_type
from v3_core.storage.inventory_reader import InventoryReader
from .cover_renderer import CoverRenderData, _is_apartment, render_cover
from .straighten import straighten_file
from .cover_styles import (
    is_video_cover_style,
    recommended_cover_style,
    resolve_cover_style_for_source,
)
from .service import PreparedSourceMedia

_EMPTY_TERMS = {"", "none", "null", "未知", "待确认", "-", "无"}


def cover_terms(offer: dict) -> str:
    """'押2付1 · 1年' from the offer's real fields (payment/deposit + contract); missing parts omitted."""
    if str(offer.get("offer_type") or "rent").strip().lower() != "rent":
        return ""
    def clean(value: object) -> str:
        text = " ".join(str(value or "").split())
        return "" if text.lower() in _EMPTY_TERMS else text[:16]
    deposit = clean(offer.get("payment_terms")) or clean(offer.get("deposit_terms"))
    contract = clean(offer.get("contract_term"))
    for prefix in ("租期", "合同"):
        contract = contract.removeprefix(prefix).lstrip(":： ")
    return " · ".join(part for part in (deposit, contract) if part)


@dataclass(frozen=True)
class RenderedCover:
    listing_id: str
    offer_id: str
    style: str
    source_image: str
    output_path: str
    data: CoverRenderData


Renderer = Callable[..., str]


class CoverRenderService:
    def __init__(
        self,
        *,
        reader: InventoryReader,
        output_dir: str | Path,
        renderer: Renderer = render_cover,
    ):
        self.reader = reader
        self.output_dir = Path(output_dir).expanduser().resolve()
        self.renderer = renderer

    @staticmethod
    def _price(offer: dict) -> str:
        if str(offer.get("offer_type") or "") == "rent":
            value = offer.get("monthly_rent_usd")
        else:
            value = offer.get("sale_price_usd")
        if value in (None, ""):
            return ""
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value)
        return str(int(numeric)) if numeric.is_integer() else str(numeric)

    def render(
        self,
        *,
        listing_id: str,
        offer_id: str,
        media: PreparedSourceMedia,
        style: str | None = None,
        output_path: str | Path | None = None,
    ) -> RenderedCover:
        listing = self.reader.listing(listing_id)
        offer = self.reader.offer(offer_id)
        if str(offer.get("listing_id") or "") != str(listing_id):
            raise ValueError("cover_offer_listing_mismatch")
        canonical = self.reader.canonical(str(listing["canonical_record_id"]))
        facts = dict(canonical["facts"])
        selected_style = style or recommended_cover_style(
            listing.get("property_type"),
            listing.get("property_subtype"),
            listing.get("display_title"),
            facts.get("property_type"),
            facts.get("property_subtype"),
        )
        public_id = str(listing.get("public_listing_id") or "").strip()
        if not public_id:
            raise ValueError("cover_requires_public_listing_id")
        cover_source = Path(media.cover_source_path).resolve()
        if not cover_source.is_file():
            raise FileNotFoundError(f"cover_source_not_found:{media.cover_source_path}")
        # Style family from admin/default; canvas orientation follows the main photo.
        normalized_style = resolve_cover_style_for_source(
            selected_style,
            cover_source,
            allow_video=is_video_cover_style(selected_style),
        )

        property_type = display_property_type(listing.get("property_type") or "")
        layout = display_layout(listing.get("layout") or "", property_type)
        floor = display_floor(listing.get("floor") or "", property_type)
        data = CoverRenderData(
            public_listing_id=public_id,
            project=str(listing.get("project_name") or ""),
            project_alias=str(listing.get("project_alias") or ""),
            property_type=property_type,
            deal_type=str(offer.get("offer_type") or facts.get("deal_type") or "rent"),
            layout=layout,
            area=str(
                listing.get("public_location_display")
                or listing.get("canonical_area_display")
                or ""
            ),
            size=str(listing.get("size_sqm") or ""),
            floor=floor,
            price=self._price(offer),
            # Channel covers stay factual and minimal. Marketing/adviser copy
            # belongs in the rental-detail view, never on the cover itself.
            highlight_1="",
            highlight_2="",
            highlight_3="",
            terms=cover_terms(offer),
        )
        target = (
            Path(output_path).expanduser().resolve()
            if output_path
            else self.output_dir / f"{public_id}_{normalized_style}.png"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        render_kwargs = {
            "style": normalized_style,
            "source_image": str(cover_source),
            "output_path": str(target),
            "data": data,
        }
        if self.renderer is render_cover:
            render_kwargs["source_images"] = (
                media.cover_gallery_paths or media.gallery_paths
            )
            render_kwargs["source_labels"] = media.cover_gallery_labels
            template = str(getattr(media, "cover_template", "") or "")
            if template:
                render_kwargs["layout"] = template
            # Use only the cover planner's approved photos. Do not promote
            # rejected gallery images just to fill a four-photo collage.
            approved_thumbs = tuple(media.cover_gallery_paths)
            if approved_thumbs:
                render_kwargs["source_images"] = approved_thumbs
            elif media.cover_plan:
                render_kwargs["source_images"] = ()
            # Channel cover is one strong photo, not a miniature collage. On a
            # phone the small detail cells and repeated facts are hard to read;
            # the remaining approved photos stay available in the detail flow.
            if template != "single":
                render_kwargs["layout"] = "mobile_single_4x3"
                render_kwargs["source_images"] = ()
            planned = str(getattr(media, "cover_render_source", "") or "")
            if planned and Path(planned).is_file():
                # Bounded correction already ran in media preparation (shared with the bot album).
                render_kwargs["source_image"] = planned
            elif not template and not _is_apartment(data):
                # Legacy media without a plan: level the villa walls; failure keeps the original.
                straight = straighten_file(
                    cover_source, target.parent / f"{public_id}_hero_straight.jpg"
                )
                if straight:
                    render_kwargs["source_image"] = straight["path"]
        rendered = self.renderer(**render_kwargs)
        rendered_path = Path(rendered).expanduser().resolve()
        if not rendered_path.is_file():
            raise RuntimeError("cover_renderer_did_not_create_output")
        return RenderedCover(
            listing_id=str(listing_id),
            offer_id=str(offer_id),
            style=normalized_style,
            source_image=str(cover_source),
            output_path=str(rendered_path),
            data=data,
        )


__all__ = ["CoverRenderService", "RenderedCover"]
