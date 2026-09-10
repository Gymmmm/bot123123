"""Locked end-of-gallery panel for V3 listing real-photo delivery."""
from __future__ import annotations

from html import escape as he

from .listing_responses import SemanticAction


def build_photos_end_text(
    *,
    has_media: bool,
    bookable: bool,
    public_listing_id: str,
) -> str:
    public_id = he(str(public_listing_id or "").strip())
    if has_media:
        if bookable:
            return (
                "📸 <b>以上实拍已经全部展示完毕。</b>\n\n"
                "可以返回租赁详情继续看条件，或直接预约看房。"
            )
        return (
            "📸 <b>以上实拍已经全部展示完毕。</b>\n\n"
            "这套房暂时不能预约。可以返回租赁详情，联系中文顾问，或重新找房。"
        )
    return (
        f"📸 <b>更多实拍｜{public_id}</b>\n\n"
        "这套房的实拍暂时没有加载出来。\n\n"
        "可以稍后再试，或直接联系中文顾问。"
    )


def build_photos_end_actions(
    *,
    bookable: bool,
    public_listing_id: str,
) -> tuple[tuple[SemanticAction, ...], ...]:
    target = str(public_listing_id or "").strip()
    details = SemanticAction("🏠 租赁详情", "details", target)
    consult = SemanticAction("💬 联系中文顾问", "consult", target)
    if bookable:
        return (
            (details, SemanticAction("📅 预约看房", "book", target)),
            (consult,),
        )
    return (
        (details,),
        (consult, SemanticAction("🔍 重新找房", "change_search")),
    )


__all__ = ["build_photos_end_actions", "build_photos_end_text"]
