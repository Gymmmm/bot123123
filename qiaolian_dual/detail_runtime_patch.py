from __future__ import annotations

_INSTALLED = False


def install_detail_runtime_patch() -> None:
    """Keep the deep-link rental detail on the current mobile copy contract."""
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True

    from . import listing
    from .talk_engine import generate_talk
    from html import escape as he

    original = listing.listing_cost_text

    def patched_listing_cost_text(listing_id: str) -> str:
        text = original(listing_id)
        old_tail = "\n\n📅 <b>想实地看看？</b>\n选择方便的时间即可。"
        if text.endswith(old_tail):
            text = text[:-len(old_tail)]

        item = listing.listing_context(listing_id)
        talk = generate_talk(item, max_points=2, allow_empty=True).strip()
        if talk:
            safe_talk = "\n".join(he(line) for line in talk.splitlines() if line.strip())
            text += f"\n\n💬 <b>侨联说</b>\n{safe_talk}"

        text += (
            "\n\n📅 <b>想看这套？</b>\n"
            "点「预约看房」选时间。\n"
            "没时间到现场，也可以在预约里选择实时视频看房。"
        )
        return text

    listing.listing_cost_text = patched_listing_cost_text
