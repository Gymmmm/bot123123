from __future__ import annotations

import asyncio
import html

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode


_INSTALLED = False


def install_review_queue_patch() -> None:
    """Restore the production review UX and lock the public publish path.

    - show up to 6 reviewable drafts instead of hard-cutting after the first
    - keep all three approved cover styles visible, including black gold
    - disable the legacy discussion-thread publisher; public listings use the
      channel main post + three deep-link buttons only
    """
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True

    import autopilot_publish_bot as ap
    import meihua_publisher as mp

    # The locked channel contract is a single main post with three buttons.
    # More photos/details are served by the User Bot deep links, not duplicated
    # into Telegram's discussion thread.
    async def no_discussion_segments(*args, **kwargs):
        return None, False

    mp.send_discussion_three_segments = no_discussion_segments

    original_kb_preview = ap._kb_preview

    def patched_kb_preview(draft_pk: int, selected_variant: str = "a") -> InlineKeyboardMarkup:
        markup = original_kb_preview(draft_pk, selected_variant)
        rows = [list(row) for row in markup.inline_keyboard]
        has_black_gold = any(
            "黑金" in str(getattr(button, "text", ""))
            or str(getattr(button, "callback_data", "")).startswith("ap:tg:")
            for row in rows
            for button in row
        )
        if not has_black_gold:
            p = str(draft_pk)
            insert_at = next(
                (i for i, row in enumerate(rows) if any("刷新正式预览" in str(getattr(b, "text", "")) for b in row)),
                min(2, len(rows)),
            )
            rows.insert(insert_at, [InlineKeyboardButton("黑金高级感", callback_data=f"ap:tg:{p}")])
        return InlineKeyboardMarkup(rows)

    async def patched_cmd_pending(update, context) -> None:
        if not ap._is_admin(update.effective_user.id):
            return
        limit = 6
        with ap._conn() as c:
            overview = ap.pending_overview(c, preview_min_score=0, preview_limit=50, queue_limit=50)
        pending_total = int(overview["pending_total"])
        all_pending = overview["queue_rows"]
        rows = [row for row in all_pending if not ap._review_candidate_blockers(row)][:limit]
        publishable_total = sum(1 for row in all_pending if not ap._review_candidate_blockers(row))
        exception_total = max(0, pending_total - publishable_total)
        if pending_total == 0:
            await update.message.reply_text("📋 当前没有待审核草稿。", reply_markup=ap.admin_menu())
            return
        if not rows:
            await update.message.reply_text(
                "📋 <b>当前没有可以直接审核的房源</b>\n\n"
                f"资料不完整：{exception_total} 套\n"
                "这些房源已保留在后台，不需要逐套填写技术字段。",
                parse_mode=ParseMode.HTML,
                reply_markup=ap.admin_menu(),
            )
            return

        from meihua_publisher import build_caption

        for index, row in enumerate(rows, start=1):
            d = ap._draft_to_caption_dict(row)
            cap = build_caption(d, caption_variant="a")
            canonical_head = ap.display_title(d.get("title") or d.get("project") or d.get("area") or row["title"])
            head = (
                f"📋 <b>待审核 {index}/{min(publishable_total, limit)}</b>"
                f"　·　可审核 {publishable_total} 套"
                f"　·　资料不完整 {exception_total} 套\n\n"
                "🏠 <b>请确认这套房源</b>\n"
                f"{html.escape(canonical_head)}\n\n"
            )
            text = head + (cap[:3200] if len(cap) > 3200 else cap)
            img = await asyncio.to_thread(ap._formal_preview_cover, row) or ap._cover_path_for_draft(row)
            kb = patched_kb_preview(row["id"])
            try:
                if img:
                    with open(img, "rb") as f:
                        await update.message.reply_photo(
                            photo=f,
                            caption=text[:1024],
                            reply_markup=kb,
                            parse_mode=ParseMode.HTML,
                        )
                    if len(text) > 1024:
                        await update.message.reply_text(
                            text[1024 : 1024 + 3500],
                            reply_markup=kb,
                            parse_mode=ParseMode.HTML,
                        )
                else:
                    await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
            except Exception:
                ap.logger.exception("发送预览失败")
                await update.message.reply_text(text[:3500], reply_markup=kb, parse_mode=ParseMode.HTML)

    ap._kb_preview = patched_kb_preview
    ap.cmd_pending = patched_cmd_pending
