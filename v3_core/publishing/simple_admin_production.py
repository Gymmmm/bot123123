"""Production admin extensions for no-offer V3 anomalies.

The base simplified console handles ordinary offer-backed listings.  This thin
subclass makes no-offer canonical reviews (for example, missing rent) visible
through the same human-facing exception UI without creating fake offers.
"""
from __future__ import annotations

from html import escape
from typing import Any

from telegram import InlineKeyboardMarkup
from telegram.constants import ParseMode

from .autopilot_anomalies import FinalAutoPublishRepository, FinalAutoPublishService
from .simple_admin import SimplePublisherAdminController


class ProductionSimplePublisherAdminController(SimplePublisherAdminController):
    repository: FinalAutoPublishRepository
    autopilot: FinalAutoPublishService

    def _exception_detail(self, offer_id: str) -> dict[str, Any]:
        token = str(offer_id or "")
        if token.startswith("review:"):
            return self.repository.review_exception_detail(token)
        return super()._exception_detail(token)

    async def _recheck_review_exception(self, message: Any, context: Any, token: str) -> None:
        detail = self.repository.review_exception_detail(token)
        listing_id = str(detail["listing_id"])
        offer_id = self.repository.find_rent_offer_for_listing(listing_id)
        if not offer_id:
            await message.reply_text(
                f"未发布：{escape(str(detail.get('reason_text') or '房源信息仍不完整'))}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            return
        result = await self.autopilot.process_one(
            bot=context.bot,
            force_offer_id=offer_id,
        )
        if result.status == "published":
            await message.reply_text(
                f"✅ 已重新检查并发布。频道消息：{escape(result.channel_message_id)}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            return
        if result.status == "already_published":
            await message.reply_text(
                "该房源已经发布，不会重复发送。",
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            return
        await message.reply_text(
            f"未发布：{escape(result.reason_text or result.status)}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([self.home_row()]),
        )

    async def handle_callback(self, update: Any, context: Any) -> bool:
        query = getattr(update, "callback_query", None)
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        if raw.startswith("v3smp|recheck|review:"):
            if query is None:
                return False
            token = raw.split("|", 2)[2]
            await self._recheck_review_exception(query.message, context, token)
            return True
        return await super().handle_callback(update, context)


__all__ = ["ProductionSimplePublisherAdminController"]
