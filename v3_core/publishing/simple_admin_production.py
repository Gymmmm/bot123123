"""Production admin extensions for V3 anomaly repair.

The base simplified console handles ordinary offer-backed listings.  This
subclass makes no-offer canonical reviews visible through the same human-facing
exception UI and, when an operator adds media, updates the original source
identity instead of creating a second admin source.
"""
from __future__ import annotations

import asyncio
from html import escape
from typing import Any

from telegram import InlineKeyboardMarkup
from telegram.constants import ParseMode

from v3_core.ingest.intake_service import SourceIntake
from .autopilot import ERROR_LABELS
from .autopilot_anomalies import FinalAutoPublishRepository, FinalAutoPublishService
from .simple_admin import NEW_LISTING_STATE_KEY, SimplePublisherAdminController


class ProductionSimplePublisherAdminController(SimplePublisherAdminController):
    repository: FinalAutoPublishRepository
    autopilot: FinalAutoPublishService

    def _exception_detail(self, offer_id: str) -> dict[str, Any]:
        token = str(offer_id or "")
        if token.startswith("review:"):
            return self.repository.review_exception_detail(token)
        return super()._exception_detail(token)

    async def append_exception_media(self, message: Any, context: Any, offer_id: str) -> None:
        row = self._exception_detail(offer_id)
        source = self.source_reader.source_post(int(row["source_post_id"]))
        context.user_data[NEW_LISTING_STATE_KEY] = {
            "session_id": str(source["source_post_id"]),
            "text": str(source.get("raw_text") or ""),
            "images": list(source.get("raw_images") or []),
            "source_post_pk": int(source["id"]),
            "offer_id": offer_id,
            "review_id": str(row["review_id"]),
            "cover_index": 0,
            "repair_source_identity": {
                "source_id": source.get("source_id"),
                "source_type": str(source.get("source_type") or "telegram_channel"),
                "source_name": str(source.get("source_name") or ""),
                "source_post_id": str(source.get("source_post_id") or ""),
                "source_url": str(source.get("source_url") or ""),
                "source_author": str(source.get("source_author") or "channel"),
                "meta": dict(source.get("raw_meta") or {}),
            },
        }
        await message.reply_text(
            "请直接发送要补充的图片。收到后会合并到原房源，并使用原房源身份重新检查。"
        )

    async def _materialize_manual(self, update: Any, context: Any) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict):
            return
        identity = state.get("repair_source_identity")
        if not isinstance(identity, dict):
            await super()._materialize_manual(update, context)
            return

        meta = dict(identity.get("meta") or {})
        meta.update(
            {
                "origin": "publisher_admin_repair",
                "telegram_user_id": int(update.effective_user.id),
            }
        )
        source = SourceIntake(
            source_id=(str(identity.get("source_id")) if identity.get("source_id") not in (None, "") else None),
            source_type=str(identity.get("source_type") or "telegram_channel"),
            source_name=str(identity.get("source_name") or ""),
            source_post_id=str(identity.get("source_post_id") or ""),
            source_url=str(identity.get("source_url") or ""),
            source_author=str(identity.get("source_author") or "channel"),
            raw_text=str(state.get("text") or ""),
            raw_images=list(state.get("images") or []),
            raw_videos=[],
            meta=meta,
        )
        result = await asyncio.to_thread(self.intake.persist, source)
        state["source_post_pk"] = result.source_post_pk
        if result.source_post_pk is None:
            return

        worker_result = await asyncio.to_thread(self.worker.process_one, int(result.source_post_pk))
        if worker_result.status != "materialized":
            await update.effective_message.reply_text(
                "重新解析失败，原房源没有发布。可以继续补充资料后再次发送。\n"
                + escape(worker_result.error[:500]),
                parse_mode=ParseMode.HTML,
            )
            return

        self.repository.sync_candidates()
        with self.repository._connect() as conn:
            row = conn.execute(
                """SELECT a.offer_id,a.review_id,o.offer_type FROM publisher_auto_items_v3 a
                   JOIN listing_offers o ON o.offer_id=a.offer_id
                   WHERE a.listing_id=? ORDER BY CASE o.offer_type WHEN 'rent' THEN 0 ELSE 1 END LIMIT 1""",
                (worker_result.listing_id,),
            ).fetchone()
        if row is None:
            self.repository.sync_review_exceptions()
            await update.effective_message.reply_text(
                "房源信息仍不完整，已经保留在异常列表。可以继续补充资料。",
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            return

        offer_id = str(row["offer_id"])
        review_id = str(row["review_id"])
        state["offer_id"], state["review_id"] = offer_id, review_id
        if str(row["offer_type"]) != "rent":
            self.repository.set_item(
                offer_id,
                state="exception",
                reason_code="sale_store_only",
                reason_text=ERROR_LABELS["sale_store_only"],
                origin="manual",
            )
            await update.effective_message.reply_text(
                "✅ 已识别为出售房源并保存归档。出售房源不会发布到租赁频道。",
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            return
        await self.prepare_manual_preview(
            update.effective_message,
            context,
            review_id=review_id,
            offer_id=offer_id,
        )

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
