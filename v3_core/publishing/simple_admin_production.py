"""Production admin extensions for the V3 Publisher.

The production controller keeps the current V3 publication pipeline intact while
presenting the operator with the compact, task-oriented console that proved
easier to use in the previous Publisher UI.  It also preserves the anomaly
repair behavior for no-offer canonical reviews.
"""
from __future__ import annotations

import asyncio
import json
from html import escape
from pathlib import Path
import sqlite3
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from v3_core.ingest.intake_service import SourceIntake
from .autopilot import ERROR_LABELS
from .autopilot_anomalies import FinalAutoPublishRepository, FinalAutoPublishService
from .simple_admin import NEW_LISTING_STATE_KEY, SimplePublisherAdminController
from .telegram_adapter import build_channel_keyboard


class ProductionSimplePublisherAdminController(SimplePublisherAdminController):
    repository: FinalAutoPublishRepository
    autopilot: FinalAutoPublishService

    @staticmethod
    def home_keyboard() -> InlineKeyboardMarkup:
        """Compact operator home: daily work first, settings last."""
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("➕ 发布房源", callback_data="v3smp|new"),
                    InlineKeyboardButton("🔵 房态管理", callback_data="v3smp|listings"),
                ],
                [
                    InlineKeyboardButton("📢 广播中心", callback_data="v3bc"),
                    InlineKeyboardButton("📡 采集源", callback_data="v3smp|sources"),
                ],
                [
                    InlineKeyboardButton("🧪 查看发布效果", callback_data="v3smp|preview"),
                    InlineKeyboardButton("📚 发布记录", callback_data="v3smp|logs"),
                ],
                [InlineKeyboardButton("⚙️ 发布设置", callback_data="v3smp|settings")],
            ]
        )

    async def show_home(self, message: Any) -> None:
        cfg = self.repository.config()
        await message.reply_text(
            "<b>📣 侨联发布助手</b>\n\n"
            "发布房源、修改房态、发送广播和管理采集源都从这里处理。\n"
            f"自动发布：<b>{'运行中' if cfg.enabled else '已暂停'}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=self.home_keyboard(),
        )

    async def show_settings(self, message: Any) -> None:
        cfg = self.repository.config()
        await message.reply_text(
            "<b>⚙️ 发布设置</b>\n\n"
            f"自动发布：<b>{'运行中' if cfg.enabled else '已暂停'}</b>\n"
            "运行状态、发帖时段和统计放在这里，不占用日常操作首页。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("🟢 运行状态", callback_data="v3smp|runtime"),
                        InlineKeyboardButton("🕒 发帖时段", callback_data="v3smp|windows"),
                    ],
                    [InlineKeyboardButton("📊 今日统计", callback_data="v3smp|stats")],
                    self.home_row(),
                ]
            ),
        )

    def _production_listing_rows(self, category: str, *, limit: int = 20) -> list[dict[str, Any]]:
        clean = str(category or "").strip().lower()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if clean == "recent":
                rows = conn.execute(
                    """SELECT l.listing_id,l.public_listing_id,l.display_title,l.project_name,
                              l.inventory_status,MAX(p.published_at) AS sort_at
                       FROM listings_v3 l
                       JOIN publication_instances p ON p.listing_id=l.listing_id
                       WHERE p.platform='telegram' AND p.publish_status='published'
                       GROUP BY l.listing_id,l.public_listing_id,l.display_title,
                                l.project_name,l.inventory_status
                       ORDER BY sort_at DESC LIMIT ?""",
                    (max(1, int(limit)),),
                ).fetchall()
            else:
                statuses = {
                    "active": ("active",),
                    "reserved": ("reserved",),
                    "rented": ("rented",),
                    "offline": ("inactive", "offline"),
                }.get(clean)
                if not statuses:
                    return []
                placeholders = ",".join("?" for _ in statuses)
                rows = conn.execute(
                    f"""SELECT listing_id,public_listing_id,display_title,project_name,
                               inventory_status,updated_at AS sort_at
                        FROM listings_v3
                        WHERE inventory_status IN ({placeholders})
                        ORDER BY updated_at DESC LIMIT ?""",
                    (*statuses, max(1, int(limit))),
                ).fetchall()
        return [dict(row) for row in rows]

    async def show_listing_categories(self, message: Any) -> None:
        await message.reply_text(
            "<b>🔵 房态管理</b>\n\n请选择要处理的房源状态：",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("🟢 当前可预约", callback_data="v3smp|ls|active"),
                        InlineKeyboardButton("🟡 已有预约", callback_data="v3smp|ls|reserved"),
                    ],
                    [
                        InlineKeyboardButton("🔴 已租出", callback_data="v3smp|ls|rented"),
                        InlineKeyboardButton("⚫ 已下架", callback_data="v3smp|ls|offline"),
                    ],
                    [
                        InlineKeyboardButton(
                            f"⚠️ 待处理异常 ({self.repository.exception_count()})",
                            callback_data="v3smp|exceptions",
                        ),
                        InlineKeyboardButton("🕘 最近发布", callback_data="v3smp|ls|recent"),
                    ],
                    self.home_row(),
                ]
            ),
        )

    async def show_listing_rows(self, message: Any, category: str) -> None:
        rows = self._production_listing_rows(category)
        labels = {
            "active": "🟢 当前可预约",
            "reserved": "🟡 已有预约",
            "rented": "🔴 已租出",
            "offline": "⚫ 已下架",
            "recent": "🕘 最近发布",
        }
        buttons: list[list[InlineKeyboardButton]] = []
        for row in rows:
            public_id = str(row.get("public_listing_id") or "房源")
            title = str(row.get("display_title") or row.get("project_name") or "未命名")
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"{public_id} · {title}"[:58],
                        callback_data=f"v3smp|listing|{row['listing_id']}",
                    )
                ]
            )
        buttons.append([InlineKeyboardButton("⬅️ 返回房态管理", callback_data="v3smp|listings")])
        buttons.append(self.home_row())
        await message.reply_text(
            f"<b>{escape(labels.get(str(category), str(category)))}</b>\n\n"
            + ("请选择房源：" if rows else "当前没有房源。"),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    async def show_listing(self, message: Any, listing_id: str) -> None:
        row = self._listing_detail(listing_id)
        public_id = str(row.get("public_listing_id") or "")
        status = str(row.get("inventory_status") or "").strip().lower()
        status_label = {
            "active": "🟢 当前可预约",
            "reserved": "🟡 已有预约",
            "rented": "🔴 已租出",
            "inactive": "⚫ 已下架",
            "offline": "⚫ 已下架",
        }.get(status, "🔵 待确认")
        rent = row.get("monthly_rent_usd")
        price = f"${int(rent):,}/月" if rent not in (None, "") else "未识别"
        rows: list[list[InlineKeyboardButton]] = []
        if public_id:
            rows.append(
                [
                    InlineKeyboardButton(
                        "🏠 查看租赁详情",
                        url=f"https://t.me/{self.user_bot_username}?start=property_{public_id}_details",
                    )
                ]
            )

        if status == "active":
            rows.append(
                [
                    InlineKeyboardButton(
                        "🟡 标记已有预约",
                        callback_data=f"v3smp|status|reserved|{listing_id}",
                    ),
                    InlineKeyboardButton(
                        "🔴 标记已租出",
                        callback_data=f"v3smp|status|rented|{listing_id}",
                    ),
                ]
            )
            rows.append(
                [InlineKeyboardButton("⚫ 标记已下架", callback_data=f"v3smp|status|offline|{listing_id}")]
            )
        elif status == "reserved":
            rows.append(
                [
                    InlineKeyboardButton(
                        "🟢 恢复可预约",
                        callback_data=f"v3smp|status|active|{listing_id}",
                    ),
                    InlineKeyboardButton(
                        "🔴 标记已租出",
                        callback_data=f"v3smp|status|rented|{listing_id}",
                    ),
                ]
            )
            rows.append(
                [InlineKeyboardButton("⚫ 标记已下架", callback_data=f"v3smp|status|offline|{listing_id}")]
            )
        elif status == "rented":
            rows.append(
                [
                    InlineKeyboardButton(
                        "🟢 恢复可预约",
                        callback_data=f"v3smp|status|active|{listing_id}",
                    ),
                    InlineKeyboardButton(
                        "⚫ 标记已下架",
                        callback_data=f"v3smp|status|offline|{listing_id}",
                    ),
                ]
            )
        else:
            rows.append(
                [InlineKeyboardButton("🟢 恢复可预约", callback_data=f"v3smp|status|active|{listing_id}")]
            )

        offer_id = str(row.get("offer_id") or "")
        if offer_id:
            rows.append(
                [InlineKeyboardButton("🔄 重新检查并发布", callback_data=f"v3smp|recheck|{offer_id}")]
            )
        rows.append([InlineKeyboardButton("⬅️ 返回房态管理", callback_data="v3smp|listings")])
        rows.append(self.home_row())
        await message.reply_text(
            f"<b>{escape(public_id or listing_id)}</b>\n"
            f"{escape(str(row.get('display_title') or row.get('project_name') or '未命名房源'))}\n\n"
            f"房态：{escape(status_label)}\n"
            f"租金：{escape(price)}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def show_publish_logs(self, message: Any) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT pi.listing_id,pi.channel_message_id,pi.published_at,
                          l.public_listing_id,l.display_title,l.project_name,l.inventory_status
                   FROM publication_instances pi
                   JOIN listings_v3 l ON l.listing_id=pi.listing_id
                   WHERE pi.platform='telegram' AND pi.publish_status='published'
                   ORDER BY pi.published_at DESC,pi.id DESC LIMIT 10"""
            ).fetchall()
        if not rows:
            text = "<b>📚 发布记录</b>\n\n目前还没有正式发布记录。"
        else:
            lines = ["<b>📚 最近发布</b>", ""]
            for row in rows:
                public_id = str(row["public_listing_id"] or "房源")
                title = str(row["display_title"] or row["project_name"] or "未命名")
                published_at = str(row["published_at"] or "")
                lines.append(f"• <b>{escape(public_id)}</b>｜{escape(title)}")
                if published_at:
                    lines.append(f"  {escape(published_at)}")
            text = "\n".join(lines)
        await message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🧪 查看最新发布效果", callback_data="v3smp|preview")],
                    [InlineKeyboardButton("🕘 最近发布房源", callback_data="v3smp|ls|recent")],
                    self.home_row(),
                ]
            ),
        )

    async def show_publish_preview(self, message: Any) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT pi.post_text AS current_post_text,p.post_text AS frozen_post_text,
                          p.cover_path,p.actions_json,l.inventory_status
                   FROM publication_instances pi
                   JOIN publication_packages_v3 p ON p.package_id=pi.package_id
                   JOIN listings_v3 l ON l.listing_id=pi.listing_id
                   WHERE pi.platform='telegram' AND pi.publish_status='published'
                   ORDER BY pi.published_at DESC,pi.id DESC LIMIT 1"""
            ).fetchone()
        if row is None:
            await message.reply_text(
                "<b>🧪 查看发布效果</b>\n\n目前还没有正式发布过的房源。",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            return

        caption = str(row["current_post_text"] or row["frozen_post_text"] or "").strip()
        try:
            decoded = json.loads(str(row["actions_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded = {}
        actions = {key: str(decoded.get(key) or "") for key in ("details", "photos", "book")}
        keyboard = build_channel_keyboard(actions, inventory_status=str(row["inventory_status"] or ""))
        keyboard_rows = list(keyboard.inline_keyboard)
        keyboard_rows.append(self.home_row())
        reply_markup = InlineKeyboardMarkup(keyboard_rows)
        cover = Path(str(row["cover_path"] or "")).expanduser()
        if cover.is_file():
            with cover.open("rb") as handle:
                await message.reply_photo(
                    photo=handle,
                    caption=caption[:1024],
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
        else:
            await message.reply_text(
                caption or "最新发布房源暂无可显示文案。",
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )

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
        if raw == "v3smp|settings":
            if query is None:
                return False
            await self.show_settings(query.message)
            return True
        if raw == "v3smp|preview":
            if query is None:
                return False
            await self.show_publish_preview(query.message)
            return True
        if raw == "v3smp|logs":
            if query is None:
                return False
            await self.show_publish_logs(query.message)
            return True
        if raw.startswith("v3smp|recheck|review:"):
            if query is None:
                return False
            token = raw.split("|", 2)[2]
            await self._recheck_review_exception(query.message, context, token)
            return True
        return await super().handle_callback(update, context)


__all__ = ["ProductionSimplePublisherAdminController"]
