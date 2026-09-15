"""Ten-at-a-time pending inventory review for the Publisher operator UI.

Newly collected listings already enter ``pending``.  This controller keeps the
existing publishing flow intact and adds a fast operator path for reviewing ten
pending listings at a time.  Batch writes are guarded with
``inventory_status='pending'`` so a listing changed to rented/offline elsewhere
cannot be accidentally reopened by a stale batch screen.
"""
from __future__ import annotations

from html import escape
import sqlite3
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .operator_flow import OperatorPublisherAdminController


PENDING_BATCH_SIZE = 10
BATCH_STATUS_SYNC_KEY = "v3_pending_batch_status_sync"


class PendingBatchOperatorPublisherAdminController(OperatorPublisherAdminController):
    """Operator controller with a ten-listing pending review queue."""

    def _pending_count(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM listings_v3 WHERE inventory_status='pending'"
            ).fetchone()
        return int(row[0] or 0) if row else 0

    def _pending_batch_rows(self, page: int = 0) -> list[dict[str, Any]]:
        clean_page = max(0, int(page or 0))
        offset = clean_page * PENDING_BATCH_SIZE
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT l.listing_id,l.public_listing_id,l.display_title,l.project_name,
                          l.inventory_status,l.updated_at,
                          (SELECT o.monthly_rent_usd
                             FROM listing_offers o
                            WHERE o.listing_id=l.listing_id
                              AND o.offer_type='rent'
                              AND o.offer_status='active'
                            ORDER BY o.rowid DESC LIMIT 1) AS monthly_rent_usd
                     FROM listings_v3 l
                    WHERE l.inventory_status='pending'
                    ORDER BY l.updated_at ASC,l.listing_id ASC
                    LIMIT ? OFFSET ?""",
                (PENDING_BATCH_SIZE, offset),
            ).fetchall()
        return [dict(row) for row in rows]

    def _status_counts(self) -> dict[str, int]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """SELECT inventory_status,COUNT(*)
                     FROM listings_v3
                    GROUP BY inventory_status"""
            ).fetchall()
        raw = {str(status or "").strip().lower(): int(count or 0) for status, count in rows}
        return {
            "pending": raw.get("pending", 0),
            "active": raw.get("active", 0),
            "reserved": raw.get("reserved", 0),
            "rented": raw.get("rented", 0),
            "offline": raw.get("offline", 0) + raw.get("inactive", 0),
        }

    async def show_listing_categories(self, message: Any) -> None:
        counts = self._status_counts()
        pending = counts["pending"]
        pending_label = (
            f"🔵 待确认 {pending}｜10套一组" if pending else "✅ 暂无待确认房源"
        )
        await message.reply_text(
            "<b>🔵 房态管理</b>\n\n"
            "新房源默认进入待确认。先按 10 套一组批量处理，需要例外时再单独处理。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton(pending_label, callback_data="v3smp|pbat|0")],
                    [
                        InlineKeyboardButton(
                            f"🟢 可预约 {counts['active']}", callback_data="v3smp|ls|active"
                        ),
                        InlineKeyboardButton(
                            f"🟡 已有预约 {counts['reserved']}", callback_data="v3smp|ls|reserved"
                        ),
                    ],
                    [
                        InlineKeyboardButton(
                            f"🔴 已租出 {counts['rented']}", callback_data="v3smp|ls|rented"
                        ),
                        InlineKeyboardButton(
                            f"⚫ 已下架 {counts['offline']}", callback_data="v3smp|ls|offline"
                        ),
                    ],
                    [InlineKeyboardButton("🕘 最近发布", callback_data="v3smp|ls|recent")],
                    self.home_row(),
                ]
            ),
        )

    @staticmethod
    def _row_line(index: int, row: dict[str, Any]) -> str:
        public_id = str(row.get("public_listing_id") or row.get("listing_id") or "房源")
        title = str(row.get("display_title") or row.get("project_name") or "未命名房源")
        rent = row.get("monthly_rent_usd")
        if rent in (None, ""):
            price = ""
        else:
            try:
                price = f"｜${int(float(rent)):,}"
            except (TypeError, ValueError):
                price = f"｜${rent}"
        return f"{index}. <b>{escape(public_id)}</b>｜{escape(title[:28])}{escape(price)}"

    async def show_pending_batch(self, message: Any, page: int = 0) -> None:
        total = self._pending_count()
        if total <= 0:
            await message.reply_text(
                "<b>🔵 待确认房源</b>\n\n✅ 当前没有待确认房源。",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ 返回房态管理", callback_data="v3smp|listings")], self.home_row()]
                ),
            )
            return

        max_page = max(0, (total - 1) // PENDING_BATCH_SIZE)
        clean_page = min(max(0, int(page or 0)), max_page)
        rows = self._pending_batch_rows(clean_page)
        start = clean_page * PENDING_BATCH_SIZE + 1
        end = start + len(rows) - 1
        lines = [
            "<b>🔵 待确认房源</b>",
            "",
            f"第 {clean_page + 1} 组｜{start}–{end}",
            f"待确认共 {total} 套",
            "",
        ]
        lines.extend(self._row_line(start + index, row) for index, row in enumerate(rows))
        current_count = len(rows)
        buttons: list[list[InlineKeyboardButton]] = [
            [
                InlineKeyboardButton(
                    f"✅ 这{current_count}套全部可租",
                    callback_data=f"v3smp|pbapply|active|{clean_page}",
                )
            ],
            [
                InlineKeyboardButton(
                    f"⚫ 这{current_count}套全部下架",
                    callback_data=f"v3smp|pbapply|offline|{clean_page}",
                )
            ],
            [InlineKeyboardButton("🔎 单独处理本组", callback_data=f"v3smp|pbpick|{clean_page}")],
        ]
        nav: list[InlineKeyboardButton] = []
        if clean_page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一组", callback_data=f"v3smp|pbat|{clean_page - 1}"))
        if clean_page < max_page:
            nav.append(InlineKeyboardButton("下一组 ➡️", callback_data=f"v3smp|pbat|{clean_page + 1}"))
        if nav:
            buttons.append(nav)
        buttons.append([InlineKeyboardButton("⬅️ 返回房态管理", callback_data="v3smp|listings")])
        buttons.append(self.home_row())
        await message.reply_text(
            "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons)
        )

    async def show_pending_pick(self, message: Any, page: int = 0) -> None:
        total = self._pending_count()
        if total <= 0:
            await self.show_pending_batch(message, 0)
            return
        max_page = max(0, (total - 1) // PENDING_BATCH_SIZE)
        clean_page = min(max(0, int(page or 0)), max_page)
        rows = self._pending_batch_rows(clean_page)
        start = clean_page * PENDING_BATCH_SIZE + 1
        buttons: list[list[InlineKeyboardButton]] = []
        for index, row in enumerate(rows):
            public_id = str(row.get("public_listing_id") or row.get("listing_id") or "房源")
            title = str(row.get("display_title") or row.get("project_name") or "未命名")
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"{start + index}. {public_id} · {title}"[:58],
                        callback_data=f"v3smp|pbone|{row['listing_id']}|{clean_page}",
                    )
                ]
            )
        buttons.append([InlineKeyboardButton("⬅️ 返回本组", callback_data=f"v3smp|pbat|{clean_page}")])
        buttons.append(self.home_row())
        await message.reply_text(
            "<b>🔎 单独处理本组</b>\n\n选择需要单独设置的房源：",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    async def show_pending_one(self, message: Any, listing_id: str, page: int = 0) -> None:
        row = self._listing_detail(str(listing_id))
        public_id = str(row.get("public_listing_id") or listing_id)
        title = str(row.get("display_title") or row.get("project_name") or "未命名房源")
        status = str(row.get("inventory_status") or "").strip().lower()
        if status != "pending":
            await message.reply_text(
                f"<b>{escape(public_id)}</b>｜{escape(title)}\n\n"
                "这套房的房态已经被其他操作修改，不再属于待确认。",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ 返回本组", callback_data=f"v3smp|pbat|{max(0, int(page or 0))}")], self.home_row()]
                ),
            )
            return
        await message.reply_text(
            f"<b>{escape(public_id)}</b>\n{escape(title)}\n\n房态：🔵 待确认",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🟢 确认可租",
                            callback_data=f"v3smp|pboneapply|active|{listing_id}|{max(0, int(page or 0))}",
                        ),
                        InlineKeyboardButton(
                            "🔴 标记已租出",
                            callback_data=f"v3smp|pboneapply|rented|{listing_id}|{max(0, int(page or 0))}",
                        ),
                    ],
                    [
                        InlineKeyboardButton(
                            "⚫ 标记已下架",
                            callback_data=f"v3smp|pboneapply|offline|{listing_id}|{max(0, int(page or 0))}",
                        )
                    ],
                    [InlineKeyboardButton("⬅️ 返回本组", callback_data=f"v3smp|pbat|{max(0, int(page or 0))}")],
                    self.home_row(),
                ]
            ),
        )

    def _apply_pending_batch(self, status: str, page: int = 0) -> tuple[list[str], list[str]]:
        clean_status = str(status or "").strip().lower()
        if clean_status not in {"active", "offline"}:
            raise ValueError("unsupported pending batch status")
        rows = self._pending_batch_rows(max(0, int(page or 0)))
        updated: list[str] = []
        skipped: list[str] = []
        with sqlite3.connect(self.db_path) as conn:
            for row in rows:
                listing_id = str(row["listing_id"])
                cur = conn.execute(
                    """UPDATE listings_v3
                          SET inventory_status=?,updated_at=CURRENT_TIMESTAMP
                        WHERE listing_id=? AND inventory_status='pending'""",
                    (clean_status, listing_id),
                )
                (updated if cur.rowcount == 1 else skipped).append(listing_id)
            conn.commit()
        return updated, skipped

    def _apply_pending_one(self, listing_id: str, status: str) -> bool:
        clean_status = str(status or "").strip().lower()
        if clean_status not in {"active", "rented", "offline"}:
            raise ValueError("unsupported pending status")
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """UPDATE listings_v3
                      SET inventory_status=?,updated_at=CURRENT_TIMESTAMP
                    WHERE listing_id=? AND inventory_status='pending'""",
                (clean_status, str(listing_id)),
            )
            conn.commit()
        return cur.rowcount == 1

    async def _show_batch_result(
        self,
        message: Any,
        *,
        status: str,
        updated: list[str],
        skipped: list[str],
        page: int,
    ) -> None:
        labels = {"active": "🟢 可租", "offline": "⚫ 已下架"}
        remaining = self._pending_count()
        lines = [
            "<b>✅ 本组已处理</b>",
            "",
            f"{labels.get(status, status)}：{len(updated)} 套",
            f"⏭ 已跳过：{len(skipped)} 套",
            f"🔵 待确认剩余：{remaining} 套",
        ]
        if skipped:
            lines.extend(["", "跳过的是执行时已经不再处于待确认的房源。"])
        buttons: list[list[InlineKeyboardButton]] = []
        if remaining:
            max_page = max(0, (remaining - 1) // PENDING_BATCH_SIZE)
            next_page = min(max(0, int(page or 0)), max_page)
            buttons.append(
                [InlineKeyboardButton("继续下一组 ➡️", callback_data=f"v3smp|pbat|{next_page}")]
            )
        buttons.append([InlineKeyboardButton("⬅️ 返回房态管理", callback_data="v3smp|listings")])
        buttons.append(self.home_row())
        await message.reply_text(
            "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons)
        )

    async def handle_callback(self, update: Any, context: Any) -> bool:
        query = getattr(update, "callback_query", None)
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        if not raw.startswith("v3smp|"):
            return False
        parts = raw.split("|")
        action = parts[1] if len(parts) > 1 else ""

        if action == "pbat" and len(parts) == 3:
            await self.show_pending_batch(query.message, int(parts[2]))
            return True
        if action == "pbpick" and len(parts) == 3:
            await self.show_pending_pick(query.message, int(parts[2]))
            return True
        if action == "pbone" and len(parts) == 4:
            await self.show_pending_one(query.message, parts[2], int(parts[3]))
            return True
        if action == "pbapply" and len(parts) == 4:
            status, page = parts[2], int(parts[3])
            updated, skipped = self._apply_pending_batch(status, page)
            if updated:
                queue = context.user_data.setdefault(BATCH_STATUS_SYNC_KEY, [])
                queue.extend({"listing_id": listing_id, "status": status} for listing_id in updated)
            await self._show_batch_result(
                query.message, status=status, updated=updated, skipped=skipped, page=page
            )
            return True
        if action == "pboneapply" and len(parts) == 5:
            status, listing_id, page = parts[2], parts[3], int(parts[4])
            changed = self._apply_pending_one(listing_id, status)
            if changed:
                queue = context.user_data.setdefault(BATCH_STATUS_SYNC_KEY, [])
                queue.append({"listing_id": listing_id, "status": status})
                await query.message.reply_text("✅ 房态已更新。")
            else:
                await query.message.reply_text("⏭ 这套房的房态已经变化，本次没有覆盖。")
            await self.show_pending_batch(query.message, page)
            return True

        return await super().handle_callback(update, context)


__all__ = [
    "BATCH_STATUS_SYNC_KEY",
    "PENDING_BATCH_SIZE",
    "PendingBatchOperatorPublisherAdminController",
]
