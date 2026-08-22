
from __future__ import annotations

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .formatters import AREA_OPTIONS, TYPE_LABELS, deep_link

log = logging.getLogger(__name__)


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ 新建房源", callback_data="pub:new")],
            [InlineKeyboardButton("🧪 检查频道权限", callback_data="pub:test")],
            [InlineKeyboardButton("❌ 取消当前流程", callback_data="pub:cancel")],
        ]
    )


def admin_menu() -> InlineKeyboardMarkup:
    """批量主线优先；单套补录仅作为新增入口。"""
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⚡ 批量生成以前微信房源", callback_data="cmd:batch_generate")],
            [InlineKeyboardButton("➕ 导入新的微信房源", callback_data="cmd:intake")],
            [InlineKeyboardButton("👀 查看生成结果", callback_data="cmd:pending")],
            [InlineKeyboardButton("📤 发布已确认房源", callback_data="cmd:send_queue")],
            [InlineKeyboardButton("❓ 怎么用", callback_data="cmd:quick_help")],
        ]
    )



def type_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("🏢 公寓", callback_data="type:apartment"),
            InlineKeyboardButton("🏡 别墅", callback_data="type:villa"),
        ],
        [
            InlineKeyboardButton("🏪 商铺", callback_data="type:shop"),
            InlineKeyboardButton("💼 办公室", callback_data="type:office"),
        ],
        [InlineKeyboardButton("❌ 取消", callback_data="pub:cancel")],
    ]
    return InlineKeyboardMarkup(rows)


def area_keyboard() -> InlineKeyboardMarkup:
    rows = []
    options = list(AREA_OPTIONS.keys())
    for i in range(0, len(options), 2):
        row = [InlineKeyboardButton(x, callback_data=f"area:{x}") for x in options[i : i + 2]]
        rows.append(row)
    rows.append([InlineKeyboardButton("❌ 取消", callback_data="pub:cancel")])
    return InlineKeyboardMarkup(rows)


def skip_keyboard(back: bool = False) -> InlineKeyboardMarkup:
    row = [InlineKeyboardButton("⏭ 跳过", callback_data="skip")]
    if back:
        row.append(InlineKeyboardButton("❌ 取消", callback_data="pub:cancel"))
    return InlineKeyboardMarkup([row])


def preview_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ 立即发布", callback_data="preview:publish"),
                InlineKeyboardButton("✏️ 修改字段", callback_data="preview:edit"),
            ],
            [InlineKeyboardButton("🎨 切换封面模板", callback_data="preview:style")],
            [InlineKeyboardButton("❌ 取消", callback_data="pub:cancel")],
        ]
    )


def edit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("标题", callback_data="edit:title"),
                InlineKeyboardButton("价格", callback_data="edit:price"),
            ],
            [
                InlineKeyboardButton("区域", callback_data="edit:area"),
                InlineKeyboardButton("亮点", callback_data="edit:highlights"),
            ],
            [
                InlineKeyboardButton("费用", callback_data="edit:fee_note"),
                InlineKeyboardButton("缺点/提醒", callback_data="edit:advisor_note"),
            ],
            [InlineKeyboardButton("⬅️ 返回预览", callback_data="edit:done")],
        ]
    )


def publish_post_keyboard(
    listing_id: str,
    area: str,
    user_bot_username: str,
    detail_url: str | None = None,
    maps_url: str | None = None,
    channel_username: str = "",
    channel_message_id: int | None = None,
    discussion_group_link: str = "",
    post_token: str = "",
) -> InlineKeyboardMarkup:
    """频道房源帖统一四动作，并为“更多实拍”提供可靠降级。"""
    def payload(action: str) -> str:
        if post_token:
            return f"{action}__{post_token}__{listing_id}"
        return f"{action}_{listing_id}"

    book_url = deep_link(user_bot_username, payload("book"))
    consult_url = deep_link(user_bot_username, payload("consult"))
    similar_url = deep_link(user_bot_username, payload("similar"))
    clean_channel = str(channel_username or "").strip().lstrip("@")
    if clean_channel and channel_message_id:
        media_url = f"https://t.me/{clean_channel}/{int(channel_message_id)}?comment=1"
    elif str(discussion_group_link or "").strip():
        media_url = str(discussion_group_link).strip()
    else:
        media_url = similar_url
        log.warning(
            "频道评论链接未配置，更多实拍按钮已安全降级到类似房源：listing_id=%s",
            listing_id,
        )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📅 预约", url=book_url),
                InlineKeyboardButton("💬 问顾问", url=consult_url),
            ],
            [
                InlineKeyboardButton("🖼 更多实拍", url=media_url),
                InlineKeyboardButton("🔍 类似房源", url=similar_url),
            ],
        ]
    )
