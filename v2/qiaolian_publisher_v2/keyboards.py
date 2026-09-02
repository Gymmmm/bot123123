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
    """Six plain-language entry points for the normal publishing workflow."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🏠 待发布房源", callback_data="cmd:queue"),
                InlineKeyboardButton("🟢 房态管理", callback_data="cmd:listing_states"),
            ],
            [
                InlineKeyboardButton("➕ 添加房源", callback_data="cmd:intake"),
                InlineKeyboardButton("📡 采集源", callback_data="cmd:sources"),
            ],
            [
                InlineKeyboardButton("📢 每日广播", callback_data="cmd:daily"),
                InlineKeyboardButton("📚 发布记录", callback_data="cmd:logs"),
            ],
            [
                InlineKeyboardButton("⚙️ 运营设置", callback_data="cmd:settings_hub"),
                InlineKeyboardButton("❓ 使用帮助", callback_data="cmd:quick_help"),
            ],
        ]
    )


def type_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("🏢 公寓", callback_data="type:apartment"),
            InlineKeyboardButton("🏡 别墅", callback_data="type:villa"),
        ],
        [
            InlineKeyboardButton("🏘 排屋", callback_data="type:townhouse"),
            InlineKeyboardButton("🏪 商铺", callback_data="type:shop"),
        ],
        [InlineKeyboardButton("💼 办公室", callback_data="type:office")],
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
                InlineKeyboardButton("✅ 保存到待审", callback_data="preview:publish"),
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
    """频道房源帖固定三个动作：租赁详情、更多实拍、预约看房。"""
    _ = (area, detail_url, maps_url, channel_username, channel_message_id, discussion_group_link)

    def payload(action: str) -> str:
        if post_token:
            return f"{action}__{post_token}__{listing_id}"
        return f"{action}_{listing_id}"

    detail_link = deep_link(user_bot_username, payload("detail"))
    photos_link = deep_link(user_bot_username, payload("photos"))
    book_link = deep_link(user_bot_username, payload("book"))

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📋 租赁详情", url=detail_link),
                InlineKeyboardButton("📸 更多实拍", url=photos_link),
            ],
            [InlineKeyboardButton("📅 预约看房", url=book_link)],
        ]
    )
