
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
    """管理员主面板：只保留高频、可执行的动作。"""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➕ 新建房源", callback_data="pub:new"),
                InlineKeyboardButton("📋 待审预览", callback_data="cmd:pending"),
            ],
            [
                InlineKeyboardButton("🚀 立即发布", callback_data="cmd:send_help"),
                InlineKeyboardButton("⚡ 一屏总览", callback_data="cmd:ops"),
            ],
            [
                InlineKeyboardButton("📡 运行状态", callback_data="cmd:status"),
                InlineKeyboardButton("⏱ 发帖时段", callback_data="cmd:slots"),
            ],
            [
                InlineKeyboardButton("⏸ 暂停队列", callback_data="cmd:pause"),
                InlineKeyboardButton("▶️ 恢复队列", callback_data="cmd:resume"),
            ],
            [
                InlineKeyboardButton("📥 微信导入", callback_data="cmd:intake"),
                InlineKeyboardButton("📋 导入草稿", callback_data="cmd:intake_pending"),
            ],
            [
                InlineKeyboardButton("🧾 最近日志", callback_data="cmd:logs"),
                InlineKeyboardButton("🎨 封面测试", callback_data="cmd:cover_test"),
            ],
            [InlineKeyboardButton("❓ 命令速查", callback_data="cmd:quick_help")],
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
            [
                InlineKeyboardButton("🎨 切换封面模板", callback_data="preview:style"),
                InlineKeyboardButton("🧪 多文案对比发布", callback_data="preview:publish_variants"),
            ],
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
) -> InlineKeyboardMarkup:
    """频道房源帖按钮，升级为 4 个。

    第一排：📅 预约看房 | 💎 问问顾问
    第二排：🖼 更多实拍/评论区 | 🔍 找类似房源

    评论区链接优先使用 channel_username + channel_message_id。
    降级策略：CHANNEL_USERNAME 缺失时使用 discussion_group_link；
    两者均无时「🖼 更多实拍/评论区」使用「找类似房源」链接兜底，保持 4 按钮布局。
    """
    book_url = deep_link(user_bot_username, f"book_{listing_id}")
    consult_url = deep_link(user_bot_username, f"consult_{listing_id}")
    similar_url = deep_link(user_bot_username, f"similar_{listing_id}")

    # 评论区链接生成（三级优先）
    _ch_user = (channel_username or "").strip().lstrip("@")
    if _ch_user and channel_message_id:
        comment_url: str = f"https://t.me/{_ch_user}/{channel_message_id}?comment=1"
    elif discussion_group_link:
        comment_url = discussion_group_link
    else:
        # 兜底：使用「找类似房源」深链，避免按钮缺失；同时记录警告提示运维配置
        log.warning(
            "[publish_post_keyboard] listing=%s: 无 channel_message_id 且无 DISCUSSION_GROUP_LINK，"
            "「🖼 更多实拍/评论区」将降级为「找类似房源」链接。请在 .env 配置 DISCUSSION_GROUP_LINK。",
            listing_id,
        )
        comment_url = similar_url

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📅 预约看房", url=book_url),
                InlineKeyboardButton("💎 问问顾问", url=consult_url),
            ],
            [
                InlineKeyboardButton("🖼 更多实拍/评论区", url=comment_url),
                InlineKeyboardButton("🔍 找类似房源", url=similar_url),
            ],
        ]
    )
