
from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .formatters import AREA_OPTIONS, TYPE_LABELS, deep_link


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
) -> InlineKeyboardMarkup:
    # 频道卡片按钮：预约看房 + 问这套，可选 📍 查看位置。
    ask_url = detail_url or deep_link(user_bot_username, f"consult_{listing_id}")
    rows = [
        [
            InlineKeyboardButton("📅 预约看房", url=deep_link(user_bot_username, f"appoint_{listing_id}")),
            InlineKeyboardButton("💬 问这套", url=ask_url),
        ],
    ]
    if maps_url:
        rows.append([InlineKeyboardButton("📍 查看位置", url=maps_url)])
    return InlineKeyboardMarkup(rows)
