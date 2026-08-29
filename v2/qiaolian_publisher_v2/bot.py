from __future__ import annotations
import asyncio
import hashlib
import logging
import os
import sys
import uuid
import json
import time
import sqlite3
import random
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from telegram import BotCommand, BotCommandScopeChat, InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.error import NetworkError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
from .config import Settings, get_settings
from .db import Database
from .formatters import (
    CHANNEL_BUTTON_PROMPT,
    TYPE_LABELS,
    build_post_text,
    build_preview_text,
    normalize_tags,
)
from .keyboards import (
    area_keyboard,
    admin_menu,
    edit_keyboard,
    main_menu,
    preview_keyboard,
    skip_keyboard,
    type_keyboard,
)
from . import messages
from .extractor import extract_house_info
# v2 手搓封面单独模块，避免占用 cover_generator 包名（与 meihua CoverGenerator 冲突）
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from discussion_map_store import load_discuss_map, save_discuss_map
from meihua_publisher import add_detail_logo_watermark, build_chinese_listing_post
from db import DatabaseManager as CoreDatabaseManager

for _cover_module_dir in (
    _REPO_ROOT / "v2_admin",
    Path("/opt/qiaolian_dual_bots/v2_admin"),
):
    if _cover_module_dir.exists():
        sys.path.append(str(_cover_module_dir))
from house_cover_v2 import generate_house_cover

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
# httpx logs full Bot API URLs, which include Telegram credentials in the path.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
DISCUSSION_BRIDGE_FILE = Path(
    os.getenv("DISCUSSION_BRIDGE_FILE", "/opt/qiaolian_dual_bots/data/discussion_bridge.json")
)


def _default_discussion_bridge() -> dict:
    return {"publish_queue": [], "discuss_mgid": {}}


def load_discussion_bridge() -> dict:
    if DISCUSSION_BRIDGE_FILE.exists():
        try:
            with open(DISCUSSION_BRIDGE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data.setdefault("publish_queue", [])
                    data.setdefault("discuss_mgid", {})
                    if not isinstance(data["publish_queue"], list):
                        data["publish_queue"] = []
                    if not isinstance(data["discuss_mgid"], dict):
                        data["discuss_mgid"] = {}
                    return data
        except Exception:
            logger.exception("读取 discussion_bridge 失败")
    return _default_discussion_bridge()


def save_discussion_bridge(data: dict) -> None:
    DISCUSSION_BRIDGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DISCUSSION_BRIDGE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


(
    ST_MEDIA,
    ST_TYPE,
    ST_AREA,
    ST_TITLE,
    ST_PRICE,
    ST_COMMUNITY,
    ST_LAYOUT,
    ST_SIZE,
    ST_TAGS,
    ST_HIGHLIGHTS,
    ST_FEE,
    ST_ADVISOR,
    ST_DEPOSIT,
    ST_AVAIL,
    ST_PREVIEW,
    ST_EDIT_VALUE,
) = range(16)

@dataclass
class Draft:
    listing_id: str
    property_type: str = ""
    area: str = ""
    title: str = ""
    price: str = ""
    community: str = ""
    layout: str = ""
    size_sqm: str = ""
    tags: list[str] = field(default_factory=list)
    highlights: list[str] = field(default_factory=list)
    fee_note: str = ""
    advisor_note: str = ""
    deposit_rule: str = ""
    available_date: str = ""
    media_type: str = ""
    media_file_id: str = ""
    media_file_ids: list[str] = field(default_factory=list)
    source_caption: str = ""
    cover_style: str = "minimal"  # classic | minimal | price_tag | vertical
    google_maps_url: str = ""  # 留接口：可手填精确链接，空则自动生成搜索链接

    def to_dict(self, user_id: int) -> dict[str, Any]:
        return {
            "listing_id": self.listing_id,
            "property_type": self.property_type,
            "area": self.area,
            "title": self.title,
            "price": self.price,
            "community": self.community,
            "layout": self.layout,
            "size_sqm": self.size_sqm,
            "tags": self.tags,
            "highlights": self.highlights,
            "fee_note": self.fee_note,
            "advisor_note": self.advisor_note,
            "deposit_rule": self.deposit_rule,
            "available_date": self.available_date,
            "media_type": self.media_type,
            "media_file_id": self.media_file_id,
            "media_file_ids": list(self.media_file_ids),
            "source_caption": self.source_caption,
            "created_by": user_id,
            "status": "active",
        }

class PublisherBot:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.sqlite_path)
        self._channel_chat_id: int | None = None

    @staticmethod
    def _build_maps_url(project: str, area: str, custom_url: str = "") -> str | None:
        """仅在手填链接时返回地图 URL，默认不再自动生成搜索链接。"""
        _ = (project, area)
        if custom_url and custom_url.startswith("http"):
            return custom_url
        return None

    def _is_admin(self, update: Update) -> bool:
        user = update.effective_user
        return bool(user and user.id in self.settings.admin_ids)

    async def _ensure_admin(self, update: Update) -> bool:
        user = update.effective_user
        chat = update.effective_chat
        # 忽略机器人账号消息，避免在讨论组被机器人互相触发
        if user and user.is_bot:
            return False
        # 采集/发布向导只允许在管理员私聊中运行。公开讨论组只用于
        # 捕获频道自动转发并建立映射，不回复任何后台流程文案。
        if self._is_admin(update) and chat and chat.type == "private":
            return True
        if chat and chat.type != "private":
            return False
        target = update.effective_message or update.callback_query.message
        await target.reply_text("⛔ 你没有权限使用这个发布 Bot。")
        return False

    def _draft(self, context: ContextTypes.DEFAULT_TYPE) -> Draft:
        if "draft" not in context.user_data:
            context.user_data["draft"] = Draft(listing_id=self.db.next_listing_id())
        return context.user_data["draft"]

    @staticmethod
    def _runtime_render_dir() -> Path:
        """选择一个当前进程可写的临时渲染目录。"""
        candidates: list[Path] = []
        env_dir = str(os.getenv("QIAOLIAN_RENDER_TMP", "")).strip()
        if env_dir:
            candidates.append(Path(env_dir).expanduser())
        candidates.extend(
            [
                Path("/opt/qiaolian_dual_bots/media/renders/runtime"),
                Path("/tmp/qiaolian"),
            ]
        )
        for p in candidates:
            try:
                p.mkdir(parents=True, exist_ok=True)
                probe = p / ".write_probe"
                probe.write_text("ok", encoding="utf-8")
                probe.unlink(missing_ok=True)
                return p
            except Exception:
                continue
        raise PermissionError("没有可写的封面渲染目录（QIAOLIAN_RENDER_TMP / /opt / /tmp 均不可写）")

    async def _resolve_cover_background(self, msg: Message, draft: Draft, out_dir: Path) -> str:
        """优先下载管理员刚上传的实拍图，作为封面背景底图。"""
        if draft.media_type != "photo" or not str(draft.media_file_id or "").strip():
            return ""
        suffix = str(draft.media_file_id)[-10:].replace("/", "_")
        bg_path = out_dir / f"cover_bg_{draft.listing_id}_{suffix}.jpg"
        if bg_path.exists() and bg_path.stat().st_size > 0:
            return str(bg_path)
        try:
            tf = await msg.get_bot().get_file(draft.media_file_id)
            await tf.download_to_drive(custom_path=str(bg_path))
            if bg_path.exists() and bg_path.stat().st_size > 0:
                return str(bg_path)
        except Exception as e:
            logger.warning("下载封面底图失败，回退纯模板: %s", e)
        return ""

    async def capture_discussion_forward(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """
        捕获频道自动转发到讨论组的消息，建立：
        channel_post_id -> discussion_msg_id 映射。
        """
        msg = update.effective_message
        if not msg or not getattr(msg, "is_automatic_forward", False):
            return

        # 新版 Telegram API 把自动转发来源放到 forward_origin；旧字段仍作兼容。
        origin = getattr(msg, "forward_origin", None)
        sender_chat = getattr(msg, "sender_chat", None) or getattr(origin, "chat", None)
        if not sender_chat:
            return

        try:
            if self._channel_chat_id is None:
                channel_chat = await context.bot.get_chat(self.settings.channel_id)
                self._channel_chat_id = channel_chat.id
        except Exception:
            logger.exception("读取频道信息失败")
            return

        if self._channel_chat_id is not None and sender_chat.id != self._channel_chat_id:
            return

        now = time.time()
        bridge = load_discussion_bridge()
        q = bridge["publish_queue"]
        while q and now - float(q[0].get("t", 0)) > 120:
            q.pop(0)

        channel_post_id = getattr(msg, "forward_from_message_id", None)
        if channel_post_id is None:
            channel_post_id = getattr(origin, "message_id", None)
        if channel_post_id is None:
            channel_post_id = getattr(msg, "message_thread_id", None)

        mgid = getattr(msg, "media_group_id", None)
        mgid_d = str(mgid) if mgid is not None else None

        # 讨论区相册的 media_group_id 与频道侧不同：同组后续消息用 discuss_mgid 反查
        if channel_post_id is None and mgid_d:
            slot = bridge["discuss_mgid"].get(mgid_d)
            if slot:
                try:
                    if now - float(slot.get("t", 0)) < 7200:
                        channel_post_id = int(slot["channel_post_id"])
                except (TypeError, ValueError):
                    channel_post_id = None

        # 首条自动转发：用最近 120s 内的发帖队列对齐频道首帖 id
        if channel_post_id is None:
            if q and now - float(q[0].get("t", 0)) <= 120:
                try:
                    channel_post_id = int(q[0]["channel_post_id"])
                    q.pop(0)
                    if mgid_d:
                        bridge["discuss_mgid"][mgid_d] = {
                            "channel_post_id": channel_post_id,
                            "t": now,
                        }
                except (TypeError, ValueError, KeyError):
                    channel_post_id = None

        if channel_post_id is None:
            logger.warning(
                "自动转发无法解析频道帖 id（无 forward_from/message_thread，讨论mgid=%s，队列为空或超时）",
                mgid_d,
            )
            return

        for k, v in list(bridge["discuss_mgid"].items()):
            try:
                if now - float(v.get("t", 0)) > 7200:
                    del bridge["discuss_mgid"][k]
            except (TypeError, ValueError):
                del bridge["discuss_mgid"][k]

        mapping = load_discuss_map()
        sk = str(channel_post_id)
        cur = mapping.get(sk)
        mid = msg.message_id
        if cur is None or mid < int(cur):
            mapping[sk] = mid
        try:
            with sqlite3.connect(self.settings.sqlite_path) as conn:
                conn.execute(
                    """
                    UPDATE posts
                    SET discuss_chat_id=?, discuss_thread_id=?, discuss_message_id=?, updated_at=CURRENT_TIMESTAMP
                    WHERE channel_message_id=?
                    """,
                    (
                        str(msg.chat_id),
                        str(getattr(msg, "message_thread_id", "") or ""),
                        str(msg.message_id),
                        str(channel_post_id),
                    ),
                )
                conn.commit()
        except Exception:
            logger.exception("回写 posts discussion 映射失败: channel_post_id=%s", channel_post_id)
        save_discuss_map(mapping)
        save_discussion_bridge(bridge)
        logger.info(
            "已记录评论映射: channel_post_id=%s -> discussion_msg_id=%s",
            channel_post_id,
            msg.message_id,
        )

    def _reset_draft(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        context.user_data.pop("draft", None)
        context.user_data.pop("edit_field", None)
        context.user_data.pop("_album_msgs", None)
        context.user_data.pop("_mg_gen", None)

    async def _public_channel_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """频道帖按钮深链由 USER_BOT_USERNAME 指向面向租客的用户 Bot。"""
        if self._is_admin(update):
            return False
        args = context.args or []
        if not args:
            return False
        arg = args[0]
        from html import escape as he

        m = update.effective_message
        brand_name = os.getenv("BRAND_NAME", "侨联地产")

        if arg.startswith("consult_"):
            lid = arg.replace("consult_", "", 1)
            await m.reply_text(
                f"💬 <b>咨询房源</b>\n编号：<code>{he(lid)}</code>\n\n"
                "请直接发一条消息说明问题，顾问会人工回复。",
                parse_mode=ParseMode.HTML,
            )
            return True
        if arg.startswith("appoint_"):
            lid = arg.replace("appoint_", "", 1)
            await m.reply_text(
                f"📅 <b>预约看房</b>\n编号：<code>{he(lid)}</code>\n\n"
                "请回复：意向日期、上午或下午、线下或视频看房。\n"
                "也可留下方便联系的方式。",
                parse_mode=ParseMode.HTML,
            )
            return True
        if arg.startswith("fav_"):
            await m.reply_text(
                "❤️ <b>收藏意向</b>\n已收到。需要锁房或视频带看请直接留言。",
                parse_mode=ParseMode.HTML,
            )
            return True
        if arg.startswith("more_"):
            area = arg.replace("more_", "", 1)
            await m.reply_text(
                f"🏠 <b>同区域更多</b>\n区域：<b>{he(area)}</b>\n\n"
                "请发预算（USD/月）与户型，顾问按实拍房源推荐。",
                parse_mode=ParseMode.HTML,
            )
            return True
        if arg == "brand":
            await m.reply_text(
                "<b>📖 品牌故事</b>\n\n"
                f"{he(brand_name)} 扎根金边，坚持实拍真房源；中文顾问陪你看房。\n"
                "从带看到签约，流程与条款可逐项确认。",
                parse_mode=ParseMode.HTML,
            )
            return True
        if arg == "about":
            await m.reply_text(
                "<b>🏢 介绍侨联</b>\n\n"
                f"<b>{he(brand_name)}</b> · QIAO LIAN PROPERTY\n"
                "金边租赁 · 实拍房源 · 中文服务。\n"
                "需求梳理 → 实地/视频带看 → 合同与押金说明。",
                parse_mode=ParseMode.HTML,
            )
            return True
        if arg == "want_home":
            await m.reply_text(
                "<b>🏠 预约想住</b>\n\n"
                "请发：预算（USD/月）、意向区域、户型、入住时间。\n"
                "顾问按实拍列表为您匹配。",
                parse_mode=ParseMode.HTML,
            )
            return True
        if arg == "ask":
            await m.reply_text(
                "<b>💬 咨询</b>\n\n请直接说明租房或区域问题，人工回复。",
                parse_mode=ParseMode.HTML,
            )
            return True
        return False

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._public_channel_start(update, context):
            return
        if not await self._ensure_admin(update):
            return
        self._reset_draft(context)
        await update.effective_message.reply_text(
            "🏠 <b>侨联发布助手</b>\n\n"
            "采集或导入房源后，系统会自动清洗并生成封面、文案。\n\n"
            "采集到合格房源后，我会主动提醒你。\n"
            "点“房源队列”即可查看、修改并发布。\n\n"
            "资料不完整的房源会自动留在后台，不会打断正常发布。",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu(),
        )

    async def admin_menu_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None:
            return
        # Telegram callback query 只能可靠地确认一次。旧实现在执行
        # 命令前后各 answer() 一次，第二次会导致 BadRequest，
        # 在客户端表现为按钮一直转圈或没有反馈。
        await query.answer("正在处理…", show_alert=False)
        if not await self._ensure_admin(update):
            return
        action = (query.data or "").split(":", 1)[1] if query.data else ""
        logger.info(
            "管理面板按钮：user=%s action=%s",
            getattr(update.effective_user, "id", None),
            action,
        )
        try:
            import autopilot_publish_bot as ap
        except Exception as e:
            await query.edit_message_text(f"❌ 面板调用失败：{e}", reply_markup=admin_menu(), parse_mode=ParseMode.HTML)
            return

        cmd_map = {
            "ops": ap.cmd_ops,
            "pending": ap.cmd_pending,
            "queue": ap.cmd_queue,
            "status": ap.cmd_status,
            "stats": ap.cmd_stats,
            "slots": ap.cmd_slots,
            "pause": ap.cmd_pause,
            "resume": ap.cmd_resume,
            "sources": ap.cmd_sources,
            "logs": ap.cmd_logs,
            "quality": ap.cmd_check,
            "dashboard": ap.cmd_analytics,
            "intake": ap.cmd_intake,
            "batch_generate": ap.cmd_batch_generate,
            "intake_done": ap.cmd_intake_done,
            "intake_pending": ap.cmd_intake_pending,
            "post_menu": ap.cmd_post_menu,
            "daily": ap.cmd_daily,
            "tpl": ap.cmd_tpl,
            "help": ap.cmd_help,
            "quick_help": self.cmd_quick_help,
            "settings_hub": self.cmd_settings_hub,
            "send_help": self.cmd_send_help,
            "send_queue": ap.cmd_send,
            "cover_test": self.cmd_cover_test,
            "listing_states": self.cmd_listing_states,
        }
        func = cmd_map.get(action)
        if func is None:
            await query.message.reply_text("⚠️ 这个按钮已失效，请发送 /start 刷新后再试。")
            return

        fake_update = SimpleNamespace(
            effective_user=update.effective_user,
            effective_chat=update.effective_chat,
            effective_message=query.message,
            message=query.message,
            callback_query=query,
        )
        try:
            await func(fake_update, context)
        except Exception:
            logger.exception("管理面板按钮执行失败：action=%s", action)
            await query.message.reply_text(
                "❌ 这个操作刚才没有完成，已经记录故障。\n"
                "请发送 /start 刷新面板后再试一次。",
                reply_markup=admin_menu(),
            )

    async def cmd_settings_hub(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        await update.effective_message.reply_text(
            "⚙️ <b>运营设置</b>\n\n"
            "日常发布不需要改这里。只有调整广播、检查运行状态或测试封面时才使用。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📢 每日广播", callback_data="cmd:daily"),
                    InlineKeyboardButton("📊 运行状态", callback_data="cmd:status"),
                ],
                [InlineKeyboardButton("🎨 封面测试", callback_data="cmd:cover_test")],
                [InlineKeyboardButton("⬅️ 返回首页", callback_data="cmd:quick_help")],
            ]),
        )

    async def cmd_send_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        await update.effective_message.reply_text(
            "📤 <b>审核与发布</b>\n\n"
            "1) 点 <code>/pending</code> 查看房源\n"
            "2) A/B/C 三版任选一版预览\n"
            "3) 点“采用并审核”，系统冻结正文、封面与图片\n"
            "4) 点 <code>/send</code> 打开已审核队列，一键发布\n\n"
            "也可使用 <code>/approve QC0032 B</code> 和 <code>/send QC0032</code>。",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu(),
        )

    async def private_message_fallback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """管理员私聊中的贴纸/GIF/文件/视频等也必须有反馈。"""
        if not await self._ensure_admin(update):
            return
        await update.effective_message.reply_text(
            "✅ 侨联发布助手在线。\n\n"
            "录入房源：点「➕ 录入房源」，再发文字和至少 4 张图片。\n"
            "查看和发布：点「🏠 房源队列」。\n\n"
            "贴纸、GIF 和其他文件不会被当作房源素材。",
            reply_markup=admin_menu(),
        )

    def _draft_post_dict_from_db(self, wanted_draft_id: str) -> dict[str, Any] | None:
        wanted = str(wanted_draft_id or "").strip()
        with sqlite3.connect(self.settings.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            if wanted:
                row = cur.execute(
                    """
                    SELECT id, draft_id, listing_id, title, project, community, area, property_type,
                           price, layout, size, floor, deposit, available_date,
                           highlights, drawbacks, advisor_comment, cost_notes, google_maps_url
                    FROM drafts
                    WHERE draft_id=?
                    LIMIT 1
                    """,
                    (wanted,),
                ).fetchone()
            else:
                row = cur.execute(
                    """
                    SELECT id, draft_id, listing_id, title, project, community, area, property_type,
                           price, layout, size, floor, deposit, available_date,
                           highlights, drawbacks, advisor_comment, cost_notes, google_maps_url
                    FROM drafts
                    WHERE review_status IN ('pending','ready','published')
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 1
                    """
                ).fetchone()
            if not row:
                return None
            listing_key = str(row["listing_id"] or "").strip() or f"l_{int(time.time())}"
            return {
                "draft_id": str(row["draft_id"] or "").strip(),
                "listing_id": listing_key,
                "type": TYPE_LABELS.get(str(row["property_type"] or "").strip(), str(row["property_type"] or "公寓")),
                "area": str(row["area"] or "").strip(),
                "project": str(row["project"] or row["community"] or row["title"] or "").strip(),
                "title": str(row["title"] or "").strip(),
                "price": str(row["price"] or "").strip(),
                "layout": str(row["layout"] or "").strip(),
                "size": str(row["size"] or "").strip(),
                "floor": str(row["floor"] or "").strip(),
                "deposit": str(row["deposit"] or "押一付一").strip(),
                "available_date": str(row["available_date"] or "随时入住").strip(),
                "highlights": row["highlights"],
                "drawbacks": row["drawbacks"],
                "advisor_comment": str(row["advisor_comment"] or "").strip(),
                "cost_notes": str(row["cost_notes"] or "").strip(),
                "google_maps_url": str(row["google_maps_url"] or "").strip(),
            }

    async def cmd_send_variants(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Legacy direct-to-channel variant publishing is intentionally disabled."""
        if not await self._ensure_admin(update):
            return
        await update.effective_message.reply_text(
            "⚠️ 多文案现场直发已停用。\n\n"
            "请使用 /pending 预览 A/B/C 三版，审核冻结后再用 /send 发布。",
            parse_mode=ParseMode.HTML,
        )

    async def cmd_quick_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        await update.effective_message.reply_text(
            "❓ <b>怎么使用</b>\n\n"
            "1. 采集到合格房源后，机器人会主动提醒\n"
            "2. 点“房源队列”查看封面和文案\n"
            "3. 需要时点“修改文案”或重新选主图\n"
            "4. 确认后直接发布\n\n"
            "临时录入：点“录入房源”，发送文字和至少 4 张图片。",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu(),
        )

    async def cmd_cover_test(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """测试 2 款精选封面模板。支持 /cover_test DRF_xxx 使用真实草稿数据。"""
        if not await self._ensure_admin(update):
            return
        msg = update.effective_message
        preview_chat_id = update.effective_chat.id

        wanted_draft_id = str(context.args[0]).strip() if context.args else ""
        test_data, bg_image_path, source_desc = self._cover_test_payload_from_db(wanted_draft_id)

        if wanted_draft_id and source_desc == "示例数据":
            await msg.reply_text(
                f"⚠️ 未找到草稿 <code>{wanted_draft_id}</code>，已回退为示例数据。",
                parse_mode=ParseMode.HTML,
            )

        # 旧的 classic / vertical 视觉稳定性一般，已下线测试入口，仅保留可上线模板。
        styles = [
            ("minimal", "精选A·清爽信息条"),
            ("price_tag", "精选B·价格角标"),
        ]

        await msg.reply_text(f"正在生成 2 款精选封面并发送到当前管理员私聊...\n数据来源：{source_desc}")
        try:
            out_dir = self._runtime_render_dir()
        except Exception as e:
            await msg.reply_text(f"❌ 渲染目录不可写：{e}")
            return
        failed: list[str] = []

        name_seed = str(test_data.get("project") or test_data.get("area") or "cover")
        safe_seed = "".join(ch for ch in name_seed if ch.isalnum() or ch in ("_", "-", " "))[:32].strip()
        safe_seed = safe_seed.replace(" ", "_") or "cover"

        for style, name in styles:
            output_path = str(out_dir / f"test_out_{safe_seed}_{style}.jpg")
            try:
                generate_house_cover(
                    bg_image_path,
                    output_path,
                    style=style,
                    **test_data,
                )
                with open(output_path, "rb") as f:
                    await context.bot.send_photo(
                        chat_id=preview_chat_id,
                        photo=f,
                        caption=f"🎨 封面模板测试：{name}\n风格代码：<code>{style}</code>\n来源：{source_desc}",
                        parse_mode=ParseMode.HTML,
                    )
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.error("Cover test failed for %s: %s", style, e)
                await msg.reply_text(f"❌ {name} 生成失败：{e}")
                failed.append(name)

        if failed:
            await msg.reply_text(
                f"⚠️ 已完成封面测试，但有失败：{len(failed)}/{len(styles)}\n"
                f"失败项：{', '.join(failed)}"
            )
        else:
            await msg.reply_text("✅ 2 款精选封面已发送到频道，请查看对比效果。")

    @staticmethod
    def _coerce_highlights(raw: Any) -> list[str]:
        if isinstance(raw, list):
            vals = [str(x).strip() for x in raw if str(x).strip()]
        else:
            text = str(raw or "").strip()
            vals: list[str] = []
            if text:
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        vals = [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    pass
            if not vals and text:
                vals = [x.strip() for x in text.replace("；", "，").replace(";", "，").split("，") if x.strip()]
        if not vals:
            vals = ["实拍真房源", "中文顾问", "可预约看房"]
        return vals[:3]

    def _cover_test_payload_from_db(self, wanted_draft_id: str) -> tuple[dict[str, Any], str, str]:
        fallback = {
            "project": "富力城",
            "property_type": "1房1卫",
            "area": "BKK1",
            "size": "45㎡",
            "floor": "8楼",
            "price": "$680/月",
            "highlights": ["家具基本全新", "小区泳池", "健身房"],
        }
        wanted = str(wanted_draft_id or "").strip()
        try:
            with sqlite3.connect(self.settings.sqlite_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                if wanted:
                    row = cur.execute(
                        """
                        SELECT id, draft_id, title, project, area, property_type, layout, price, size, floor, highlights
                        FROM drafts
                        WHERE draft_id=?
                        LIMIT 1
                        """,
                        (wanted,),
                    ).fetchone()
                else:
                    row = cur.execute(
                        """
                        SELECT id, draft_id, title, project, area, property_type, layout, price, size, floor, highlights
                        FROM drafts
                        WHERE review_status IN ('pending','ready','published')
                        ORDER BY updated_at DESC, id DESC
                        LIMIT 1
                        """
                    ).fetchone()
                if not row:
                    return fallback, "", "示例数据"

                media = cur.execute(
                    """
                    SELECT local_path
                    FROM media_assets
                    WHERE owner_type='draft' AND owner_ref_id=? AND status='active'
                    ORDER BY is_cover DESC, sort_order ASC, id ASC
                    LIMIT 1
                    """,
                    (int(row["id"]),),
                ).fetchone() if "id" in row.keys() else None

                project = (str(row["project"] or "").strip() or str(row["title"] or "").strip() or "侨联地产")
                room = (str(row["layout"] or "").strip() or str(row["property_type"] or "").strip() or "精选房源")
                area = (str(row["area"] or "").strip() or "金边")
                size = (str(row["size"] or "").strip() or "—")
                floor = (str(row["floor"] or "").strip() or "—")
                price = (str(row["price"] or "").strip() or "面议")
                highlights = self._coerce_highlights(row["highlights"])
                bg_path = str((media["local_path"] if media else "") or "").strip()

                return (
                    {
                        "project": project,
                        "property_type": room,
                        "area": area,
                        "size": size,
                        "floor": floor,
                        "price": price,
                        "highlights": highlights,
                    },
                    bg_path,
                    f"草稿 {row['draft_id']}",
                )
        except Exception:
            logger.exception("cover_test: 读取草稿数据失败")
            return fallback, "", "示例数据"

    async def cmd_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        await update.effective_message.reply_text(
            "🏠 侨联发布助手\n\n"
            "只走三步：\n"
            "1. 发文字和图片\n"
            "2. 检查并确认\n"
            "3. 发布到频道",
            reply_markup=admin_menu(),
        )

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        try:
            from autopilot_publish_bot import clear_autopilot_input_state

            clear_autopilot_input_state(context)
        except Exception:
            pass
        self._reset_draft(context)
        msg = update.effective_message
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("已取消当前发布流程。", reply_markup=main_menu())
        else:
            await msg.reply_text("已取消当前发布流程。", reply_markup=main_menu())
        return ConversationHandler.END

    async def skip_media_cmd(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await self._ensure_admin(update):
            return ConversationHandler.END
        context.user_data.pop("_album_msgs", None)
        context.user_data.pop("_mg_gen", None)
        draft = self._draft(context)
        draft.media_type = ""
        draft.media_file_id = ""
        draft.media_file_ids = []
        await update.message.reply_text("已跳过媒体。\n\n请选择房源类型：", reply_markup=type_keyboard())
        return ST_TYPE

    async def new_listing(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """兼容旧菜单入口：不再启动现场直发向导，统一转入待审 intake。"""
        if not await self._ensure_admin(update):
            return ConversationHandler.END
        self._reset_draft(context)
        text = (
            "📥 <b>新建房源已统一进入待审流程</b>\n\n"
            "请使用 <code>/intake</code> 导入房源文字和图片。\n"
            "完成后系统会生成 draft 与 publication package，审核 approved 后再使用 <code>/send</code>。\n\n"
            "当前不会从此入口现场生成封面或直接发布。"
        )
        target = update.callback_query.message if update.callback_query else update.effective_message
        if update.callback_query:
            await update.callback_query.answer()
        await target.reply_text(text, parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    def _merge_caption_into_draft(self, draft: Draft, caption: str) -> None:
        """Compatibility helper for album intake; extraction is advisory only."""
        cap = str(caption or "").strip()
        if not cap:
            return
        if cap not in draft.source_caption:
            draft.source_caption = (draft.source_caption + "\n" + cap).strip()
        info = extract_house_info(cap)
        if info.get("project"):
            draft.title = info["project"]
        if info.get("price"):
            draft.price = info["price"]
        if info.get("size"):
            draft.size_sqm = info["size"]
        if info.get("floor"):
            draft.fee_note = info["floor"]
        if info.get("layout"):
            draft.layout = info["layout"]
        if info.get("highlights"):
            draft.highlights = list(info["highlights"])

    def _store_album_messages(self, draft: Draft, messages: list[Message]) -> Message:
        """Preserve one Telegram album in message order and choose its largest cover."""
        ordered = sorted(
            messages,
            key=lambda message: int(getattr(message, "message_id", 0) or 0),
        )
        if not ordered:
            raise ValueError("album is empty")
        for message in ordered:
            self._merge_caption_into_draft(
                draft, getattr(message, "caption", "") or getattr(message, "text", "") or ""
            )
        draft.media_type = "photo"
        draft.media_file_ids = [message.photo[-1].file_id for message in ordered]
        best = max(
            ordered,
            key=lambda message: message.photo[-1].width * message.photo[-1].height,
        )
        draft.media_file_id = best.photo[-1].file_id
        return best

    @staticmethod
    def _legacy_direct_new_enabled() -> bool:
        """The legacy direct publisher is permanently disabled in production."""
        return False

    def _persist_new_as_pending(
        self, draft: Draft, *, local_paths: list[str], operator_user_id: int
    ) -> str:
        """Persist admin intake, then canonicalize it through the shared parser."""
        from ai_parser import AIParserModule
        from source_sanitizer import sanitize_source_text

        if len(draft.media_file_ids) != len(local_paths):
            raise ValueError("admin album file-id/path counts do not match")
        core = CoreDatabaseManager(self.settings.sqlite_path)
        source_post_id = f"admin_new_{uuid.uuid4().hex}"
        structured_lines = [
            draft.source_caption.strip(),
            f"项目：{draft.community or draft.title}" if (draft.community or draft.title) else "",
            f"区域：{draft.area}" if draft.area else "",
            f"物业类型：{TYPE_LABELS.get(draft.property_type, draft.property_type)}" if draft.property_type else "",
            f"户型：{draft.layout}" if draft.layout else "",
            f"面积：{draft.size_sqm}" if draft.size_sqm else "",
            f"楼层：{draft.fee_note}" if draft.fee_note else "",
            f"租金：${draft.price}/月" if draft.price else "",
            f"押付：{draft.deposit_rule}" if draft.deposit_rule else "",
        ]
        raw_text = "\n".join(line for line in structured_lines if line)
        sanitized = sanitize_source_text(raw_text)
        raw_images = [
            {"local_path": path, "telegram_file_id": file_id, "sort_order": index}
            for index, (file_id, path) in enumerate(zip(draft.media_file_ids, local_paths))
        ]
        source_pk = core.save_source_post(
            None, "telegram_admin_upload", "publisher_bot_new", source_post_id, "",
            f"admin:{operator_user_id}", raw_text, raw_images, [], "",
            {"ingest_kind": "admin_new", "sanitized_text": sanitized.text},
            hashlib.sha256((source_post_id + raw_text).encode()).hexdigest(), "pending",
        )
        for index, item in enumerate(raw_images):
            core.save_media_asset(
                f"AST_{uuid.uuid4().hex[:16].upper()}", "source_post", source_pk, str(source_pk),
                "photo", "telegram_admin_upload", "", item["telegram_file_id"], item["local_path"],
                "", "", item["telegram_file_id"], "", "photo", 0, 0, index,
            )
        draft_id = AIParserModule(self.settings.sqlite_path).process_single_source_post(source_pk)
        if not draft_id:
            raise ValueError("admin intake could not produce a canonical draft")
        return str(draft_id)

    async def _download_new_album(
        self, context: ContextTypes.DEFAULT_TYPE, draft: Draft
    ) -> list[str]:
        out_dir = _REPO_ROOT / "data" / "management_intake"
        out_dir.mkdir(parents=True, exist_ok=True)
        paths: list[str] = []
        for index, file_id in enumerate(draft.media_file_ids):
            tg_file = await context.bot.get_file(file_id)
            path = out_dir / f"{draft.listing_id}_{index + 1}_{uuid.uuid4().hex[:8]}.jpg"
            await tg_file.download_to_drive(custom_path=str(path))
            paths.append(str(path))
        return paths

    async def _generate_and_reply_cover(
        self, reply_to: Message, draft: Draft, style: str | None = None
    ) -> str | None:
        """Generate a private admin preview; this method never publishes."""
        try:
            out_dir = self._runtime_render_dir()
            output_path = str(out_dir / f"out_{draft.listing_id}_{style or draft.cover_style}.jpg")
            bg_path = await self._resolve_cover_background(reply_to, draft, out_dir)
            generate_house_cover(
                bg_path,
                output_path,
                project=(draft.title or "侨联地产").strip(),
                property_type=(draft.layout or "精选房源").strip(),
                area=(draft.area or "金边").strip(),
                size=(draft.size_sqm or "—").strip(),
                floor=(draft.fee_note or "—").strip(),
                price=(draft.price or "面议").strip(),
                highlights=draft.highlights or ["实拍真房源", "中文顾问", "可预约看房"],
                style=(style or draft.cover_style or "minimal").lower().strip(),
            )
            with open(output_path, "rb") as preview:
                await reply_to.reply_photo(preview, caption="✨ 品牌封面预览（仅管理员可见）")
            return output_path
        except Exception as exc:
            logger.error("Cover generation failed: %s", exc)
            return None

    async def capture_media(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        msg = update.message
        assert msg is not None
        user = update.effective_user
        chat = update.effective_chat

        # 只允许管理员在私聊里走发布流程；群聊/讨论组和机器人消息直接忽略
        if (not user) or user.is_bot or (chat and chat.type != "private") or (user.id not in self.settings.admin_ids):
            return ST_MEDIA

        draft = self._draft(context)

        if msg.photo:
            draft.media_type = "photo"
            mgid = msg.media_group_id

            if mgid is not None:
                # 相册：多条 Update 并发进入，只让「最后一条」统一处理，避免刷屏
                context.user_data.setdefault("_album_msgs", []).append(msg)
                context.user_data["_mg_gen"] = context.user_data.get("_mg_gen", 0) + 1
                my_gen = context.user_data["_mg_gen"]
                await asyncio.sleep(1.0)
                if context.user_data.get("_mg_gen") != my_gen:
                    return ST_MEDIA
                msgs = context.user_data.pop("_album_msgs", [])
                if not msgs:
                    return ST_MEDIA
                # 保留整组相册的 file_id；media_file_id 仅作为封面候选的兼容字段。
                best = self._store_album_messages(draft, msgs)
                n = len(draft.media_file_ids)
                await best.reply_text(
                    f"✅ 媒体已记录（相册共 <b>{n}</b> 张）\n\n请选择房源类型：",
                    reply_markup=type_keyboard(),
                    parse_mode=ParseMode.HTML,
                )
                return ST_TYPE

            # 单图
            context.user_data.pop("_album_msgs", None)
            context.user_data.pop("_mg_gen", None)
            self._merge_caption_into_draft(draft, msg.caption or msg.text or "")
            draft.media_file_id = msg.photo[-1].file_id
            draft.media_file_ids = [draft.media_file_id]
            await msg.reply_text(
                "✅ 媒体已记录\n\n请选择房源类型：",
                reply_markup=type_keyboard()
            )
            return ST_TYPE

        if msg.video:
            context.user_data.pop("_album_msgs", None)
            context.user_data.pop("_mg_gen", None)
            self._merge_caption_into_draft(draft, msg.caption or msg.text or "")
            draft.media_type = "video"
            draft.media_file_id = msg.video.file_id
            await msg.reply_text("媒体已记录。\n\n请选择房源类型：", reply_markup=type_keyboard())
            return ST_TYPE

        await msg.reply_text("请发送图片或视频，或者输入 /skipmedia 跳过。")
        return ST_MEDIA

    async def pick_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()
        draft = self._draft(context)
        draft.property_type = query.data.split(":", 1)[1]
        await query.edit_message_text(
            f"类型已选：{TYPE_LABELS.get(draft.property_type, draft.property_type)}\n\n请选择区域：",
            reply_markup=area_keyboard(),
        )
        return ST_AREA

    async def pick_area(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()
        draft = self._draft(context)
        draft.area = query.data.split(":", 1)[1]
        # 已从图文里识别到核心字段时，直接给预览，减少人工补录步骤。
        if str(draft.title or "").strip() and str(draft.price or "").strip():
            draft.layout = (draft.layout or "精选房源").strip() or "精选房源"
            draft.size_sqm = (draft.size_sqm or "—").strip() or "—"
            draft.fee_note = (draft.fee_note or "—").strip() or "—"
            await query.edit_message_text(
                f"区域已选：{draft.area}\n\n已识别到标题和价格，已自动进入预览。"
            )
            await self.show_preview(update, context)
            return ST_PREVIEW
        await query.edit_message_text(f"区域已选：{draft.area}\n\n{messages.ASK_TITLE} (当前识别: {draft.title or '空'})")
        return ST_TITLE

    async def save_title(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        draft = self._draft(context)
        val = update.message.text.strip()
        if val != ".": draft.title = val
        await update.message.reply_text(f"项目名已记录。\n\n{messages.ASK_PRICE} (当前识别: {draft.price or '空'})")
        return ST_PRICE

    async def save_price(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        draft = self._draft(context)
        val = update.message.text.strip()
        if val != ".": draft.price = val
        await update.message.reply_text("价格已记录。\n\n请输入楼层 (如: 8楼):")
        return ST_COMMUNITY

    async def save_community(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        draft = self._draft(context)
        draft.fee_note = update.message.text.strip()
        await update.message.reply_text(f"楼层已记录。\n\n请输入户型 (如: 1房1卫):", reply_markup=skip_keyboard())
        return ST_LAYOUT

    async def on_layout_enter(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        draft = self._draft(context)
        draft.layout = update.message.text.strip()
        await update.message.reply_text(f"户型已记录。\n\n请输入面积 (如: 45㎡):", reply_markup=skip_keyboard())
        return ST_SIZE

    async def on_size_enter(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        draft = self._draft(context)
        draft.size_sqm = update.message.text.strip()
        await self.show_preview(update, context)
        return ST_PREVIEW

    async def show_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        draft = self._draft(context)
        preview_data = draft.to_dict(update.effective_user.id)
        preview_data.update({
            "project": draft.community or draft.title,
            "size": draft.size_sqm,
            "deposit": draft.deposit_rule,
            "available_date": draft.available_date,
        })
        text = "📋 <b>发布预览</b>\n\n" + build_chinese_listing_post(preview_data)
        await update.effective_message.reply_text(
            text,
            reply_markup=preview_keyboard(),
            parse_mode=ParseMode.HTML,
        )

    async def preview_actions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()
        action = query.data.split(":", 1)[1]

        if action in {"publish", "publish_variants"}:
            # 生产安全：/new 只能进入统一待审流水线，禁止现场重建正文/封面并直发。
            draft = self._draft(context)
            uid = int(update.effective_user.id)
            if action == "publish_variants":
                await query.edit_message_text(
                    "⚠️ 多版本现场直发已停用。请先保存为待审草稿，审核 approved package 后使用 /send。"
                )
                self._reset_draft(context)
                return ConversationHandler.END
            try:
                local_paths = await self._download_new_album(context, draft)
                pending_id = self._persist_new_as_pending(
                    draft, local_paths=local_paths, operator_user_id=uid
                )
                await query.edit_message_text(
                    f"✅ 已保存到统一待审队列\n草稿：<code>{pending_id}</code>\n\n"
                    "未发布到频道。请在生成并审核 approved package 后使用 /send。",
                    parse_mode=ParseMode.HTML,
                )
                self._reset_draft(context)
                return ConversationHandler.END
            except Exception as exc:
                logger.exception("save /new into unified pipeline failed")
                await query.edit_message_text(f"❌ 保存待审草稿失败：{exc}")
                return ST_PREVIEW

    async def style_actions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """处理封面模板风格选择。"""
        query = update.callback_query
        await query.answer()
        data = query.data

        if data == "style:back":
            await self.show_preview(update, context)
            return ST_PREVIEW

        if data.startswith("style:set:"):
            style = data.split(":", 2)[2]
            draft = self._draft(context)
            draft.cover_style = style

            # 重新生成并发送预览（不依赖原图）
            try:
                await query.edit_message_text(f"正在生成 {style} 风格封面...")
                await self._generate_and_reply_cover(
                    query.message, draft, style=style
                )
            except Exception as e:
                logger.error("Style preview failed: %s", e)
                await query.edit_message_text(f"⚠️ 预览生成失败：{e}")

            await asyncio.sleep(0.5)
            await self.show_preview(update, context)
            return ST_PREVIEW

        return ST_PREVIEW

    @staticmethod
    def _admin_listing_id(raw: str) -> str:
        value = str(raw or "").strip()
        if value.upper().startswith("QC") and value[2:].isdigit():
            return f"l_{int(value[2:])}"
        return value

    def _is_admin(self, update: Update) -> bool:
        user = getattr(update, "effective_user", None)
        try:
            return bool(user and int(user.id) in {int(x) for x in self.settings.admin_ids})
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _display_listing_id(listing_id: str) -> str:
        raw = str(listing_id or "").strip()
        if raw.lower().startswith("l_") and raw[2:].isdigit():
            return f"QC{int(raw[2:]):04d}"
        return raw

    async def cmd_listing_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update):
            await update.effective_message.reply_text("无权限。")
            return
        raw = (context.args or [""])[0]
        if not raw:
            await update.effective_message.reply_text("用法：/listing_status QC0032")
            return
        listing_id = self._admin_listing_id(raw)
        with self.db._connect() as conn:
            row = conn.execute("SELECT listing_id, community, area, layout, price, status, updated_at FROM listings WHERE listing_id=?", (listing_id,)).fetchone()
        if not row:
            await update.effective_message.reply_text("未找到这套房源。请检查 QC 编号。")
            return
        await update.effective_message.reply_text(
            f"房源状态\n\n编号｜{self._display_listing_id(row['listing_id'])}\n项目｜{row['community'] or '-'}\n区域｜{row['area'] or '-'}\n户型｜{row['layout'] or '-'}\n租金｜${row['price'] or '-'} / 月\n状态｜{row['status'] or '-'}\n更新时间｜{row['updated_at'] or '-'}"
        )

    @staticmethod
    def _listing_status_label(status: str) -> str:
        return {
            "active": "🟢 可预约",
            "reserved": "🟡 已有预约",
            "pending": "🔵 待确认",
            "rented": "🔴 已租出",
            "inactive": "⚫ 已下架",
        }.get(str(status or "").strip().lower(), "⚪ 未知")

    async def cmd_listing_states(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update):
            await update.effective_message.reply_text("无权限。")
            return
        with self.db._connect() as conn:
            rows = conn.execute(
                "SELECT listing_id, community, area, layout, status FROM listings "
                "ORDER BY updated_at DESC, created_at DESC LIMIT 12"
            ).fetchall()
        buttons = []
        for row in rows:
            place = str(row["community"] or row["area"] or "房源").strip()
            layout = str(row["layout"] or "").strip()
            summary = "｜".join(x for x in (place, layout) if x)
            label = f'{self._listing_status_label(row["status"])} {self._display_listing_id(row["listing_id"])}｜{summary}'
            buttons.append([InlineKeyboardButton(label[:60], callback_data=f'listingpick:{row["listing_id"]}')])
        buttons.append([InlineKeyboardButton("⬅️ 返回首页", callback_data="cmd:quick_help")])
        await update.effective_message.reply_text(
            "🏠 <b>房态管理</b>\n\n选择一套房源，再更新当前房态。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    async def listing_state_pick(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None:
            return
        await query.answer()
        if not self._is_admin(update):
            return
        listing_id = str(query.data or "").split(":", 1)[-1].strip()
        with self.db._connect() as conn:
            row = conn.execute(
                "SELECT listing_id, community, area, layout, price, status, updated_at FROM listings WHERE listing_id=?",
                (listing_id,),
            ).fetchone()
        if not row:
            await query.message.reply_text("未找到这套房源。")
            return
        title = "｜".join(x for x in (str(row["community"] or row["area"] or "房源"), str(row["layout"] or "")) if x)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 可预约", callback_data=f"listingstate:{listing_id}:active"), InlineKeyboardButton("🟡 已有预约", callback_data=f"listingstate:{listing_id}:reserved")],
            [InlineKeyboardButton("🔵 待确认", callback_data=f"listingstate:{listing_id}:pending"), InlineKeyboardButton("🔴 已租出", callback_data=f"listingstate:{listing_id}:rented")],
            [InlineKeyboardButton("⚫ 下架", callback_data=f"listingstate:{listing_id}:inactive")],
            [InlineKeyboardButton("⬅️ 返回房态列表", callback_data="cmd:listing_states")],
        ])
        await query.message.reply_text(
            f"🏠 <b>{self._display_listing_id(listing_id)}｜{title}</b>\n"
            f"💰 ${row['price'] or '-'} /月\n"
            f"当前房态｜{self._listing_status_label(row['status'])}\n\n"
            "请选择新的房态：",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )

    async def listing_state_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None:
            return
        await query.answer("正在更新…")
        if not self._is_admin(update):
            return
        parts = str(query.data or "").split(":")
        if len(parts) != 3:
            return
        _, listing_id, status = parts
        allowed = {"active", "pending", "reserved", "rented", "inactive"}
        if status not in allowed:
            return
        with self.db._connect() as conn:
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(listings)").fetchall()}
            if "availability_confirmed_at" not in columns:
                conn.execute("ALTER TABLE listings ADD COLUMN availability_confirmed_at TEXT")
            old = conn.execute("SELECT status FROM listings WHERE listing_id=?", (listing_id,)).fetchone()
            if not old:
                await query.message.reply_text("未找到这套房源。")
                return
            if status in {"active", "reserved"}:
                conn.execute(
                    "UPDATE listings SET status=?, availability_confirmed_at=datetime('now','localtime'), updated_at=datetime('now','localtime') WHERE listing_id=?",
                    (status, listing_id),
                )
            else:
                conn.execute(
                    "UPDATE listings SET status=?, availability_confirmed_at=NULL, updated_at=datetime('now','localtime') WHERE listing_id=?",
                    (status, listing_id),
                )
        await query.message.reply_text(
            f"✅ 房态已更新\n\n{self._display_listing_id(listing_id)}｜{self._listing_status_label(status)}\n\n"
            + ("当前可以继续预约看房。" if status in {"active", "reserved"} else "用户端会立即停止不适用的预约入口。"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回房态列表", callback_data="cmd:listing_states")], [InlineKeyboardButton("🏠 返回首页", callback_data="cmd:quick_help")]]),
        )

    async def cmd_listing_area_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update):
            await update.effective_message.reply_text("无权限。")
            return
        args=list(context.args or [])
        if len(args)<3:
            await update.effective_message.reply_text("用法：/listing_area_set QC0032 炳发城 原文明确写有具体位置")
            return
        listing_id=self._admin_listing_id(args[0]); area=str(args[1]).strip(); reason=' '.join(args[2:]).strip()
        try:
            from qiaolian_dual.area_admin import set_canonical_area
            from publication_package import build_package
            result=set_canonical_area(self.db.db_path, listing_id, area, str(update.effective_user.id), reason)
            package=build_package(self.db.db_path, result['draft_id'])
            await update.effective_message.reply_text(f"已记录人工区域确认\n\n房源｜{self._display_listing_id(listing_id)}\n区域｜{result['old_area'] or '-'} → {result['new_area']}\n审核记录｜{result['audit_id']}\n新包｜{package.get('package_id')}\n状态｜pending，需重新审核后发布")
        except Exception as exc:
            await update.effective_message.reply_text(f"区域补录未执行：{exc}")

    async def cmd_listing_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update):
            await update.effective_message.reply_text("无权限。")
            return
        args = list(context.args or [])
        if len(args) < 2:
            await update.effective_message.reply_text("用法：/listing_set QC0032 active|pending|reserved|rented|inactive")
            return
        listing_id = self._admin_listing_id(args[0])
        status = str(args[1] or "").strip().lower()
        allowed = {"active", "pending", "reserved", "rented", "inactive"}
        if status not in allowed:
            await update.effective_message.reply_text("状态只能是 active、pending、reserved、rented 或 inactive。")
            return
        with self.db._connect() as conn:
            row = conn.execute("SELECT listing_id, status FROM listings WHERE listing_id=?", (listing_id,)).fetchone()
            if not row:
                await update.effective_message.reply_text("未找到这套房源。请检查 QC 编号。")
                return
            conn.execute("UPDATE listings SET status=?, updated_at=CURRENT_TIMESTAMP WHERE listing_id=?", (status, listing_id))
        await update.effective_message.reply_text(
            f"已更新房态\n\n{self._display_listing_id(listing_id)}｜{row['status'] or '-'} → {status}\n\n自动同步不会覆盖管理员手动设置。"
        )

    async def start_polling(self):
        pass # Placeholder for actual run_polling if needed

def main() -> None:
    settings = get_settings()
    bot = PublisherBot(settings)

    _root = Path(__file__).resolve().parents[2]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
    from autopilot_publish_bot import register_autopilot_features

    application = Application.builder().token(settings.publisher_bot_token).build()
    register_autopilot_features(application, include_cancel=False, simple_mode=True)

    async def post_init(app):
        # 只保留入口和取消；正常操作全部点按钮。
        commands = [
            BotCommand("start", "打开发布助手"),
            BotCommand("cancel", "取消当前流程"),
        ]
        # 默认命令
        await app.bot.set_my_commands(commands)
        # 强制覆盖管理员私聊命令作用域（清掉历史旧命令）
        for admin_id in settings.admin_ids:
            scope = BotCommandScopeChat(chat_id=admin_id)
            await app.bot.set_my_commands(commands, scope=scope)
            await app.bot.set_my_commands(commands, scope=scope, language_code="zh")

    application.post_init = post_init

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("new", bot.new_listing),
            CallbackQueryHandler(bot.new_listing, pattern="^pub:new$"),
        ],
        states={
            ST_MEDIA: [
                MessageHandler(filters.PHOTO | filters.VIDEO, bot.capture_media),
                CommandHandler("skipmedia", bot.skip_media_cmd),
                CallbackQueryHandler(bot.cancel, pattern="^pub:cancel$"),
            ],
            ST_TYPE: [
                CallbackQueryHandler(bot.pick_type, pattern=r"^type:"),
                CallbackQueryHandler(bot.cancel, pattern="^pub:cancel$"),
            ],
            ST_AREA: [
                CallbackQueryHandler(bot.pick_area, pattern=r"^area:"),
                CallbackQueryHandler(bot.cancel, pattern="^pub:cancel$"),
            ],
            ST_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.save_title)],
            ST_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.save_price)],
            ST_COMMUNITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.save_community)],
            ST_LAYOUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.on_layout_enter),
                CallbackQueryHandler(bot.cancel, pattern="^pub:cancel$"),
            ],
            ST_SIZE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.on_size_enter),
                CallbackQueryHandler(bot.cancel, pattern="^pub:cancel$"),
            ],
            ST_PREVIEW: [
                CallbackQueryHandler(bot.preview_actions, pattern=r"^preview:"),
                CallbackQueryHandler(bot.style_actions, pattern=r"^style:"),
                CallbackQueryHandler(bot.cancel, pattern="^pub:cancel$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", bot.cancel)],
        allow_reentry=True,
    )
    
    application.add_handler(CommandHandler("start", bot.start))
    application.add_handler(CommandHandler("menu", bot.cmd_menu))
    application.add_handler(CommandHandler("quick", bot.cmd_quick_help))
    application.add_handler(CommandHandler("send_variants", bot.cmd_send_variants))
    application.add_handler(CommandHandler("cover_test", bot.cmd_cover_test))
    application.add_handler(CommandHandler("listing_status", bot.cmd_listing_status))
    application.add_handler(CommandHandler("listing_set", bot.cmd_listing_set))
    application.add_handler(CommandHandler("listing_area_set", bot.cmd_listing_area_set))
    application.add_handler(CallbackQueryHandler(bot.listing_state_pick, pattern=r"^listingpick:"))
    application.add_handler(CallbackQueryHandler(bot.listing_state_set, pattern=r"^listingstate:"))
    application.add_handler(CallbackQueryHandler(bot.admin_menu_action, pattern=r"^cmd:"))
    application.add_handler(conv)
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE
            & filters.ALL
            & ~filters.TEXT
            & ~filters.PHOTO
            & ~filters.COMMAND,
            bot.private_message_fallback,
        ),
        group=1,
    )
    application.add_handler(
        MessageHandler(filters.ALL & ~filters.COMMAND, bot.capture_discussion_forward),
        group=2,
    )

    async def _app_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        err = context.error
        if isinstance(err, NetworkError):
            logger.warning("Telegram 网络瞬断，已自动重试：%s", err)
            return
        logger.exception("Publisher Bot 未处理异常: %s", err)

    application.add_error_handler(_app_error_handler)

    logger.info("统一管理员 Bot 启动（v2 向导 + autopilot 队列/定时，单 Token）")
    application.run_polling()

if __name__ == "__main__":
    main()
