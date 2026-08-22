from __future__ import annotations
import asyncio
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
    build_post_variants,
    build_preview_text,
    normalize_tags,
)
from .keyboards import (
    area_keyboard,
    admin_menu,
    edit_keyboard,
    main_menu,
    preview_keyboard,
    publish_post_keyboard,
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
from meihua_publisher import add_detail_logo_watermark, resolve_discussion_id

for _cover_module_dir in (
    _REPO_ROOT / "v2_admin",
    Path("/opt/qiaolian_dual_bots/v2_admin"),
):
    if _cover_module_dir.exists():
        sys.path.append(str(_cover_module_dir))
from house_cover_v2 import generate_house_cover

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
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
        # 忽略机器人账号消息，避免在讨论组被机器人互相触发
        if user and user.is_bot:
            return False
        if self._is_admin(update):
            return True
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

        sender_chat = getattr(msg, "sender_chat", None)
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
        """频道帖按钮深链：非管理员由本 Bot 承接（USER_BOT_USERNAME 指向 @meihua666bot 时）。"""
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
        await update.effective_message.reply_text(messages.WELCOME, reply_markup=admin_menu())

    async def admin_menu_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()
        if not await self._ensure_admin(update):
            return
        action = (query.data or "").split(":", 1)[1] if query.data else ""
        try:
            import autopilot_publish_bot as ap
        except Exception as e:
            await query.edit_message_text(f"❌ 面板调用失败：{e}", reply_markup=admin_menu(), parse_mode=ParseMode.HTML)
            return

        cmd_map = {
            "ops": ap.cmd_ops,
            "pending": ap.cmd_pending,
            "status": ap.cmd_status,
            "stats": ap.cmd_stats,
            "slots": ap.cmd_slots,
            "pause": ap.cmd_pause,
            "resume": ap.cmd_resume,
            "sources": ap.cmd_sources,
            "logs": ap.cmd_logs,
            "intake": ap.cmd_intake,
            "intake_done": ap.cmd_intake_done,
            "intake_pending": ap.cmd_intake_pending,
            "post_menu": ap.cmd_post_menu,
            "daily": ap.cmd_daily,
            "tpl": ap.cmd_tpl,
            "help": ap.cmd_help,
            "quick_help": self.cmd_quick_help,
            "send_help": self.cmd_send_help,
            "cover_test": self.cmd_cover_test,
        }
        func = cmd_map.get(action)
        if func is None:
            await query.answer("未知按钮", show_alert=False)
            return

        fake_update = SimpleNamespace(
            effective_user=update.effective_user,
            effective_chat=update.effective_chat,
            effective_message=query.message,
            message=query.message,
            callback_query=query,
        )
        await func(fake_update, context)
        await query.answer("已执行", show_alert=False)

    async def cmd_send_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        await update.effective_message.reply_text(
            "🚀 <b>立即发布（最常用）</b>\n\n"
            "<code>/send DRF_xxx</code> 或 <code>/publish DRF_xxx</code>\n"
            "例如：<code>/send DRF_7a806f77-5451-49a4-90eb-be9c95ca95aa</code>\n\n"
            "批量文案对比：<code>/send_variants DRF_xxx</code>\n\n"
            "先用 <code>/pending</code> 看待审编号，再复制粘贴发布。\n\n"
            "🎨 封面测试：<code>/cover_test DRF_xxx</code>（按真实草稿渲染 2 款精选）",
            parse_mode=ParseMode.HTML,
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
        if not await self._ensure_admin(update):
            return
        ch = self.settings.channel_id
        if not ch:
            await update.effective_message.reply_text("❌ CHANNEL_ID 未配置")
            return
        wanted = str(context.args[0]).strip() if context.args else ""
        try:
            post_dict = self._draft_post_dict_from_db(wanted)
        except Exception as e:
            logger.exception("读取草稿失败")
            await update.effective_message.reply_text(f"❌ 读取草稿失败：{e}")
            return
        if not post_dict:
            await update.effective_message.reply_text(
                "❌ 未找到草稿。用法：/send_variants DRF_xxx（不带参数则取最近一条）"
            )
            return

        listing_key = str(post_dict.get("listing_id") or "").strip()
        _maps_url = self._build_maps_url(
            str(post_dict.get("project") or post_dict.get("title") or ""),
            str(post_dict.get("area") or ""),
            str(post_dict.get("google_maps_url") or ""),
        )
        kb = publish_post_keyboard(
            listing_key,
            str(post_dict.get("area") or "").strip(),
            self.settings.user_bot_username,
            channel_username=self.settings.channel_username,
            discussion_group_link=self.settings.discussion_group_link,
            maps_url=_maps_url,
        )
        variants = build_post_variants(post_dict)
        await update.effective_message.reply_text(
            f"🧪 开始挨个发布文案：{len(variants)} 条\n草稿：<code>{post_dict.get('draft_id') or '最近一条'}</code>\n编号：<code>{listing_key}</code>",
            parse_mode=ParseMode.HTML,
        )
        sent_count = 0
        for idx, (variant_name, variant_text) in enumerate(variants, start=1):
            await context.bot.send_message(
                chat_id=ch,
                text=f"【文案{idx}/{len(variants)}｜{variant_name}】\n\n{variant_text}",
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=kb,
            )
            sent_count += 1
            await asyncio.sleep(0.5)
        await update.effective_message.reply_text(
            f"✅ 已挨个发完：{sent_count} 条\n编号：<code>{listing_key}</code>",
            parse_mode=ParseMode.HTML,
        )

    async def cmd_quick_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        await update.effective_message.reply_text(
            "📘 <b>管理员命令（精简实用版）</b>\n\n"
            "<b>高频主链（每天都用）</b>\n"
            "1) <code>/pending</code> 查看待审草稿\n"
            "2) <code>/send DRF_xxx</code> 立即发指定草稿\n"
            "3) <code>/new</code> 手工补录并发布\n\n"
            "<b>运行控制</b>\n"
            "<code>/ops</code> 一屏总览（待审/队列/已发）\n"
            "<code>/status</code> 检查采集与发布状态\n"
            "<code>/slots</code> 查看/修改发帖时段\n"
            "<code>/pause</code> / <code>/resume</code> 队列开关\n\n"
            "<b>导入与排障</b>\n"
            "<code>/intake</code> 开始微信导入\n"
            "<code>/intake_done</code> 导入完成并入库\n"
            "<code>/intake_pending</code> 查看导入待发草稿\n"
            "<code>/logs</code> 最近发布日志\n"
            "<code>/cover_test DRF_xxx</code> 封面回归测试",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu(),
        )

    async def cmd_cover_test(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """测试 2 款精选封面模板。支持 /cover_test DRF_xxx 使用真实草稿数据。"""
        if not await self._ensure_admin(update):
            return
        msg = update.effective_message
        ch = self.settings.channel_id
        if not ch:
            await msg.reply_text("❌ CHANNEL_ID 未配置")
            return

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

        await msg.reply_text(f"正在生成 2 款精选封面并发送到频道...\n数据来源：{source_desc}")
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
                        chat_id=ch,
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
                ).fetchone()

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
            "🧭 管理员面板（高频优先）\n\n"
            "主链：待审 -> 立即发布 -> 运行监控\n"
            "推荐顺序：/pending → /send DRF_xxx → /ops",
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
        await update.message.reply_text("已跳过媒体。\n\n请选择房源类型：", reply_markup=type_keyboard())
        return ST_TYPE

    async def new_listing(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await self._ensure_admin(update):
            return ConversationHandler.END
        self._reset_draft(context)
        draft = self._draft(context)
        text = f"新建房源编号：{draft.listing_id}\n\n{messages.HELP_NEW}"
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(text)
        else:
            await update.effective_message.reply_text(text)
        return ST_MEDIA

    def _merge_caption_into_draft(self, draft: Draft, caption: str) -> None:
        cap = (caption or "").strip()
        if not cap:
            return
        info = extract_house_info(cap)
        if info["project"]:
            draft.title = info["project"]
        if info["price"]:
            draft.price = info["price"]
        if info["size"]:
            draft.size_sqm = info["size"]
        if info["floor"]:
            draft.fee_note = info["floor"]
        if info["layout"]:
            draft.layout = info["layout"]
        if info["highlights"]:
            draft.highlights = list(info["highlights"])

    async def _generate_and_reply_cover(
        self, reply_to: Message, draft: Draft, style: str | None = None
    ) -> str | None:
        """生成封面并发送预览，返回生成的文件路径。"""
        try:
            out_dir = self._runtime_render_dir()
            output_path = str(out_dir / f"out_{draft.listing_id}_{style or draft.cover_style}.jpg")
            bg_path = await self._resolve_cover_background(reply_to, draft, out_dir)
            hl = draft.highlights or ["实拍真房源", "中文顾问", "可预约看房"]
            s = (style or draft.cover_style or "classic").lower().strip()
            generate_house_cover(
                bg_path,
                output_path,
                project=(draft.title or "侨联地产").strip() or "侨联地产",
                property_type=(draft.layout or "精选房源").strip() or "精选房源",
                area=(draft.area or "金边").strip() or "金边",
                size=(draft.size_sqm or "—").strip() or "—",
                floor=(draft.fee_note or "—").strip() or "—",
                price=(draft.price or "面议").strip() or "面议",
                highlights=hl,
                style=s,
            )
            style_names = {
                "classic": "经典蓝卡",
                "minimal": "极简白条",
                "price_tag": "右侧价签",
                "vertical": "竖版视频",
            }
            style_name = style_names.get(s, s)
            with open(output_path, "rb") as f:
                await reply_to.reply_photo(
                    f,
                    caption=f"✨ 品牌封面预览（{style_name}）\n\n"
                    f"底图：{'实拍图' if bg_path else '纯模板'}\n"
                    f"项目：{draft.title or '未识别'}\n"
                    f"价格：{draft.price or '未识别'}",
                )
            return output_path
        except Exception as e:
            logger.error("Cover generation failed: %s", e)
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
                for m in msgs:
                    self._merge_caption_into_draft(draft, m.caption or m.text or "")
                best = max(msgs, key=lambda m: m.photo[-1].width * m.photo[-1].height)
                draft.media_file_id = best.photo[-1].file_id
                n = len(msgs)
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
        text = build_preview_text(draft.to_dict(update.effective_user.id))
        await update.effective_message.reply_text(
            text,
            reply_markup=preview_keyboard(),
            parse_mode=ParseMode.HTML,
        )

    async def preview_actions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()
        action = query.data.split(":", 1)[1]

        if action == "compare_covers":
            draft = self._draft(context)
            await query.edit_message_text("⏳ 正在生成两款封面对比，请稍候…")
            try:
                out_dir = self._runtime_render_dir()
            except Exception as e:
                await query.edit_message_text(f"❌ 渲染目录不可写：{e}")
                return ST_PREVIEW
            bg_path = await self._resolve_cover_background(query.message, draft, out_dir)
            hl = draft.highlights or ["实拍真房源", "中文顾问", "可预约看房"]
            cover_kwargs = dict(
                project=(draft.title or "侨联地产").strip() or "侨联地产",
                property_type=(draft.layout or "精选房源").strip() or "精选房源",
                area=(draft.area or "金边").strip() or "金边",
                size=(draft.size_sqm or "—").strip() or "—",
                floor=(draft.fee_note or "—").strip() or "—",
                price=(draft.price or "面议").strip() or "面议",
                highlights=hl,
            )
            styles = [
                ("minimal", "🅰️ A款：清爽信息条"),
                ("price_tag", "🅱️ B款：价格角标"),
            ]
            chat_id = query.message.chat_id
            sent_any = False
            for style_code, style_label in styles:
                out_path = str(out_dir / f"compare_{draft.listing_id}_{style_code}.jpg")
                try:
                    generate_house_cover(bg_path, out_path, style=style_code, **cover_kwargs)
                    with open(out_path, "rb") as f:
                        await context.bot.send_photo(
                            chat_id=chat_id,
                            photo=f,
                            caption=f"{style_label}\n\n点下方按钮选用此款：",
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton(f"✅ 选用此款", callback_data=f"style:pick:{style_code}"),
                            ]]),
                        )
                    sent_any = True
                except Exception as e:
                    logger.warning("compare_covers: 生成 %s 失败: %s", style_code, e)
                    await context.bot.send_message(chat_id=chat_id, text=f"⚠️ {style_label} 生成失败：{e}")
            if sent_any:
                current = draft.cover_style or "minimal"
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"👆 对比两款后，点图片下方「✅ 选用此款」设定封面。\n当前选定款式：<code>{current}</code>",
                    reply_markup=preview_keyboard(),
                    parse_mode=ParseMode.HTML,
                )
            return ST_PREVIEW

        if action in {"publish", "publish_variants"}:
            draft = self._draft(context)
            uid = update.effective_user.id
            listing_key = str(draft.listing_id)
            ch = self.settings.channel_id
            if not ch:
                await query.edit_message_text("❌ CHANNEL_ID 未配置，无法发频道。")
                return ST_PREVIEW

            type_cn = TYPE_LABELS.get(draft.property_type, draft.property_type or "公寓")
            self.db.save_listing(
                {
                    "listing_id": listing_key,
                    "type": type_cn,
                    "area": draft.area,
                    "project": (draft.community or draft.title or "").strip(),
                    "title": draft.title,
                    "price": draft.price,
                    "layout": draft.layout,
                    "size": draft.size_sqm,
                    "floor": draft.fee_note,
                    "deposit": draft.deposit_rule or "押一付一",
                    "available_date": draft.available_date or "随时入住",
                    "tags": draft.tags,
                    "highlights": draft.highlights,
                    "cost_notes": "",
                    "advisor_comment": draft.advisor_note,
                    "drawbacks": [],
                    "status": "published",
                }
            )

            post_dict = dict(draft.to_dict(uid))
            post_dict["listing_id"] = listing_key
            post_dict["size"] = draft.size_sqm
            post_dict["floor"] = draft.fee_note
            post_dict["project"] = draft.community or draft.title
            post_dict["advisor_comment"] = draft.advisor_note

            contact = self.settings.default_contact_handle
            body_variants = build_post_variants(post_dict)
            # 默认发布按 A/B/C 权重抽样，避免长期只有单一文案。
            abc_pool = body_variants[:3] if len(body_variants) >= 3 else body_variants
            variant_code = random.choices(
                ["a", "b", "c"][: len(abc_pool)],
                weights=[0.4, 0.3, 0.3][: len(abc_pool)],
                k=1,
            )[0] if abc_pool else "a"
            variant_index = {"a": 0, "b": 1, "c": 2}.get(variant_code, 0)
            body_html = abc_pool[variant_index][1] if abc_pool else build_post_text(post_dict, contact)
            _maps_url = self._build_maps_url(
                draft.community or draft.title or "",
                draft.area or "",
                draft.google_maps_url or "",
            )
            kb = publish_post_keyboard(
                listing_key,
                draft.area or "",
                self.settings.user_bot_username,
                channel_username=self.settings.channel_username,
                discussion_group_link=self.settings.discussion_group_link,
                maps_url=_maps_url,
            )

            bot = context.bot
            try:
                if action == "publish_variants":
                    await query.edit_message_text("🧪 正在批量发文案版本，请稍候…")
                    sent_count = 0
                    # 每个文案版本单独发一条，房源不变，便于频道内直观对比
                    for idx, (variant_name, variant_text) in enumerate(body_variants, start=1):
                        await bot.send_message(
                            chat_id=ch,
                            text=f"【文案{idx}/{len(body_variants)}｜{variant_name}】\n\n{variant_text}",
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                            reply_markup=kb,
                        )
                        sent_count += 1
                        await asyncio.sleep(0.5)
                    await query.edit_message_text(
                        f"✅ 已发 {sent_count} 条文案对比帖\n编号：<code>{listing_key}</code>",
                        parse_mode=ParseMode.HTML,
                    )
                    self._reset_draft(context)
                    return ConversationHandler.END

                # 1) 长文 HTML，不带按钮（与 MediaGroup 无法挂键盘的限制解耦）
                text_msg = await bot.send_message(
                    chat_id=ch,
                    text=body_html,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                await asyncio.sleep(0.4)

                # 2) 生成品牌封面并上传
                media_msg = None
                out_dir = self._runtime_render_dir()
                cover_path = str(out_dir / f"pub_cover_{draft.listing_id}_{draft.cover_style}.jpg")
                bg_path = await self._resolve_cover_background(query.message, draft, out_dir)
                generate_house_cover(
                    bg_path,
                    cover_path,
                    project=(draft.title or "侨联地产").strip() or "侨联地产",
                    property_type=(draft.layout or "精选房源").strip() or "精选房源",
                    area=(draft.area or "金边").strip() or "金边",
                    size=(draft.size_sqm or "—").strip() or "—",
                    floor=(draft.fee_note or "—").strip() or "—",
                    price=(draft.price or "面议").strip() or "面议",
                    highlights=draft.highlights or ["实拍真房源", "中文顾问", "可预约看房"],
                    style=draft.cover_style,
                )
                with open(cover_path, "rb") as f:
                    media_msg = await bot.send_photo(
                        chat_id=ch,
                        photo=f,
                        caption=CHANNEL_BUTTON_PROMPT,
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb,
                    )

                # 发完封面图后立即更新按钮，注入真实 channel_message_id 以生成评论区链接
                try:
                    _channel_message_id = int(media_msg.message_id)
                    _kb_with_detail = publish_post_keyboard(
                        listing_key,
                        draft.area or "",
                        self.settings.user_bot_username,
                        channel_username=self.settings.channel_username,
                        channel_message_id=_channel_message_id,
                        discussion_group_link=self.settings.discussion_group_link,
                        maps_url=_maps_url,
                    )
                    await bot.edit_message_reply_markup(
                        chat_id=ch,
                        message_id=media_msg.message_id,
                        reply_markup=_kb_with_detail,
                    )
                except Exception as _e:
                    logger.warning("更新详情按钮失败（不影响发布）: %s", _e)

                # 3) 如果用户上传了原图/视频，发讨论区而非频道主贴（主贴只保留封面+按钮，讨论区承载实拍）
                if draft.media_file_id and draft.media_type in {"photo", "video"}:
                    await asyncio.sleep(0.5)
                    discuss_id = await resolve_discussion_id(bot)
                    media_dest = discuss_id if discuss_id else ch
                    if not discuss_id:
                        logger.info("讨论组未配置，实拍图回退发频道主贴: listing=%s", listing_key)
                    try:
                        if draft.media_type == "photo":
                            wm_photo = None
                            try:
                                tg_file = await bot.get_file(draft.media_file_id)
                                raw = await tg_file.download_as_bytearray()
                                wm_photo = add_detail_logo_watermark(bytes(raw))
                            except Exception as e:
                                logger.warning("原图加 logo 失败，回退原图发送: %s", e)
                            await bot.send_photo(
                                chat_id=media_dest,
                                photo=wm_photo if wm_photo is not None else draft.media_file_id,
                                caption="📷 实拍图",
                            )
                        else:
                            await bot.send_video(
                                chat_id=media_dest,
                                video=draft.media_file_id,
                                caption="🎥 实拍视频",
                            )
                    except Exception as e:
                        logger.warning("Failed to send original %s: %s", draft.media_type, e)

                post_id = f"v2_{uuid.uuid4().hex[:16]}"
                try:
                    bridge = load_discussion_bridge()
                    bridge.setdefault("publish_queue", []).append(
                        {"channel_post_id": int(text_msg.message_id), "t": time.time()}
                    )
                    if len(bridge["publish_queue"]) > 50:
                        bridge["publish_queue"] = bridge["publish_queue"][-50:]
                    save_discussion_bridge(bridge)

                    self.db.create_post_record(
                        {
                            "post_id": post_id,
                            "listing_id": listing_key,
                            "draft_id": None,
                            "platform": "telegram",
                            "channel_chat_id": str(ch),
                            "channel_message_id": str(text_msg.message_id),
                            "media_group_id": str(media_msg.message_id) if media_msg else None,
                            "caption_message_id": str(media_msg.message_id) if media_msg else None,
                            "button_message_id": str(media_msg.message_id) if media_msg else str(text_msg.message_id),
                            "post_text": body_html[:8000],
                            "published_by": str(uid),
                        }
                    )
                    with sqlite3.connect(self.settings.sqlite_path) as conn:
                        conn.execute(
                            """
                            INSERT INTO publish_analytics
                            (draft_id, post_id, message_id, listing_id, area, property_type,
                             monthly_rent, caption_variant, publish_hour, publish_day_of_week, published_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                None,
                                post_id,
                                str(text_msg.message_id),
                                listing_key,
                                draft.area or "",
                                type_cn,
                                draft.price or "",
                                variant_code,
                                datetime.now().hour,
                                datetime.now().weekday(),
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            ),
                        )
                        conn.commit()
                except Exception as log_err:
                    logger.warning("create_post_record: %s", log_err)

                await query.edit_message_text(
                    f"✅ 已发布到频道\n编号：<code>{listing_key}</code>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                logger.exception("v2 发频道失败")
                await query.edit_message_text(f"❌ 发布失败：{e}")
                return ST_PREVIEW

        self._reset_draft(context)
        return ConversationHandler.END

    async def style_actions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """处理封面模板风格选择。"""
        query = update.callback_query
        await query.answer()
        data = query.data

        if data == "style:back":
            await self.show_preview(update, context)
            return ST_PREVIEW

        if data.startswith("style:pick:"):
            # 来自「对比两款封面」图片下方的直接选款按钮（不尝试编辑图片消息为文字）
            style = data.split(":", 2)[2]
            draft = self._draft(context)
            draft.cover_style = style
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
    register_autopilot_features(application, include_cancel=False)

    async def post_init(app):
        # 仅暴露高频命令，减少管理员命令噪音。
        commands = [
            BotCommand("start", "打开后台面板"),
            BotCommand("menu", "显示管理员面板"),
            BotCommand("pending", "查看待审草稿"),
            BotCommand("send", "立即发布：/send DRF_xxx"),
            BotCommand("new", "手工新建并发布"),
            BotCommand("send_variants", "多文案对比发布"),
            BotCommand("ops", "一屏总览"),
            BotCommand("status", "运行状态"),
            BotCommand("slots", "发帖时段"),
            BotCommand("pause", "暂停队列"),
            BotCommand("resume", "恢复队列"),
            BotCommand("intake", "开始微信导入"),
            BotCommand("intake_done", "导入完成并入库"),
            BotCommand("intake_pending", "查看导入草稿"),
            BotCommand("logs", "最近发布日志"),
            BotCommand("cover_test", "封面测试"),
            BotCommand("quick", "命令速查"),
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
    application.add_handler(CallbackQueryHandler(bot.admin_menu_action, pattern=r"^cmd:"))
    application.add_handler(conv)
    application.add_handler(
        MessageHandler(filters.ALL & ~filters.COMMAND, bot.capture_discussion_forward),
        group=1,
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
