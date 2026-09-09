from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)


def install_intake_media_patch() -> None:
    """Store publisher intake photos beside the configured DB, not under repo/data.

    The publisher service may run as a non-root user while the repository tree is
    owned by root.  DB_PATH already points at the runtime-writable data directory,
    so manual intake media must use the same writable root.
    """
    import autopilot_publish_bot as autopilot

    async def on_photo_private(update, context) -> None:
        if not autopilot._is_admin(update.effective_user.id):
            return
        if context.user_data.get("await") not in {"intake_text", "intake_images"}:
            return
        photos = update.message.photo or []
        if not photos:
            return

        best = photos[-1]
        try:
            telegram_file = await context.bot.get_file(best.file_id)
            inbox = Path(autopilot.DB_PATH).resolve().parent / "wechat_inbox"
            inbox.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            local_path = inbox / f"wx_{ts}_{best.file_unique_id}.jpg"
            await telegram_file.download_to_drive(custom_path=str(local_path))

            bucket = context.user_data.setdefault("intake_images", [])
            bucket.append(str(local_path))
            context.user_data["await"] = "intake_images"

            caption = str(update.message.caption or "").strip()
            if caption:
                old = str(context.user_data.get("intake_text") or "").strip()
                context.user_data["intake_text"] = (
                    (old + "\n" + caption).strip()[:12000] if old else caption[:12000]
                )

            text_len = len(str(context.user_data.get("intake_text") or ""))
            await update.message.reply_text(
                f"📷 已收图 {len(bucket)} 张" + (f" · 文字 {text_len} 字" if text_len else ""),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("✅ 完成导入", callback_data="cmd:intake_done")]]
                ),
            )
        except Exception:
            logger.exception(
                "save intake photo failed: db=%s inbox=%s",
                autopilot.DB_PATH,
                str(Path(autopilot.DB_PATH).resolve().parent / "wechat_inbox"),
            )
            await update.message.reply_text(
                "图片保存失败。请稍后重发；系统已保留当前文字和已成功接收的图片。"
            )

    autopilot.on_photo_private = on_photo_private
