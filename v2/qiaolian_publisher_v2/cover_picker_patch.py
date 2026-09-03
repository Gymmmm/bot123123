from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from .config import get_settings

_INSTALLED = False
_MANUAL_COVER_PATH = ""


def _display_qc(listing_id: str) -> str:
    match = re.search(r"(\d{1,8})", str(listing_id or ""))
    return f"QC{int(match.group(1)):04d}" if match else str(listing_id or "")


def _resolve_draft(conn: sqlite3.Connection, token: str):
    raw = str(token or "").strip()
    if not raw:
        return None
    row = conn.execute(
        "SELECT id,draft_id,listing_id,source_post_id,review_status FROM drafts WHERE draft_id=? LIMIT 1",
        (raw,),
    ).fetchone()
    if row:
        return row
    match = re.search(r"(\d{1,8})", raw)
    if not match:
        return None
    internal = f"l_{int(match.group(1))}"
    return conn.execute(
        """SELECT id,draft_id,listing_id,source_post_id,review_status
           FROM drafts WHERE lower(listing_id)=lower(?) ORDER BY id DESC LIMIT 1""",
        (internal,),
    ).fetchone()


def _source_assets(conn: sqlite3.Connection, source_post_id: Any) -> list[sqlite3.Row]:
    return conn.execute(
        """SELECT id,asset_id,local_path,is_cover,sort_order,status
           FROM media_assets
           WHERE owner_type='source_post'
             AND (CAST(owner_ref_id AS TEXT)=CAST(? AS TEXT) OR owner_ref_key=CAST(? AS TEXT))
             AND asset_type='photo' AND status='active'
           ORDER BY COALESCE(sort_order,999999), id""",
        (source_post_id, source_post_id),
    ).fetchall()


async def coverpick_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings = get_settings()
    user = update.effective_user
    message = update.effective_message
    if not user or user.id not in settings.admin_ids:
        return
    if not context.args:
        await message.reply_text("用法：/coverpick QC0102")
        return

    with sqlite3.connect(settings.sqlite_path) as conn:
        conn.row_factory = sqlite3.Row
        draft = _resolve_draft(conn, context.args[0])
        if not draft:
            await message.reply_text("没找到这套待审房源。")
            return
        frozen = conn.execute(
            "SELECT status FROM publication_packages WHERE draft_id=? AND status IN ('approved','published') LIMIT 1",
            (draft["draft_id"],),
        ).fetchone()
        if frozen:
            await message.reply_text("这套发布包已经批准/发布，主图已冻结，不能再改。")
            return
        assets = _source_assets(conn, draft["source_post_id"])

    paths = [str(row["local_path"] or "") for row in assets if Path(str(row["local_path"] or "")).is_file()]
    if not paths:
        await message.reply_text("这套没有可读取的本地实拍图。")
        return

    from photo_ranker import get_cover_candidates_from_paths

    candidates = get_cover_candidates_from_paths(paths, limit=5)
    if not candidates:
        await message.reply_text("没有找到可用主图候选。")
        return

    by_path = {str(Path(str(row["local_path"])).resolve()): row for row in assets if row["local_path"]}
    await message.reply_text(
        f"🖼 <b>选择主图｜{_display_qc(draft['listing_id'])}</b>\n\n"
        "已先过滤明显坏图，再按 TG 封面适配度选 Top 5。\n"
        "点你要的那张，完整实拍顺序不会改变。",
        parse_mode="HTML",
    )
    for index, item in enumerate(candidates, start=1):
        path = str(Path(item["file"]).resolve())
        asset = by_path.get(path)
        if not asset:
            continue
        label = "✅ 当前主图" if int(asset["is_cover"] or 0) else f"设为主图 #{index}"
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton(label, callback_data=f"coverpick:{int(draft['id'])}:{int(asset['id'])}")]]
        )
        caption = (
            f"候选 {index}/5｜分数 {item.get('score', 0):.1f}\n"
            f"比例 {item.get('ratio', 0)}｜清晰 {item.get('sharpness', 0):.0f}｜曝光 {item.get('exposure', 0):.0f}"
        )
        try:
            with open(path, "rb") as photo:
                await context.bot.send_photo(
                    chat_id=message.chat_id,
                    photo=photo,
                    caption=caption,
                    reply_markup=keyboard,
                )
        except Exception:
            await context.bot.send_message(
                chat_id=message.chat_id,
                text=f"{caption}\n{Path(path).name}",
                reply_markup=keyboard,
            )


async def coverpick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings = get_settings()
    query = update.callback_query
    user = update.effective_user
    if not query or not user or user.id not in settings.admin_ids:
        return
    await query.answer("正在设置主图…")
    parts = str(query.data or "").split(":")
    if len(parts) != 3:
        return
    try:
        draft_pk = int(parts[1])
        asset_pk = int(parts[2])
    except ValueError:
        return

    with sqlite3.connect(settings.sqlite_path) as conn:
        conn.row_factory = sqlite3.Row
        draft = conn.execute(
            "SELECT id,draft_id,listing_id,source_post_id FROM drafts WHERE id=? LIMIT 1",
            (draft_pk,),
        ).fetchone()
        if not draft:
            await query.message.reply_text("这套草稿已经不存在。")
            return
        frozen = conn.execute(
            "SELECT status FROM publication_packages WHERE draft_id=? AND status IN ('approved','published') LIMIT 1",
            (draft["draft_id"],),
        ).fetchone()
        if frozen:
            await query.message.reply_text("这套发布包已经批准/发布，主图已冻结。")
            return
        chosen = conn.execute(
            """SELECT id,local_path FROM media_assets
               WHERE id=? AND owner_type='source_post'
                 AND (CAST(owner_ref_id AS TEXT)=CAST(? AS TEXT) OR owner_ref_key=CAST(? AS TEXT))
               LIMIT 1""",
            (asset_pk, draft["source_post_id"], draft["source_post_id"]),
        ).fetchone()
        if not chosen:
            await query.message.reply_text("这张图不属于当前房源。")
            return
        conn.execute(
            """UPDATE media_assets SET is_cover=0
               WHERE owner_type='source_post'
                 AND (CAST(owner_ref_id AS TEXT)=CAST(? AS TEXT) OR owner_ref_key=CAST(? AS TEXT))""",
            (draft["source_post_id"], draft["source_post_id"]),
        )
        conn.execute("UPDATE media_assets SET is_cover=1 WHERE id=?", (asset_pk,))
        conn.commit()

    package_message = ""
    try:
        # Critical bridge: source-photo selection alone is not a publishable cover.
        # Regenerate the canonical HTML cover and register it as the draft's
        # cover asset before rebuilding the frozen package.
        from cover_generator import CoverGenerator
        from publication_package import build_package

        generator = CoverGenerator(settings.sqlite_path)
        cover_asset_id, cover_path = generator.generate_for_draft(str(draft["draft_id"]))
        if not cover_asset_id or not cover_path or not Path(cover_path).is_file():
            raise RuntimeError("cover_regeneration_failed")

        package = build_package(settings.sqlite_path, str(draft["draft_id"]))
        package_message = (
            f"\n封面已重生成：{Path(cover_path).name}"
            f"\n新预览包：{package.get('package_id', '-')}"
        )
    except Exception as exc:
        package_message = f"\n主图已保存；正式封面暂未重建：{exc}"

    await query.message.reply_text(
        f"✅ 已设为主图｜{_display_qc(draft['listing_id'])}"
        f"{package_message}\n\n"
        "现在回到待发布房源继续选封面样式/预览即可。"
    )


def _install_publication_package_hooks() -> None:
    import publication_package as package
    from photo_ranker import get_best_cover_from_paths

    def patched_paths(conn: sqlite3.Connection, source_post_id: Any) -> list[str]:
        global _MANUAL_COVER_PATH
        _MANUAL_COVER_PATH = ""
        rows = conn.execute(
            """SELECT local_path,is_cover FROM media_assets
               WHERE owner_type='source_post'
                 AND (CAST(owner_ref_id AS TEXT)=CAST(? AS TEXT) OR owner_ref_key=CAST(? AS TEXT))
                 AND asset_type='photo' AND status='active'
               ORDER BY is_cover DESC, COALESCE(sort_order,999999), id""",
            (source_post_id, source_post_id),
        ).fetchall()
        paths: list[str] = []
        for row in rows:
            path = str(row[0] or "")
            if path and Path(path).is_file() and path not in paths:
                paths.append(path)
                if int(row[1] or 0) and not _MANUAL_COVER_PATH:
                    _MANUAL_COVER_PATH = path
        if paths:
            return paths

        row = conn.execute("SELECT raw_images_json FROM source_posts WHERE id=?", (source_post_id,)).fetchone()
        if not row or not row[0]:
            return []
        import json

        try:
            raw_images = json.loads(row[0])
        except Exception:
            return []
        for item in raw_images if isinstance(raw_images, list) else []:
            path = item if isinstance(item, str) else item.get("local_path") or item.get("path")
            if path and Path(path).is_file() and path not in paths:
                paths.append(str(path))
        return paths

    def patched_select_cover_source(paths: list[str], *, property_type: str = "") -> str:
        _ = property_type
        if _MANUAL_COVER_PATH and _MANUAL_COVER_PATH in paths:
            return _MANUAL_COVER_PATH
        best = get_best_cover_from_paths(paths)
        if not best:
            raise ValueError("missing_usable_images")
        return str(best["file"])

    package._paths = patched_paths
    package._select_cover_source = patched_select_cover_source


def install_cover_picker() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    _install_publication_package_hooks()

    original_run_polling = Application.run_polling

    def patched_run_polling(self, *args, **kwargs):
        self.add_handler(CommandHandler("coverpick", coverpick_command), group=-2)
        self.add_handler(CallbackQueryHandler(coverpick_callback, pattern=r"^coverpick:\d+:\d+$"), group=-2)
        return original_run_polling(self, *args, **kwargs)

    Application.run_polling = patched_run_polling
