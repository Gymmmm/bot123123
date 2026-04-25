"""
admin_server.py · 侨联地产频道发布后台

路由一览：
    GET  /                  首页：所有房源列表
    GET  /new               新建房源表单
    POST /new               保存新房源
    GET  /edit/<lid>        编辑房源表单
    POST /edit/<lid>        保存编辑
    POST /publish/<lid>     发布到频道
    POST /repub/<lid>       重发（删旧+发新）
    POST /offline/<lid>     下架
    POST /rented/<lid>      标记已租
    POST /price/<lid>       仅改价（不重发）
    POST /delete/<lid>      删除草稿（仅 draft 状态可删）
    GET  /uploads/<filename> 图片预览

用 Waitress 或 Flask 内置 server 均可。
"""

import os
import json
import traceback
from pathlib import Path
from datetime import datetime

from flask import (
    Flask, request, redirect, url_for,
    render_template, flash, send_from_directory,
    jsonify,
)
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

import db
import publisher

load_dotenv()

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("FLASK_SECRET", "qiaolian-admin-secret-2025")

UPLOAD_DIR   = Path(os.getenv("UPLOAD_DIR", "uploads"))
ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

STATUS_LABEL = {
    "draft":     ("⬜ 草稿",   "badge-draft"),
    "published": ("🟢 已发布", "badge-pub"),
    "rented":    ("🔵 已租出", "badge-rented"),
    "offline":   ("🔴 已下架", "badge-off"),
}


# ── 初始化 ────────────────────────────────────────────────

@app.before_request
def _init():
    db.init_db()
    app.before_request_funcs[None].remove(_init)


# ── 图片上传 helper ───────────────────────────────────────

def _save_uploads(files) -> list[str]:
    """保存上传图片，返回路径列表。"""
    paths = []
    for f in files:
        if not f or not f.filename:
            continue
        ext = Path(f.filename).suffix.lower()
        if ext not in ALLOWED_EXTS:
            continue
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        name = secure_filename(f"img_{ts}{ext}")
        dest = UPLOAD_DIR / name
        f.save(str(dest))
        paths.append(str(dest))
    return paths


@app.route("/uploads/<path:filename>")
def uploaded_file(filename):
    return send_from_directory(UPLOAD_DIR, filename)


# ── 首页：房源列表 ────────────────────────────────────────

@app.route("/")
def index():
    status_filter = request.args.get("status")
    listings = db.list_listings(status_filter)
    # 附加最新频道帖子信息
    for l in listings:
        post = db.get_latest_channel_post(l["listing_id"])
        l["_post"] = post
        l["_status_label"] = STATUS_LABEL.get(l.get("status", "draft"), ("", ""))
    return render_template("index.html", listings=listings,
                           status_filter=status_filter, STATUS_LABEL=STATUS_LABEL)


# ── 新建房源 ──────────────────────────────────────────────

@app.route("/new", methods=["GET"])
def new_listing():
    return render_template("listing_form.html", listing=None, action="new", title="新建房源")


@app.route("/new", methods=["POST"])
def create_listing():
    data = _form_to_dict(request.form)
    # 处理图片上传
    files = request.files.getlist("images")
    paths = _save_uploads(files)
    if paths:
        data["images"] = json.dumps(paths, ensure_ascii=False)
        data["cover_image"] = paths[0]

    lid = db.create_listing(data)
    flash(f"✅ 房源 {lid} 已保存为草稿", "success")
    return redirect(url_for("index"))


# ── 编辑房源 ──────────────────────────────────────────────

@app.route("/edit/<lid>", methods=["GET"])
def edit_listing(lid):
    listing = db.get_listing(lid)
    if not listing:
        flash("房源不存在", "error")
        return redirect(url_for("index"))
    return render_template("listing_form.html", listing=listing,
                           action=f"edit/{lid}", title="编辑房源")


@app.route("/edit/<lid>", methods=["POST"])
def save_listing(lid):
    listing = db.get_listing(lid)
    if not listing:
        flash("房源不存在", "error")
        return redirect(url_for("index"))

    data  = _form_to_dict(request.form)
    files = request.files.getlist("images")
    paths = _save_uploads(files)

    if paths:
        # 新图追加到已有图
        existing = listing.get("images") or []
        if isinstance(existing, str):
            try:
                existing = json.loads(existing)
            except Exception:
                existing = []
        combined = existing + paths
        data["images"]      = json.dumps(combined, ensure_ascii=False)
        data["cover_image"] = combined[0] if combined else ""

    db.update_listing(lid, data)
    flash(f"✅ 房源 {lid} 已更新", "success")
    return redirect(url_for("index"))


# ── 发布到频道 ────────────────────────────────────────────

@app.route("/publish/<lid>", methods=["POST"])
def publish(lid):
    listing = db.get_listing(lid)
    if not listing:
        flash("房源不存在", "error")
        return redirect(url_for("index"))

    try:
        mg_id, media_ids, btn_id, file_ids = publisher.publish_listing(listing)
        db.save_channel_post(
            listing_id        = lid,
            channel_id        = publisher.CHANNEL_ID,
            media_group_id    = mg_id,
            media_message_ids = media_ids,
            button_message_id = btn_id,
            file_ids          = file_ids,
        )
        # 回写 file_id，下次复用
        db.update_file_ids(lid, file_ids)
        db.set_listing_status(lid, "published")
        flash(f"🟢 房源 {lid} 已发布到频道", "success")
    except Exception as e:
        traceback.print_exc()
        flash(f"❌ 发布失败：{e}", "error")

    return redirect(url_for("index"))


# ── 重发 ──────────────────────────────────────────────────

@app.route("/repub/<lid>", methods=["POST"])
def repub(lid):
    listing  = db.get_listing(lid)
    old_post = db.get_latest_channel_post(lid)
    if not listing:
        flash("房源不存在", "error")
        return redirect(url_for("index"))

    try:
        mg_id, media_ids, btn_id, file_ids = publisher.repub_listing(listing, old_post or {})
        db.save_channel_post(
            listing_id        = lid,
            channel_id        = publisher.CHANNEL_ID,
            media_group_id    = mg_id,
            media_message_ids = media_ids,
            button_message_id = btn_id,
            file_ids          = file_ids,
        )
        db.update_file_ids(lid, file_ids)
        db.set_listing_status(lid, "published")
        flash(f"🔄 房源 {lid} 已重发", "success")
    except Exception as e:
        traceback.print_exc()
        flash(f"❌ 重发失败：{e}", "error")

    return redirect(url_for("index"))


# ── 下架 ──────────────────────────────────────────────────

@app.route("/offline/<lid>", methods=["POST"])
def offline(lid):
    listing  = db.get_listing(lid)
    old_post = db.get_latest_channel_post(lid)
    if not listing:
        flash("房源不存在", "error")
        return redirect(url_for("index"))
    if not old_post or not old_post.get("button_message_id"):
        flash("未找到已发布帖子", "error")
        return redirect(url_for("index"))

    try:
        publisher.offline_listing(listing, old_post)
        db.update_channel_post_status(lid, "offline")
        db.set_listing_status(lid, "offline")
        flash(f"🔴 房源 {lid} 已下架", "success")
    except Exception as e:
        traceback.print_exc()
        flash(f"❌ 下架失败：{e}", "error")

    return redirect(url_for("index"))


# ── 已租 ──────────────────────────────────────────────────

@app.route("/rented/<lid>", methods=["POST"])
def rented(lid):
    listing  = db.get_listing(lid)
    old_post = db.get_latest_channel_post(lid)
    if not listing:
        flash("房源不存在", "error")
        return redirect(url_for("index"))
    if not old_post or not old_post.get("button_message_id"):
        flash("未找到已发布帖子", "error")
        return redirect(url_for("index"))

    try:
        publisher.rented_listing(listing, old_post)
        db.update_channel_post_status(lid, "rented")
        db.set_listing_status(lid, "rented")
        flash(f"🔵 房源 {lid} 已标记为已租出", "success")
    except Exception as e:
        traceback.print_exc()
        flash(f"❌ 操作失败：{e}", "error")

    return redirect(url_for("index"))


# ── 改价（只改价，不重发帖子）────────────────────────────

@app.route("/price/<lid>", methods=["POST"])
def update_price(lid):
    new_price = request.form.get("new_price", "").strip()
    if not new_price:
        flash("请输入新价格", "error")
        return redirect(url_for("index"))
    db.update_listing_price(lid, new_price)
    flash(f"💰 房源 {lid} 价格已更新为 {new_price}（帖子未变动，如需同步请重发）", "success")
    return redirect(url_for("index"))


# ── 删除草稿 ──────────────────────────────────────────────

@app.route("/delete/<lid>", methods=["POST"])
def delete_listing(lid):
    listing = db.get_listing(lid)
    if not listing:
        flash("房源不存在", "error")
        return redirect(url_for("index"))
    if listing.get("status") != "draft":
        flash("只能删除草稿状态的房源", "error")
        return redirect(url_for("index"))
    conn = db.get_conn()
    conn.execute("DELETE FROM listings WHERE listing_id = ?", (lid,))
    conn.commit()
    conn.close()
    flash(f"🗑 草稿 {lid} 已删除", "success")
    return redirect(url_for("index"))


# ── API：删除单张图片 ─────────────────────────────────────

@app.route("/api/remove_image/<lid>", methods=["POST"])
def api_remove_image(lid):
    idx = request.json.get("index", -1)
    listing = db.get_listing(lid)
    if not listing:
        return jsonify({"ok": False, "msg": "not found"})
    imgs = listing.get("images") or []
    if 0 <= idx < len(imgs):
        imgs.pop(idx)
        db.update_listing(lid, {"images": json.dumps(imgs, ensure_ascii=False),
                                 "cover_image": imgs[0] if imgs else ""})
    return jsonify({"ok": True, "images": imgs})


# ── 表单数据解析 helper ───────────────────────────────────

def _form_to_dict(form) -> dict:
    fields = [
        "listing_id", "type", "area", "project", "title",
        "price", "layout", "size", "deposit", "contract_term",
        "available_date", "tags", "highlights", "cost_notes",
        "advisor_comment", "drawbacks",
    ]
    return {k: form.get(k, "").strip() for k in fields}


# ── 入口 ──────────────────────────────────────────────────

if __name__ == "__main__":
    db.init_db()
    port = int(os.getenv("ADMIN_PORT", 5005))
    print(f"[Admin] 侨联发布后台启动，访问 http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
