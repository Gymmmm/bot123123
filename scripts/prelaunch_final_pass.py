from pathlib import Path
import re

BRANCH_FILES = {
    'meihua_publisher.py': Path('meihua_publisher.py'),
    'publication_package.py': Path('publication_package.py'),
    'discussion_map_store.py': Path('discussion_map_store.py'),
    'v2/qiaolian_publisher_v2/keyboards.py': Path('v2/qiaolian_publisher_v2/keyboards.py'),
    'v2/qiaolian_publisher_v2/formatters.py': Path('v2/qiaolian_publisher_v2/formatters.py'),
    'autopilot_publish_bot.py': Path('autopilot_publish_bot.py'),
}


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f'missing patch target: {label}')
    return text.replace(old, new, 1)


# 1) Telegram album: 4:3 white card, no hard crop; discussion becomes compact 2-layer.
p = BRANCH_FILES['meihua_publisher.py']
text = p.read_text(encoding='utf-8')
text = replace_once(
    text,
    '''# Telegram 手机端 grouped media 统一使用 4:5 竖图派生稿。\n# 原始素材永不覆盖；横图/方图使用模糊延展背景完整保留主体，不做硬裁切。\nCHANNEL_ALBUM_SIZE = (1080, 1350)\nCHANNEL_ALBUM_LAYOUT = "portrait_4_5"\n''',
    '''# Telegram grouped media / Bot 实拍统一使用 4:3 白卡片派生稿。\n# 原始素材永不覆盖；主体完整 contain，四周留统一白边，频道宫格更干净。\nCHANNEL_ALBUM_SIZE = (1200, 900)\nCHANNEL_ALBUM_LAYOUT = "clean_white_4_3"\nCHANNEL_ALBUM_MARGIN = 22\nCHANNEL_ALBUM_CORNER_RADIUS = 18\n''',
    'album constants',
)
start = text.index('def _fit_to_45_canvas(')
end = text.index('\n\ndef _album_layout_is_one_three()', start)
new_album = r'''def _fit_to_clean_album_card(
    image: Image.Image,
    *,
    canvas_size: tuple[int, int] = CHANNEL_ALBUM_SIZE,
) -> Image.Image:
    """生成 4:3 白卡片实拍图；照片完整保留，不为填满画布硬裁主体。"""
    src = image.convert("RGB")
    cw, ch = canvas_size
    margin = max(10, int(CHANNEL_ALBUM_MARGIN))
    inner_w = max(1, cw - margin * 2)
    inner_h = max(1, ch - margin * 2)

    fg = src.copy()
    fg.thumbnail((inner_w, inner_h), Image.Resampling.LANCZOS)
    x = (cw - fg.width) // 2
    y = (ch - fg.height) // 2

    canvas = Image.new("RGBA", (cw, ch), (255, 255, 255, 255))
    # 极轻阴影只负责把白边从 Telegram 深色背景中分离出来。
    shadow = Image.new("RGBA", (cw, ch), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    radius = max(8, int(CHANNEL_ALBUM_CORNER_RADIUS))
    shadow_box = (x - 3, y - 3, x + fg.width + 3, y + fg.height + 3)
    sd.rounded_rectangle(shadow_box, radius=radius + 2, fill=(0, 0, 0, 28))
    shadow = shadow.filter(ImageFilter.GaussianBlur(5))
    canvas = Image.alpha_composite(canvas, shadow)

    mask = Image.new("L", (fg.width, fg.height), 0)
    md = ImageDraw.Draw(mask)
    md.rounded_rectangle((0, 0, fg.width - 1, fg.height - 1), radius=radius, fill=255)
    canvas.paste(fg, (x, y), mask)
    return canvas.convert("RGB")


def normalize_album_image(
    image_bytes: bytes,
    *,
    target_size: int = 1280,
    force_square: bool = False,
    fit_box: tuple[int, int] | None = None,
) -> bytes:
    """统一 Telegram 实拍派生图为 1200x900 白卡片；旧参数仅兼容调用。"""
    _ = (target_size, force_square, fit_box)
    im = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    im = _fit_to_clean_album_card(im)
    out = io.BytesIO()
    im.save(out, "JPEG", quality=92, optimize=True)
    return out.getvalue()
'''
text = text[:start] + new_album + text[end:]
text = text.replace('新发布布局固定为 portrait_4_5。', '新发布布局固定为 clean_white_4_3。')
text = text.replace('所有频道主相册和评论区相册统一 4:5，不按槽位改变比例。', '所有频道主相册和评论区相册统一 4:3 白卡片，不按槽位改变比例。')

start = text.index('async def send_discussion_three_segments(')
end = text.index('\ndef build_channel_caption(', start)
new_discussion = r'''async def send_discussion_three_segments(
    bot: Bot,
    channel_post_id: int,
    listing_id: str,
    post_token: str,
    *,
    listing: dict | None = None,
    extra_album: list | None = None,
    frozen_detail_text: str = "",
    attempts: int = 30,
    delay_seconds: float = 2.0,
) -> tuple[bool, bool]:
    """兼容旧函数名；新评论区固定为两层：补充实拍 → 预约/咨询。"""
    _ = frozen_detail_text  # 历史包字段继续接收，但新帖子不再重复详情正文。
    discussion_id = await resolve_discussion_chat_id(bot)
    if not discussion_id:
        logger.warning("讨论区发帖：无法获取讨论组 chat_id，跳过。channel_post_id=%s", channel_post_id)
        return False, False

    thread_reply_id = None
    for _attempt in range(max(1, attempts)):
        await asyncio.sleep(delay_seconds)
        mapping = load_discuss_map()
        thread_reply_id = mapping.get(str(channel_post_id))
        if thread_reply_id:
            break
    if not thread_reply_id:
        logger.warning("讨论区映射等待超时。channel_post_id=%s", channel_post_id)
        return False, False

    sent_any = False
    sent_extra_photos = False

    # 第一层：只放补充实拍。冻结图片已经是最终派生字节，不再二次处理。
    if extra_album:
        chunk = 10
        total_extra = len(extra_album)
        for batch_start in range(0, total_extra, chunk):
            batch_paths = extra_album[batch_start : batch_start + chunk]
            extra_media = []
            for j, path in enumerate(batch_paths):
                try:
                    data_bytes = Path(path).read_bytes()
                    buf = io.BytesIO(data_bytes)
                    buf.name = f"extra_{batch_start + j}.jpg"
                    if j == 0:
                        qc = _qc_code_from_draft({"listing_id": listing_id})
                        cap = (
                            f"📸 <b>更多实拍｜{he(qc)}</b>\n点击图片可左右滑动查看"
                            if batch_start == 0
                            else "📸 <b>更多实拍（续）</b>"
                        )
                        extra_media.append(InputMediaPhoto(media=buf, caption=cap[:1024], parse_mode=ParseMode.HTML))
                    else:
                        extra_media.append(InputMediaPhoto(media=buf))
                except Exception:
                    logger.exception("讨论区实拍处理失败，已跳过: %s", path)
            if not extra_media:
                continue
            try:
                if len(extra_media) == 1:
                    await bot.send_photo(
                        chat_id=discussion_id,
                        photo=extra_media[0].media,
                        caption=extra_media[0].caption,
                        parse_mode=ParseMode.HTML,
                        reply_to_message_id=int(thread_reply_id),
                        allow_sending_without_reply=True,
                    )
                else:
                    await bot.send_media_group(
                        chat_id=discussion_id,
                        media=extra_media,
                        reply_to_message_id=int(thread_reply_id),
                        allow_sending_without_reply=True,
                    )
                sent_any = True
                sent_extra_photos = True
            except Exception:
                logger.exception("讨论区实拍发送失败 batch_start=%s", batch_start)
            if batch_start + chunk < total_extra:
                await asyncio.sleep(0.6)

    # 第二层：只留两个高意向动作，不重复房源详情/费用/找房说明。
    try:
        action_links = _caption_action_links(
            listing_id,
            listing=listing or {},
            post_token=post_token,
            caption_variant="a",
        )
        await bot.send_message(
            chat_id=discussion_id,
            text=f"📅 <b>预约与咨询</b>\n{action_links}",
            parse_mode=ParseMode.HTML,
            reply_to_message_id=int(thread_reply_id),
            allow_sending_without_reply=True,
        )
        sent_any = True
    except Exception:
        logger.exception("讨论区行动入口发送失败。channel_post_id=%s", channel_post_id)

    return sent_any, sent_extra_photos
'''
text = text[:start] + new_discussion + text[end:]

# Successfully published new listings should become searchable; preserve explicit admin states.
text = replace_once(
    text,
    "               ) VALUES (?,?,?,?,?,?,'USD',?,?,?,?,?,?,?,?,?,'photo',?,?,'pending',?,?)\n",
    "               ) VALUES (?,?,?,?,?,?,'USD',?,?,?,?,?,?,?,?,?,'photo',?,?,'active',?,?)\n",
    'new listing active on publish',
)
text = replace_once(
    text,
    "                   source_post_url=excluded.source_post_url,\n                   -- Preserve administrator-controlled listing status (e.g. rented/offline).\n                   updated_at=excluded.updated_at\"\"\",\n",
    "                   source_post_url=excluded.source_post_url,\n                   -- A successfully published pending/draft listing becomes active; explicit admin states remain authoritative.\n                   status=CASE WHEN lower(coalesce(listings.status,'')) IN ('','pending','draft') THEN 'active' ELSE listings.status END,\n                   updated_at=excluded.updated_at\"\"\",\n",
    'status transition after publish',
)
p.write_text(text, encoding='utf-8')


# 2) Package routing: manual defaults to single-image buttons; collected landed/high-price gets richer album.
p = BRANCH_FILES['publication_package.py']
text = p.read_text(encoding='utf-8')
anchor = '''def _source_style_scope(source_type: str, source_name: str = "") -> str:\n    raw = f"{source_type or ''} {source_name or ''}".strip().lower()\n    # wechat_note/admin/manual are explicit operator imports; all collector\n    # sources use the collected defaults. This is routing metadata only.\n    return "manual" if any(token in raw for token in ("wechat_note", "manual", "admin_import", "手工", "管理导入")) else "collected"\n\n\n'''
insert = anchor + r'''def _default_publish_layout_for_scope(scope: str) -> str:
    """管理员自录默认单图按钮；采集房源默认原生相册+评论区。"""
    return "buttons" if str(scope or "").strip().lower() == "manual" else "links"


def _main_album_limit(*, property_type: str, price: Any, scope: str, publish_layout: str) -> int:
    """频道主相册张数：普通公寓4张，别墅/排屋或高价房6张；按钮帖只发首图。"""
    if str(publish_layout or "").lower() == "buttons" or str(scope or "").lower() == "manual":
        return 1
    kind = str(property_type or "").lower()
    landed = any(token in kind for token in ("别墅", "排屋", "联排", "双拼", "townhouse", "villa"))
    try:
        monthly = float(re.sub(r"[^0-9.]", "", str(price or "0")) or 0)
    except (TypeError, ValueError):
        monthly = 0
    return 6 if landed or monthly >= 1500 else 4


'''
text = replace_once(text, anchor, insert, 'publish routing helpers')
text = replace_once(
    text,
    '''    is_villa = "别墅" in listing or "villa" in listing\n    listing_type = "villa" if is_villa else "apartment"\n''',
    '''    is_villa = "别墅" in listing or "villa" in listing\n    is_townhouse = any(token in listing for token in ("排屋", "联排", "townhouse"))\n    listing_type = "villa" if is_villa else ("townhouse" if is_townhouse else "apartment")\n''',
    'townhouse routing',
)
text = replace_once(
    text,
    '''    if is_villa:\n        return {\n            "source_type": normalized_source,\n            "listing_type": "villa",\n            "media_type": "image",\n            "cover_template": "black_gold",\n        }\n\n''',
    '''    if is_villa or is_townhouse:\n        return {\n            "source_type": normalized_source,\n            "listing_type": listing_type,\n            "media_type": "image",\n            "cover_template": "black_gold",\n        }\n\n''',
    'landed black gold',
)
text = replace_once(
    text,
    '    publish_layout = layout_match.group(1).lower() if layout_match else "links"\n',
    '    publish_layout = layout_match.group(1).lower() if layout_match else _default_publish_layout_for_scope(styles["scope"])\n',
    'manual default buttons',
)
text = replace_once(
    text,
    '''    max_main_images = max(1, min(4, int(os.getenv("CHANNEL_MAIN_ALBUM_MAX", "4"))))\n    main = [str(cover)] + processed[: max_main_images - 1]\n    discussion = processed[max_main_images - 1 :]\n''',
    '''    max_main_images = _main_album_limit(\n        property_type=d.get("property_type") or "",\n        price=d.get("price"),\n        scope=styles["scope"],\n        publish_layout=publish_layout,\n    )\n    main = [str(cover)] + processed[: max_main_images - 1]\n    discussion = processed[max_main_images - 1 :]\n''',
    'main album routing',
)
p.write_text(text, encoding='utf-8')


# 3) Discussion compatibility loader must tolerate older posts schemas.
p = BRANCH_FILES['discussion_map_store.py']
text = p.read_text(encoding='utf-8')
old = '''        if not {"channel_message_id", "discuss_message_id"}.issubset(cols):\n            return {}\n        out: dict[str, int] = {}\n        rows = conn.execute(\n            """SELECT channel_message_id, discuss_message_id\n               FROM posts\n               WHERE channel_message_id IS NOT NULL\n                 AND discuss_message_id IS NOT NULL\n                 AND TRIM(CAST(discuss_message_id AS TEXT))<>''\n                 AND platform='telegram'\n                 AND publish_status IN ('published','success','ok')"""\n        ).fetchall()\n'''
new = '''        if not {"channel_message_id", "discuss_message_id"}.issubset(cols):\n            return {}\n        out: dict[str, int] = {}\n        clauses = [\n            "channel_message_id IS NOT NULL",\n            "discuss_message_id IS NOT NULL",\n            "TRIM(CAST(discuss_message_id AS TEXT))<>''",\n        ]\n        if "platform" in cols:\n            clauses.append("platform='telegram'")\n        if "publish_status" in cols:\n            clauses.append("publish_status IN ('published','success','ok')")\n        rows = conn.execute(\n            "SELECT channel_message_id, discuss_message_id FROM posts WHERE " + " AND ".join(clauses)\n        ).fetchall()\n'''
text = replace_once(text, old, new, 'discussion old schema safety')
p.write_text(text, encoding='utf-8')


# 4) Admin UI: truthful labels, townhouse intake, safe more-photos fallback.
p = BRANCH_FILES['v2/qiaolian_publisher_v2/keyboards.py']
text = p.read_text(encoding='utf-8')
text = text.replace('InlineKeyboardButton("🏠 房源队列", callback_data="cmd:queue")', 'InlineKeyboardButton("🏠 待发布房源", callback_data="cmd:queue")')
text = text.replace('InlineKeyboardButton("➕ 录入房源", callback_data="cmd:intake")', 'InlineKeyboardButton("➕ 添加房源", callback_data="cmd:intake")')
text = replace_once(
    text,
    '''        [\n            InlineKeyboardButton("🏪 商铺", callback_data="type:shop"),\n            InlineKeyboardButton("💼 办公室", callback_data="type:office"),\n        ],\n''',
    '''        [\n            InlineKeyboardButton("🏘 排屋", callback_data="type:townhouse"),\n            InlineKeyboardButton("🏪 商铺", callback_data="type:shop"),\n        ],\n        [InlineKeyboardButton("💼 办公室", callback_data="type:office")],\n''',
    'townhouse intake button',
)
text = text.replace('InlineKeyboardButton("✅ 立即发布", callback_data="preview:publish")', 'InlineKeyboardButton("✅ 保存到待审", callback_data="preview:publish")')
text = replace_once(
    text,
    '''    similar_url = deep_link(user_bot_username, payload("similar"))\n    clean_channel = str(channel_username or "").strip().lstrip("@")\n''',
    '''    similar_url = deep_link(user_bot_username, payload("similar"))\n    photos_url = deep_link(user_bot_username, f"photos_{listing_id}")\n    clean_channel = str(channel_username or "").strip().lstrip("@")\n''',
    'photo deep link',
)
text = replace_once(
    text,
    '''    else:\n        media_url = similar_url\n        log.warning(\n            "频道评论链接未配置，更多实拍按钮已安全降级到类似房源：listing_id=%s",\n            listing_id,\n        )\n''',
    '''    else:\n        media_url = photos_url\n        log.warning(\n            "频道评论链接未配置，更多实拍已降级到同房源 Bot 相册：listing_id=%s",\n            listing_id,\n        )\n''',
    'more photos fallback',
)
p.write_text(text, encoding='utf-8')

p = BRANCH_FILES['v2/qiaolian_publisher_v2/formatters.py']
text = p.read_text(encoding='utf-8')
text = replace_once(
    text,
    '    "villa": "别墅",\n    "shop": "商铺",\n',
    '    "villa": "别墅",\n    "townhouse": "排屋",\n    "shop": "商铺",\n',
    'townhouse type label',
)
p.write_text(text, encoding='utf-8')

# 5) Admin operator copy: never ask people to use internal DRF ids.
p = BRANCH_FILES['autopilot_publish_bot.py']
text = p.read_text(encoding='utf-8')
text = text.replace(
    '"快捷：<code>/pending</code> <code>/send DRF_xxx</code> <code>/slots 10:30,17:00,21:30</code>",',
    '"快捷：<code>/pending</code> <code>/send QC0001</code> <code>/slots 10:30,17:00,21:30</code>",',
)
p.write_text(text, encoding='utf-8')


# Permanent prelaunch contract tests.
Path('tests/test_prelaunch_product_contract.py').write_text(r'''import io
import sqlite3
from pathlib import Path

from PIL import Image

import discussion_map_store
from meihua_publisher import CHANNEL_ALBUM_SIZE, normalize_album_image
from publication_package import (
    _default_publish_layout_for_scope,
    _main_album_limit,
    classify,
)
from v2.qiaolian_publisher_v2.keyboards import preview_keyboard, publish_post_keyboard, type_keyboard


def _jpeg(size, left=(250, 20, 20), right=(20, 220, 20)):
    im = Image.new('RGB', size, 'white')
    for x in range(size[0]):
        color = left if x < size[0] // 2 else right
        for y in range(size[1]):
            im.putpixel((x, y), color)
    buf = io.BytesIO(); im.save(buf, 'JPEG', quality=98)
    return buf.getvalue()


def test_album_is_clean_white_4_3_and_keeps_both_sides():
    out = normalize_album_image(_jpeg((1800, 900)))
    with Image.open(io.BytesIO(out)).convert('RGB') as im:
        assert im.size == (1200, 900) == CHANNEL_ALBUM_SIZE
        # canvas corners stay white; the contained photo keeps both red and green sides.
        assert min(im.getpixel((4, 4))) > 235
        y = im.height // 2
        assert im.getpixel((30, y))[0] > im.getpixel((30, y))[1]
        assert im.getpixel((im.width - 31, y))[1] > im.getpixel((im.width - 31, y))[0]


def test_publish_routing_matches_product_contract():
    assert _default_publish_layout_for_scope('manual') == 'buttons'
    assert _default_publish_layout_for_scope('collected') == 'links'
    assert _main_album_limit(property_type='公寓', price=700, scope='collected', publish_layout='links') == 4
    assert _main_album_limit(property_type='排屋', price=1000, scope='collected', publish_layout='links') == 6
    assert _main_album_limit(property_type='公寓', price=1800, scope='collected', publish_layout='links') == 6
    assert _main_album_limit(property_type='别墅', price=5000, scope='manual', publish_layout='buttons') == 1
    routed = classify(source_type='telegram', source_name='collector', property_type='排屋', project='Vila Town', price=1200)
    assert routed['listing_type'] == 'townhouse'
    assert routed['cover_template'] == 'black_gold'


def test_admin_ui_is_truthful_and_more_photos_never_becomes_similar():
    preview_labels = [b.text for row in preview_keyboard().inline_keyboard for b in row]
    assert '✅ 保存到待审' in preview_labels
    assert all('立即发布' not in x for x in preview_labels)
    type_labels = [b.text for row in type_keyboard().inline_keyboard for b in row]
    assert '🏘 排屋' in type_labels
    kb = publish_post_keyboard('l_2', '永旺1', 'QiaolianBot')
    buttons = [b for row in kb.inline_keyboard for b in row]
    media = next(b for b in buttons if '更多实拍' in b.text)
    assert 'photos_l_2' in media.url
    assert 'similar' not in media.url


def test_discussion_map_accepts_old_posts_schema(tmp_path, monkeypatch):
    db_path = tmp_path / 'old.db'
    with sqlite3.connect(db_path) as conn:
        conn.execute('CREATE TABLE posts(channel_message_id TEXT, discuss_message_id TEXT)')
        conn.execute('INSERT INTO posts VALUES (?,?)', ('3054', '5140'))
    monkeypatch.setenv('DB_PATH', str(db_path))
    assert discussion_map_store._load_posts_sqlite() == {'3054': 5140}
''', encoding='utf-8')
