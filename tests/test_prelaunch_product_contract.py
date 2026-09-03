import io
import sqlite3
from pathlib import Path

from PIL import Image

import discussion_map_store
from meihua_publisher import CHANNEL_ALBUM_SIZE, normalize_album_image
from publication_package import classify
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
