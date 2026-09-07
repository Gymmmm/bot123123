from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from qiaolian_v3.ingest.sanitizer import sanitize_source_text, strip_unicode_noise
from qiaolian_v3.ingest.source_service import SourceIngestService
from qiaolian_v3.media.source_media import SourceMedia, TelegramSourceMedia
from tests.v3.db._helpers import migrated_connection


def test_sanitizer_preserves_fact_prefix_and_removes_contacts_and_promo():
    raw = '租金 $800/月 联系微信 abc12345\n面积 95㎡\n欢迎关注更多房源\nhttps://t.me/example'
    cleaned = sanitize_source_text(raw)
    assert cleaned.text == '租金 $800/月\n面积 95㎡'
    assert 'https://t.me/example' in cleaned.contacts
    assert all('微信' not in line for line in cleaned.text.splitlines())
    assert any('欢迎关注' in line for line in cleaned.removed_lines)


def test_strip_unicode_noise_removes_private_use_formatting():
    assert strip_unicode_noise('A\uf003B') == 'AB'


def test_media_hash_is_content_based_and_stable(tmp_path):
    a = SourceMedia.from_bytes(b'same-bytes', sort_order=0, message_id=1)
    b = SourceMedia.from_bytes(b'same-bytes', sort_order=1, message_id=2)
    c = SourceMedia.from_bytes(b'different', sort_order=0, message_id=3)
    path = tmp_path / 'same.jpg'
    path.write_bytes(b'same-bytes')
    from_file = SourceMedia.from_file(path, sort_order=0, message_id=4)

    assert a.content_hash == b.content_hash == from_file.content_hash
    assert a.content_hash != c.content_hash
    assert len(a.content_hash) == 64
    assert a.media_identity == a.content_hash


@pytest.mark.asyncio
async def test_telegram_media_download_prefers_file_hash_and_keeps_evidence(tmp_path):
    downloaded = tmp_path / 'photo.jpg'

    class FakeClient:
        async def download_media(self, media, file):
            assert media is message.media
            assert Path(file) == tmp_path
            downloaded.write_bytes(b'telegram-photo-bytes')
            return str(downloaded)

    message = SimpleNamespace(
        id=321,
        media=SimpleNamespace(
            photo=SimpleNamespace(id=987, access_hash=654),
            document=None,
        ),
    )

    item = await TelegramSourceMedia.download(
        FakeClient(), message, download_dir=tmp_path, sort_order=2, media_type='photo'
    )

    assert item.local_path == str(downloaded.resolve())
    assert item.telegram_file_id == '987'
    assert item.telegram_file_unique_id == '654'
    assert item.message_id == 321
    assert item.sort_order == 2
    assert item.content_hash
    assert item.media_identity == item.content_hash


@pytest.mark.asyncio
async def test_telegram_media_download_falls_back_to_unique_identity_without_file_hash(tmp_path):
    class FakeClient:
        async def download_media(self, media, file):
            return None

    message = SimpleNamespace(
        id=400,
        media=SimpleNamespace(
            photo=SimpleNamespace(id=111, access_hash=222),
            document=None,
        ),
    )

    item = await TelegramSourceMedia.download(
        FakeClient(), message, download_dir=tmp_path, sort_order=0, media_type='photo'
    )

    assert item.content_hash == ''
    assert item.telegram_file_unique_id == '222'
    assert item.media_identity == 'telegram_unique:222'


def test_less_than_four_photos_are_preserved_as_revision_media():
    conn = migrated_connection()
    media = [
        SourceMedia.from_bytes(f'photo-{idx}'.encode(), sort_order=idx, message_id=10 + idx)
        for idx in range(3)
    ]
    result = SourceIngestService(conn, min_listing_images=4).ingest_telegram(
        source_name='fixture', source_external_identity='-100123', external_post_id='100',
        raw_text='租金 $800/月', media=media,
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:01:00+00:00',
    )

    assert result.insufficient_media is True
    assert result.media_count == 3
    assert conn.execute('SELECT COUNT(*) FROM source_post_revisions WHERE id=?', (result.revision_id,)).fetchone()[0] == 1
    links = conn.execute(
        'SELECT media_asset_key,sort_order FROM source_post_media WHERE source_post_revision_id=? ORDER BY sort_order',
        (result.revision_id,),
    ).fetchall()
    assert [row['media_asset_key'] for row in links] == [item.media_identity for item in media]
    assert [row['sort_order'] for row in links] == [0, 1, 2]
