from __future__ import annotations

from qiaolian_v3.ingest.sanitizer import sanitize_source_text, strip_unicode_noise
from qiaolian_v3.ingest.source_service import SourceIngestService
from qiaolian_v3.media.source_media import SourceMedia
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
    assert [row['media_asset_key'] for row in links] == [item.content_hash for item in media]
    assert [row['sort_order'] for row in links] == [0, 1, 2]
