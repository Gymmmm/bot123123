from __future__ import annotations

from qiaolian_v3.ingest.source_identity import (
    build_album_external_post_id,
    build_single_external_post_id,
    make_source_content_hash,
)
from qiaolian_v3.ingest.source_service import IngestDisposition, SourceIngestService
from qiaolian_v3.media.source_media import SourceMedia
from tests.v3.db._helpers import migrated_connection


def _media(seed: bytes, *, order: int, message_id: int) -> SourceMedia:
    return SourceMedia.from_bytes(
        seed,
        sort_order=order,
        message_id=message_id,
        telegram_file_id=f'f-{message_id}',
    )


def test_single_and_album_external_identity_contract():
    assert build_single_external_post_id(123) == '123'
    assert build_album_external_post_id(grouped_id=987654, anchor_message_id=100) == 'album_987654'
    assert build_album_external_post_id(grouped_id=None, anchor_message_id=100) == 'album_100'


def test_source_content_hash_preserves_normalized_ordered_media_identity():
    a = make_source_content_hash('租金 $800/月', ['b' * 64, 'a' * 64])
    b = make_source_content_hash('租金 $800/月', ['a' * 64, 'b' * 64])
    assert a != b
    assert a != make_source_content_hash('租金 $850/月', ['b' * 64, 'a' * 64])
    assert a != make_source_content_hash('租金 $800/月', ['b' * 64, 'c' * 64])
    assert make_source_content_hash('x', ['a', 'a']) != make_source_content_hash('x', ['a'])


def test_same_identity_same_hash_is_duplicate_ignore_and_only_moves_last_seen():
    conn = migrated_connection()
    service = SourceIngestService(conn)
    media = [_media(b'photo-1', order=0, message_id=10)]

    first = service.ingest_telegram(
        source_name='fixture',
        source_external_identity='-100123',
        external_post_id='100',
        raw_text='租金 $800/月\n微信 abc12345',
        media=media,
        source_created_at='2026-09-01T10:00:00+00:00',
        fetched_at='2026-09-01T10:01:00+00:00',
    )
    duplicate = service.ingest_telegram(
        source_name='fixture',
        source_external_identity='-100123',
        external_post_id='100',
        raw_text='租金 $800/月\n微信 changed999',
        media=media,
        source_created_at='2026-09-01T10:00:00+00:00',
        fetched_at='2026-09-01T10:05:00+00:00',
    )

    assert first.disposition is IngestDisposition.NEW_SOURCE_POST
    assert duplicate.disposition is IngestDisposition.DUPLICATE_IGNORE
    assert duplicate.revision_id == first.revision_id
    assert conn.execute(
        'SELECT COUNT(*) FROM source_post_revisions WHERE source_post_id=?',
        (first.source_post_id,),
    ).fetchone()[0] == 1
    post = conn.execute('SELECT first_seen_at,last_seen_at FROM v3_source_posts WHERE id=?', (first.source_post_id,)).fetchone()
    assert post['first_seen_at'] == '2026-09-01T10:01:00+00:00'
    assert post['last_seen_at'] == '2026-09-01T10:05:00+00:00'


def test_same_identity_changed_hash_creates_new_revision_not_duplicate_skip():
    conn = migrated_connection()
    service = SourceIngestService(conn)
    media = [_media(b'photo-1', order=0, message_id=10)]

    first = service.ingest_telegram(
        source_name='fixture', source_external_identity='-100123', external_post_id='100',
        raw_text='租金 $800/月', media=media,
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:01:00+00:00',
    )
    updated = service.ingest_telegram(
        source_name='fixture', source_external_identity='-100123', external_post_id='100',
        raw_text='租金 $850/月', media=media,
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:06:00+00:00',
    )

    assert first.disposition is IngestDisposition.NEW_SOURCE_POST
    assert updated.disposition is IngestDisposition.SOURCE_UPDATED
    assert (first.revision_no, updated.revision_no) == (1, 2)
    assert updated.revision_id != first.revision_id
    post = conn.execute('SELECT current_revision_id FROM v3_source_posts WHERE id=?', (first.source_post_id,)).fetchone()
    assert post['current_revision_id'] == updated.revision_id


def test_same_text_same_media_different_order_creates_revision():
    conn = migrated_connection()
    service = SourceIngestService(conn)
    photo_a = _media(b'photo-a', order=0, message_id=10)
    photo_b = _media(b'photo-b', order=1, message_id=11)

    first = service.ingest_telegram(
        source_name='fixture', source_external_identity='-100123', external_post_id='album_77',
        raw_text='租金 $800/月', media=[photo_a, photo_b],
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:01:00+00:00',
    )
    reordered = service.ingest_telegram(
        source_name='fixture', source_external_identity='-100123', external_post_id='album_77',
        raw_text='租金 $800/月',
        media=[
            _media(b'photo-b', order=0, message_id=11),
            _media(b'photo-a', order=1, message_id=10),
        ],
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:06:00+00:00',
    )

    assert first.source_content_hash != reordered.source_content_hash
    assert reordered.disposition is IngestDisposition.SOURCE_UPDATED
    assert reordered.revision_no == 2
    assert reordered.revision_id != first.revision_id


def test_revision_preserves_source_created_and_fetched_timestamps():
    conn = migrated_connection()
    result = SourceIngestService(conn).ingest_telegram(
        source_name='fixture', source_external_identity='-100123', external_post_id='100',
        raw_text='租金 $800/月', media=[_media(b'p', order=0, message_id=10)],
        source_created_at='2026-09-01T09:59:00+00:00', fetched_at='2026-09-01T10:01:00+00:00',
    )
    revision = conn.execute('SELECT * FROM source_post_revisions WHERE id=?', (result.revision_id,)).fetchone()
    assert revision['source_created_at'] == '2026-09-01T09:59:00+00:00'
    assert revision['fetched_at'] == '2026-09-01T10:01:00+00:00'
