from __future__ import annotations

import ast
from pathlib import Path

from qiaolian_v3.ingest.source_service import IngestDisposition, SourceIngestService
from qiaolian_v3.ingest.telegram_collector import TelegramCollector
from qiaolian_v3.media.source_media import SourceMedia
from tests.v3.db._helpers import migrated_connection


ROOT = Path(__file__).resolve().parents[3]


def _photos(count: int):
    return [
        SourceMedia.from_bytes(f'photo-{idx}'.encode(), sort_order=idx, message_id=100 + idx)
        for idx in range(count)
    ]


def test_album_collection_uses_grouped_identity_and_preserves_ordered_media():
    conn = migrated_connection()
    collector = TelegramCollector(SourceIngestService(conn))
    result = collector.collect_album(
        source_name='fixture', source_external_identity='-100123', grouped_id=777,
        anchor_message_id=100, raw_text='租金 $800/月', media=_photos(4),
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:01:00+00:00',
    )
    assert result.disposition is IngestDisposition.NEW_SOURCE_POST
    post = conn.execute('SELECT external_post_id FROM v3_source_posts WHERE id=?', (result.source_post_id,)).fetchone()
    assert post['external_post_id'] == 'album_777'
    assert conn.execute(
        'SELECT COUNT(*) FROM source_post_media WHERE source_post_revision_id=?',
        (result.revision_id,),
    ).fetchone()[0] == 4


def test_album_same_grouped_id_changed_text_becomes_source_update():
    conn = migrated_connection()
    collector = TelegramCollector(SourceIngestService(conn))
    first = collector.collect_album(
        source_name='fixture', source_external_identity='-100123', grouped_id=777,
        anchor_message_id=100, raw_text='租金 $800/月', media=_photos(4),
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:01:00+00:00',
    )
    updated = collector.collect_album(
        source_name='fixture', source_external_identity='-100123', grouped_id=777,
        anchor_message_id=101, raw_text='租金 $850/月', media=_photos(4),
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:05:00+00:00',
    )
    assert first.source_post_id == updated.source_post_id
    assert updated.disposition is IngestDisposition.SOURCE_UPDATED
    assert updated.revision_no == 2


def test_single_collection_keeps_message_identity():
    conn = migrated_connection()
    collector = TelegramCollector(SourceIngestService(conn))
    result = collector.collect_single(
        source_name='fixture', source_external_identity='-100123', message_id=321,
        raw_text='租金 $800/月', media=_photos(1),
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:01:00+00:00',
    )
    post = conn.execute('SELECT external_post_id FROM v3_source_posts WHERE id=?', (result.source_post_id,)).fetchone()
    assert post['external_post_id'] == '321'


def test_ingest_modules_have_no_publication_publisher_or_package_build_imports():
    ingest_root = ROOT / 'qiaolian_v3' / 'ingest'
    py_files = sorted(ingest_root.glob('*.py'))
    assert py_files
    for path in py_files:
        text = path.read_text(encoding='utf-8')
        tree = ast.parse(text)
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or '')
        assert not any(name.startswith('qiaolian_v3.publishing') for name in imported), path
        assert not any(name in {'publication_package', 'publication_delivery', 'meihua_publisher'} for name in imported), path
        assert 'build_package(' not in text, path
