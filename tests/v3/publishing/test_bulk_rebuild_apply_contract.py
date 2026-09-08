from __future__ import annotations

import json
import sqlite3

import pytest

from qiaolian_v3.db.migration_runner import MigrationRunner
from qiaolian_v3.publishing.package import BUTTON_TEXTS
from tools.migrations.rebuild_channel import (
    ApplyBlocked,
    RebuildCandidate,
    apply_preview,
    generate_preview,
)


CHANNEL_ID = '-100123'


def keyboard(ql: str):
    return tuple(
        (text, f'https://t.me/QiaoLianBot?start={action}_{ql}')
        for text, action in zip(BUTTON_TEXTS, ('details', 'photos', 'book'))
    )


class FakeMigrationGateway:
    def __init__(self, posts: dict[tuple[str, int], str]) -> None:
        self.posts = dict(posts)
        self.edits: list[dict] = []

    def inspect_migration_post(self, *, channel_id: str, message_id: int):
        return {'content_hash': self.posts[(str(channel_id), int(message_id))]}

    def edit_migration_post(self, **payload):
        key = (str(payload['channel_id']), int(payload['message_id']))
        assert self.posts[key] == payload['expected_before_hash']
        assert payload['cover']
        assert payload['caption']
        assert tuple(item['text'] for item in payload['keyboard']) == BUTTON_TEXTS
        self.posts[key] = str(payload['expected_after_hash'])
        self.edits.append(dict(payload))
        return {'ok': True, 'message_id': payload['message_id']}

    @property
    def write_count(self) -> int:
        return len(self.edits)


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    MigrationRunner(conn).migrate()
    return conn


def seed_package(
    conn: sqlite3.Connection,
    *,
    n: int,
    ql: str,
    package_id: str,
    after_hash: str,
    cover: str,
    caption: str,
) -> None:
    conn.execute(
        'INSERT INTO v3_sources(id,source_type,source_name,external_identity) VALUES (?,?,?,?)',
        (n, 'telegram', f'source-{n}', f'source-{n}'),
    )
    conn.execute(
        '''INSERT INTO v3_source_posts(
               id,source_id,source_identity_key,source_mode,source_type,source_name,external_post_id
           ) VALUES (?,?,?,?,?,?,?)''',
        (n, n, f'source-key-{n}', 'collector', 'telegram', f'source-{n}', str(1000 + n)),
    )
    conn.execute(
        '''INSERT INTO source_post_revisions(
               id,source_post_id,revision_no,source_content_hash,raw_text,sanitized_text
           ) VALUES (?,?,?,?,?,?)''',
        (n, n, 1, f'source-hash-{n}', 'rent', 'rent'),
    )
    conn.execute(
        '''INSERT INTO canonical_records(
               id,source_post_id,source_post_revision_id,schema_version,parser_revision,
               facts_json,facts_hash,deal_type
           ) VALUES (?,?,?,?,?,?,?,?)''',
        (n, n, n, 'canonical_facts.v3', 'phase11-test', json.dumps({'deal_type': 'rent'}), f'canon-{n}', 'rent'),
    )
    conn.execute(
        '''INSERT INTO v3_listings(
               id,public_listing_id,current_canonical_record_id,property_identity_key,listing_status
           ) VALUES (?,?,?,?,?)''',
        (n, ql, n, f'property-{n}', 'active'),
    )
    conn.execute(
        '''INSERT INTO listing_offers(
               id,listing_id,canonical_record_id,offer_type,monthly_rent_usd,publication_policy
           ) VALUES (?,?,?,'rent',800,'telegram_rent')''',
        (n, n, n),
    )
    kb = [{'text': text, 'url': url} for text, url in keyboard(ql)]
    conn.execute(
        '''INSERT INTO v3_publication_packages(
               id,package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,
               target_channel_id,status,approval_mode,cover_path,gallery_json,caption_html,
               keyboard_json,content_hash,canonical_hash
           ) VALUES (?,?,?,?,?,?,1,?,'frozen','admin',?,'[]',?,?,?,?)''',
        (n, package_id, package_id, n, n, n, CHANNEL_ID, cover, caption, json.dumps(kb), after_hash, f'canon-{n}'),
    )
    conn.commit()


def candidate(
    *, n: int, ql: str, package_id: str, before_hash: str, after_hash: str, cover: str, caption: str,
) -> RebuildCandidate:
    return RebuildCandidate(
        listing_id=n,
        public_listing_id=ql,
        offer_id=n,
        canonical_record_id=n,
        source_identity=f'source-{n}|{1000+n}|source-key-{n}',
        deal_type='rent',
        offer_type='rent',
        publication_policy='telegram_rent',
        quality_result='ADMIN_APPROVED',
        frozen=True,
        package_id=package_id,
        package_content_hash=after_hash,
        channel_id=CHANNEL_ID,
        message_id=5000 + n,
        current_content_hash=before_hash,
        cover=cover,
        caption=caption,
        buttons=keyboard(ql),
    )


def seeded_batch():
    conn = db()
    specs = (
        (1, 'QL-RF-A2B3', 'PKG_PHASE11_A', 'before-a', 'after-a', 'cover-a.svg', 'caption-a'),
        (2, 'QL-BK-C3D4', 'PKG_PHASE11_B', 'before-b', 'after-b', 'cover-b.svg', 'caption-b'),
    )
    candidates = []
    posts = {}
    for n, ql, package_id, before_hash, after_hash, cover, caption in specs:
        seed_package(
            conn, n=n, ql=ql, package_id=package_id, after_hash=after_hash,
            cover=cover, caption=caption,
        )
        item = candidate(
            n=n, ql=ql, package_id=package_id, before_hash=before_hash,
            after_hash=after_hash, cover=cover, caption=caption,
        )
        candidates.append(item)
        posts[(CHANNEL_ID, item.message_id)] = before_hash
    return conn, generate_preview(candidates), FakeMigrationGateway(posts)


def test_phase11_requires_exact_manually_approved_preview_hash_before_any_write():
    conn, preview, gateway = seeded_batch()
    with pytest.raises(ApplyBlocked, match='explicit_approved_preview_hash_required'):
        apply_preview(conn, preview, gateway=gateway, approved_preview_hash='wrong')
    assert gateway.write_count == 0
    assert conn.execute('SELECT COUNT(*) FROM v3_channel_posts').fetchone()[0] == 0


def test_before_hash_conflict_is_global_fail_fast_with_zero_writes_and_zero_mapping_mutations():
    conn, preview, gateway = seeded_batch()
    gateway.posts[(CHANNEL_ID, 5002)] = 'unexpected-live-hash'
    with pytest.raises(ApplyBlocked, match='before_hash_conflict'):
        apply_preview(conn, preview, gateway=gateway, approved_preview_hash=preview.preview_hash)
    assert gateway.write_count == 0
    assert conn.execute('SELECT COUNT(*) FROM v3_channel_posts').fetchone()[0] == 0
    assert [row[0] for row in conn.execute('SELECT status FROM v3_publication_packages ORDER BY id')] == ['frozen', 'frozen']


def test_successful_apply_exact_edits_each_slot_and_commits_new_mapping_receipts():
    conn, preview, gateway = seeded_batch()
    result = apply_preview(conn, preview, gateway=gateway, approved_preview_hash=preview.preview_hash)

    assert result.telegram_writes == 2
    assert result.mapping_mutations == 2
    assert [item.action for item in result.checkpoints] == ['edited', 'edited']
    assert gateway.write_count == 2
    assert {(call['channel_id'], call['message_id']) for call in gateway.edits} == {(CHANNEL_ID, 5001), (CHANNEL_ID, 5002)}
    assert all(tuple(item['text'] for item in call['keyboard']) == BUTTON_TEXTS for call in gateway.edits)
    assert {call['cover'] for call in gateway.edits} == {'cover-a.svg', 'cover-b.svg'}

    rows = conn.execute(
        '''SELECT channel_id,message_id,listing_id,offer_id,content_hash,post_status
           FROM v3_channel_posts ORDER BY message_id'''
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (CHANNEL_ID, 5001, 1, 1, 'after-a', 'migration_applied'),
        (CHANNEL_ID, 5002, 2, 2, 'after-b', 'migration_applied'),
    ]
    assert [row[0] for row in conn.execute('SELECT status FROM v3_publication_packages ORDER BY id')] == ['published', 'published']


def test_crash_after_telegram_edit_before_db_commit_recovers_without_second_edit():
    conn, preview, gateway = seeded_batch()
    # Simulate Telegram edit having succeeded while the process died before the
    # v3_channel_posts/package transaction was committed.
    gateway.posts[(CHANNEL_ID, 5001)] = 'after-a'

    result = apply_preview(conn, preview, gateway=gateway, approved_preview_hash=preview.preview_hash)
    assert result.telegram_writes == 1
    assert [item.action for item in result.checkpoints] == ['recovered', 'edited']
    assert gateway.write_count == 1
    assert gateway.edits[0]['message_id'] == 5002
    assert conn.execute('SELECT COUNT(*) FROM v3_channel_posts').fetchone()[0] == 2


def test_committed_retry_is_idempotent_and_never_edits_again():
    conn, preview, gateway = seeded_batch()
    first = apply_preview(conn, preview, gateway=gateway, approved_preview_hash=preview.preview_hash)
    assert first.telegram_writes == 2
    prior_writes = gateway.write_count

    second = apply_preview(conn, preview, gateway=gateway, approved_preview_hash=preview.preview_hash)
    assert second.telegram_writes == 0
    assert [item.action for item in second.checkpoints] == ['recovered', 'recovered']
    assert gateway.write_count == prior_writes
    assert conn.execute('SELECT COUNT(*) FROM v3_channel_posts').fetchone()[0] == 2


def test_apply_never_sends_or_creates_new_message_targets():
    conn, preview, gateway = seeded_batch()
    apply_preview(conn, preview, gateway=gateway, approved_preview_hash=preview.preview_hash)
    assert all('message_id' in call and call['message_id'] in {5001, 5002} for call in gateway.edits)
    assert not hasattr(gateway, 'send')
