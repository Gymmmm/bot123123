from __future__ import annotations

import hashlib
import json
from pathlib import Path

from qiaolian_v3.listing.public_id import normalize_public_id
from qiaolian_v3.publishing.package import BUTTON_TEXTS


ARTIFACT = Path('artifacts/v3/workpack_c/channel_rebuild_preview.json')
OLD_SYNTHETIC_HASH = 'c2beed370a354ba09ab9a62a35633556e1290be4928970b73acc2ae275480172'


def _stable_hash(payload: object) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def test_committed_phase10_acceptance_artifact_is_real_derived_formal_and_self_consistent():
    data = json.loads(ARTIFACT.read_text(encoding='utf-8'))
    assert data['preview_hash'] != OLD_SYNTHETIC_HASH
    assert data['telegram_writes'] == 0
    assert data['publication_status_mutations'] == 0
    assert len(data['manifest']) == 2

    for record in data['manifest']:
        assert 'production-derived' in record['source_identity']
        assert record['canonical_record_id'] > 0
        assert record['listing_id'] > 0 and record['offer_id'] > 0
        assert normalize_public_id(record['public_ql_id']) == record['public_ql_id']
        assert record['frozen_package_id'] == 'PKG_' + record['after_content_hash'][:20].upper()
        assert Path(record['cover']).is_file()
        assert tuple(button['text'] for button in record['buttons']) == BUTTON_TEXTS
        assert all(f"_{record['public_ql_id']}" in button['url'] for button in record['buttons'])
        assert record['channel_id'] and record['message_id'] > 0
        assert len(record['before_content_hash']) == len(record['after_content_hash']) == 64

    reasons = [item['reason'] for item in data['failures']]
    assert reasons.count('sale_target_blocked') == 1
    assert reasons.count('unknown_target_blocked') == 1
    assert reasons.count('unfrozen_package_blocked') == 1
    assert data['target_set'] == [f'-100123:{message_id}' for message_id in range(5001, 5006)]

    payload = {
        'manifest': [
            {**record, 'buttons': [[item['text'], item['url']] for item in record['buttons']]}
            for record in data['manifest']
        ],
        'failures': data['failures'],
        'target_set': data['target_set'],
    }
    assert _stable_hash(payload) == data['preview_hash']


def test_committed_artifact_contains_no_old_numeric_ql_ids():
    text = ARTIFACT.read_text(encoding='utf-8')
    assert 'QL000' not in text
