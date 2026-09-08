from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from qiaolian_v3.listing.public_id import normalize_public_id
from qiaolian_v3.publishing.package import BUTTON_TEXTS


class PreviewInvalid(RuntimeError):
    pass


@dataclass(frozen=True)
class RebuildCandidate:
    listing_id: int
    public_listing_id: str
    offer_id: int
    canonical_record_id: int
    source_identity: str
    deal_type: str
    offer_type: str
    publication_policy: str
    quality_result: str
    frozen: bool
    package_id: str
    package_content_hash: str
    channel_id: str
    message_id: int
    current_content_hash: str
    cover: str
    caption: str
    buttons: tuple[tuple[str, str], ...]

    @property
    def target_key(self) -> str:
        return f'{self.channel_id}:{self.message_id}'


@dataclass(frozen=True)
class ManifestRecord:
    target_key: str
    source_identity: str
    canonical_record_id: int
    listing_id: int
    public_ql_id: str
    offer_id: int
    channel_id: str
    message_id: int
    before_content_hash: str
    after_content_hash: str
    frozen_package_id: str
    cover: str
    caption: str
    buttons: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class PreviewFailure:
    target_key: str
    source_identity: str
    canonical_record_id: int
    listing_id: int
    public_ql_id: str
    reason: str


@dataclass(frozen=True)
class RebuildPreview:
    manifest: tuple[ManifestRecord, ...]
    failures: tuple[PreviewFailure, ...]
    target_set: tuple[str, ...]
    preview_hash: str
    telegram_writes: int = 0
    publication_status_mutations: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            'manifest': [
                {**asdict(record), 'buttons': [{'text': text, 'url': url} for text, url in record.buttons]}
                for record in self.manifest
            ],
            'failures': [asdict(failure) for failure in self.failures],
            'target_set': list(self.target_set),
            'preview_hash': self.preview_hash,
            'telegram_writes': self.telegram_writes,
            'publication_status_mutations': self.publication_status_mutations,
        }


def _stable_hash(payload: object) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _failure_reason(candidate: RebuildCandidate) -> str | None:
    if candidate.deal_type == 'sale':
        return 'sale_target_blocked'
    if candidate.deal_type != 'rent':
        return 'unknown_target_blocked'
    if normalize_public_id(candidate.public_listing_id) is None:
        return 'invalid_public_ql_id'
    if candidate.offer_type != 'rent':
        return 'non_rent_offer_blocked'
    if candidate.publication_policy != 'telegram_rent':
        return 'non_rent_publication_policy_blocked'
    if candidate.quality_result in {'NEEDS_REVIEW', 'REJECT', '', 'None'}:
        return 'quality_not_approved'
    if not candidate.frozen:
        return 'unfrozen_package_blocked'
    if not candidate.package_id or not candidate.package_content_hash:
        return 'missing_frozen_package_identity'
    if not candidate.channel_id or int(candidate.message_id or 0) <= 0:
        return 'explicit_message_mapping_required'
    if not candidate.current_content_hash:
        return 'missing_current_content_hash'
    if not candidate.cover or not candidate.caption:
        return 'incomplete_preview_content'
    if len(candidate.buttons) != 3 or tuple(text for text, _ in candidate.buttons) != BUTTON_TEXTS:
        return 'invalid_three_button_contract'
    return None


def generate_preview(candidates: Iterable[RebuildCandidate]) -> RebuildPreview:
    ordered = tuple(sorted(candidates, key=lambda item: (item.channel_id, item.message_id, item.listing_id, item.package_id)))
    target_set = tuple(item.target_key for item in ordered)
    if len(set(target_set)) != len(target_set):
        raise PreviewInvalid('PREVIEW_INVALID:duplicate_target_slot')

    manifest: list[ManifestRecord] = []
    failures: list[PreviewFailure] = []
    for item in ordered:
        reason = _failure_reason(item)
        normalized = normalize_public_id(item.public_listing_id)
        public_id = normalized or str(item.public_listing_id or '')
        if reason is not None:
            failures.append(PreviewFailure(
                item.target_key, item.source_identity, item.canonical_record_id,
                item.listing_id, public_id, reason,
            ))
            continue
        manifest.append(ManifestRecord(
            target_key=item.target_key,
            source_identity=item.source_identity,
            canonical_record_id=item.canonical_record_id,
            listing_id=item.listing_id,
            public_ql_id=public_id,
            offer_id=item.offer_id,
            channel_id=item.channel_id,
            message_id=item.message_id,
            before_content_hash=item.current_content_hash,
            after_content_hash=item.package_content_hash,
            frozen_package_id=item.package_id,
            cover=item.cover,
            caption=item.caption,
            buttons=item.buttons,
        ))

    payload = {
        'manifest': [{**asdict(record), 'buttons': list(record.buttons)} for record in manifest],
        'failures': [asdict(failure) for failure in failures],
        'target_set': list(target_set),
    }
    return RebuildPreview(
        manifest=tuple(manifest), failures=tuple(failures), target_set=target_set,
        preview_hash=_stable_hash(payload),
    )


def validate_preview(preview: RebuildPreview, current_candidates: Iterable[RebuildCandidate]) -> None:
    current = generate_preview(current_candidates)
    if current.target_set != preview.target_set or current.preview_hash != preview.preview_hash:
        raise PreviewInvalid('PREVIEW_INVALID:data_or_target_drift')


def _normalize_keyboard(value: object) -> tuple[tuple[str, str], ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return ()
    if isinstance(value, dict):
        value = value.get('buttons') or value.get('inline_keyboard') or []
    result: list[tuple[str, str]] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, list):
                for nested in item:
                    if isinstance(nested, dict) and nested.get('text') and nested.get('url'):
                        result.append((str(nested['text']), str(nested['url'])))
            elif isinstance(item, dict) and item.get('text') and item.get('url'):
                result.append((str(item['text']), str(item['url'])))
    return tuple(result)


def _source_identity(row: sqlite3.Row) -> str:
    return '|'.join((
        str(row['source_name'] or ''), str(row['external_post_id'] or ''),
        str(row['source_identity_key'] or ''),
    ))


def load_candidates_from_db(
    conn: sqlite3.Connection,
    slot_mappings: Iterable[Mapping[str, object]],
) -> tuple[RebuildCandidate, ...]:
    """Read-only official Phase-10 loader. It never changes publication state."""
    candidates: list[RebuildCandidate] = []
    for mapping in slot_mappings:
        package_id = str(mapping.get('package_id') or '')
        canonical_record_id = int(mapping.get('canonical_record_id') or 0)
        if package_id:
            row = conn.execute(
                '''SELECT p.*,l.public_listing_id,o.offer_type,o.publication_policy,c.deal_type,
                          c.id AS canonical_id,sp.source_name,sp.external_post_id,sp.source_identity_key
                   FROM v3_publication_packages p
                   JOIN v3_listings l ON l.id=p.listing_id
                   JOIN listing_offers o ON o.id=p.offer_id
                   JOIN canonical_records c ON c.id=p.canonical_record_id
                   JOIN v3_source_posts sp ON sp.id=c.source_post_id
                   WHERE p.package_id=?''',
                (package_id,),
            ).fetchone()
            if row is None:
                raise PreviewInvalid(f'PREVIEW_INVALID:package_not_found:{package_id}')
            channel_id = str(mapping.get('channel_id') or '')
            if channel_id != str(row['target_channel_id']):
                raise PreviewInvalid(f'PREVIEW_INVALID:channel_mapping_mismatch:{package_id}')
            public_id = str(row['public_listing_id'] or '')
            if str(row['deal_type']) == 'rent' and normalize_public_id(public_id) is None:
                raise PreviewInvalid(f'PREVIEW_INVALID:invalid_public_ql_id:{package_id}')
            candidates.append(RebuildCandidate(
                listing_id=int(row['listing_id']), public_listing_id=public_id,
                offer_id=int(row['offer_id']), canonical_record_id=int(row['canonical_id']),
                source_identity=_source_identity(row), deal_type=str(row['deal_type']),
                offer_type=str(row['offer_type']), publication_policy=str(row['publication_policy']),
                quality_result='AUTO_PUBLISH' if str(row['approval_mode']) == 'auto' else 'ADMIN_APPROVED',
                frozen=str(row['status']) == 'frozen', package_id=str(row['package_id']),
                package_content_hash=str(row['content_hash']), channel_id=channel_id,
                message_id=int(mapping.get('message_id') or 0),
                current_content_hash=str(mapping.get('current_content_hash') or ''),
                cover=str(row['cover_path']), caption=str(row['caption_html']),
                buttons=_normalize_keyboard(row['keyboard_json']),
            ))
            continue

        if canonical_record_id <= 0:
            raise PreviewInvalid('PREVIEW_INVALID:package_or_canonical_required')
        row = conn.execute(
            '''SELECT c.id AS canonical_id,c.deal_type,sp.source_name,sp.external_post_id,sp.source_identity_key,
                      l.id AS listing_id,l.public_listing_id,o.id AS offer_id,o.offer_type,o.publication_policy
               FROM canonical_records c
               JOIN v3_source_posts sp ON sp.id=c.source_post_id
               LEFT JOIN v3_listings l ON l.current_canonical_record_id=c.id
               LEFT JOIN listing_offers o ON o.canonical_record_id=c.id AND o.is_current=1
               WHERE c.id=?''',
            (canonical_record_id,),
        ).fetchone()
        if row is None:
            raise PreviewInvalid(f'PREVIEW_INVALID:canonical_not_found:{canonical_record_id}')
        candidates.append(RebuildCandidate(
            listing_id=int(row['listing_id'] or 0), public_listing_id=str(row['public_listing_id'] or ''),
            offer_id=int(row['offer_id'] or 0), canonical_record_id=int(row['canonical_id']),
            source_identity=_source_identity(row), deal_type=str(row['deal_type']),
            offer_type=str(row['offer_type'] or ''), publication_policy=str(row['publication_policy'] or 'store_only'),
            quality_result='NEEDS_REVIEW', frozen=False, package_id='', package_content_hash='',
            channel_id=str(mapping.get('channel_id') or ''), message_id=int(mapping.get('message_id') or 0),
            current_content_hash=str(mapping.get('current_content_hash') or ''), cover='', caption='', buttons=(),
        ))
    return tuple(candidates)


def _candidate_from_json(value: Mapping[str, object]) -> RebuildCandidate:
    """Unit-test helper only; the official CLI never accepts generic candidates."""
    buttons = tuple((str(item['text']), str(item['url'])) for item in value.get('buttons', []) if isinstance(item, dict))
    return RebuildCandidate(
        listing_id=int(value['listing_id']), public_listing_id=str(value['public_listing_id']),
        offer_id=int(value['offer_id']), canonical_record_id=int(value.get('canonical_record_id') or 0),
        source_identity=str(value.get('source_identity') or 'unit-test'),
        deal_type=str(value['deal_type']), offer_type=str(value['offer_type']),
        publication_policy=str(value['publication_policy']), quality_result=str(value['quality_result']),
        frozen=bool(value['frozen']), package_id=str(value['package_id']),
        package_content_hash=str(value['package_content_hash']), channel_id=str(value['channel_id']),
        message_id=int(value['message_id']), current_content_hash=str(value['current_content_hash']),
        cover=str(value['cover']), caption=str(value['caption']), buttons=buttons,
    )


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if '--apply' in argv:
        raise SystemExit('PHASE_11_NOT_STARTED:apply_is_blocked')
    parser = argparse.ArgumentParser(description='V3 Phase 10 channel rebuild PREVIEW ONLY')
    parser.add_argument('--db', required=True, help='SQLite DB containing V3 frozen packages')
    parser.add_argument('--slots', required=True, help='JSON file containing explicit TEST slot mappings')
    parser.add_argument('--output', required=True, help='Destination JSON preview manifest')
    args = parser.parse_args(argv)
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    mappings = json.loads(Path(args.slots).read_text(encoding='utf-8'))
    preview = generate_preview(load_candidates_from_db(conn, mappings))
    Path(args.output).write_text(json.dumps(preview.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + '\n', encoding='utf-8')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


__all__ = [
    'ManifestRecord', 'PreviewFailure', 'PreviewInvalid', 'RebuildCandidate', 'RebuildPreview',
    'generate_preview', 'load_candidates_from_db', 'main', 'validate_preview',
]
