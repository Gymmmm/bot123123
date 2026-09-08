from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from qiaolian_v3.listing.public_id import normalize_public_id
from qiaolian_v3.publishing.package import BUTTON_TEXTS, FrozenPublicationPackage


class PreviewInvalid(RuntimeError):
    pass


class ApplyBlocked(RuntimeError):
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


@dataclass(frozen=True)
class ApplyCheckpoint:
    target_key: str
    action: str
    package_id: str
    listing_id: int
    offer_id: int
    channel_id: str
    message_id: int
    before_content_hash: str
    after_content_hash: str


@dataclass(frozen=True)
class ApplyResult:
    preview_hash: str
    checkpoints: tuple[ApplyCheckpoint, ...]
    failures: tuple[str, ...]
    telegram_writes: int
    mapping_mutations: int

    def to_dict(self) -> dict[str, Any]:
        return {
            'preview_hash': self.preview_hash,
            'checkpoints': [asdict(item) for item in self.checkpoints],
            'failures': list(self.failures),
            'telegram_writes': self.telegram_writes,
            'mapping_mutations': self.mapping_mutations,
        }


def _stable_hash(payload: object) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _preview_payload(preview: RebuildPreview) -> dict[str, Any]:
    return {
        'manifest': [{**asdict(record), 'buttons': list(record.buttons)} for record in preview.manifest],
        'failures': [asdict(failure) for failure in preview.failures],
        'target_set': list(preview.target_set),
    }


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


def _preview_from_dict(value: Mapping[str, Any]) -> RebuildPreview:
    manifest = tuple(
        ManifestRecord(
            target_key=str(item['target_key']), source_identity=str(item['source_identity']),
            canonical_record_id=int(item['canonical_record_id']), listing_id=int(item['listing_id']),
            public_ql_id=str(item['public_ql_id']), offer_id=int(item['offer_id']),
            channel_id=str(item['channel_id']), message_id=int(item['message_id']),
            before_content_hash=str(item['before_content_hash']), after_content_hash=str(item['after_content_hash']),
            frozen_package_id=str(item['frozen_package_id']), cover=str(item['cover']), caption=str(item['caption']),
            buttons=tuple((str(button['text']), str(button['url'])) for button in item.get('buttons', [])),
        )
        for item in value.get('manifest', [])
    )
    failures = tuple(
        PreviewFailure(
            target_key=str(item['target_key']), source_identity=str(item['source_identity']),
            canonical_record_id=int(item['canonical_record_id']), listing_id=int(item['listing_id']),
            public_ql_id=str(item['public_ql_id']), reason=str(item['reason']),
        )
        for item in value.get('failures', [])
    )
    preview = RebuildPreview(
        manifest=manifest,
        failures=failures,
        target_set=tuple(str(item) for item in value.get('target_set', [])),
        preview_hash=str(value.get('preview_hash') or ''),
        telegram_writes=int(value.get('telegram_writes') or 0),
        publication_status_mutations=int(value.get('publication_status_mutations') or 0),
    )
    if _stable_hash(_preview_payload(preview)) != preview.preview_hash:
        raise ApplyBlocked('PREVIEW_INVALID:preview_hash_mismatch')
    return preview


def _load_apply_package(conn: sqlite3.Connection, record: ManifestRecord) -> tuple[int, FrozenPublicationPackage]:
    row = conn.execute(
        '''SELECT p.*,c.deal_type,o.offer_type,o.publication_policy,l.public_listing_id,sp.source_mode
           FROM v3_publication_packages p
           JOIN canonical_records c ON c.id=p.canonical_record_id
           JOIN listing_offers o ON o.id=p.offer_id
           JOIN v3_listings l ON l.id=p.listing_id
           JOIN v3_source_posts sp ON sp.id=c.source_post_id
           WHERE p.package_id=?''',
        (record.frozen_package_id,),
    ).fetchone()
    if row is None:
        raise ApplyBlocked(f'package_not_found:{record.frozen_package_id}')
    if str(row['status']) not in {'frozen', 'published'}:
        raise ApplyBlocked(f'package_not_frozen:{record.frozen_package_id}')
    public_id = normalize_public_id(str(row['public_listing_id'] or ''))
    if public_id != record.public_ql_id:
        raise ApplyBlocked(f'public_id_drift:{record.target_key}')
    if str(row['deal_type']) != 'rent' or str(row['offer_type']) != 'rent' or str(row['publication_policy']) != 'telegram_rent':
        raise ApplyBlocked(f'non_rent_package:{record.target_key}')
    if int(row['listing_id']) != record.listing_id or int(row['offer_id']) != record.offer_id or int(row['canonical_record_id']) != record.canonical_record_id:
        raise ApplyBlocked(f'package_identity_drift:{record.target_key}')
    if str(row['target_channel_id']) != record.channel_id or str(row['content_hash']) != record.after_content_hash:
        raise ApplyBlocked(f'package_content_drift:{record.target_key}')
    keyboard = _normalize_keyboard(row['keyboard_json'])
    if keyboard != record.buttons or tuple(text for text, _ in keyboard) != BUTTON_TEXTS:
        raise ApplyBlocked(f'button_contract_drift:{record.target_key}')
    if str(row['cover_path']) != record.cover or str(row['caption_html']) != record.caption:
        raise ApplyBlocked(f'package_render_drift:{record.target_key}')
    package = FrozenPublicationPackage(
        package_id=str(row['package_id']), listing_id=int(row['listing_id']), offer_id=int(row['offer_id']),
        canonical_record_id=int(row['canonical_record_id']), target_channel_id=str(row['target_channel_id']),
        deal_type='rent', offer_type='rent', publication_policy='telegram_rent', source_mode=str(row['source_mode']),
        quality_result='AUTO_PUBLISH' if str(row['approval_mode']) == 'auto' else 'ADMIN_APPROVED',
        canonical_hash=str(row['canonical_hash']), cover_path=str(row['cover_path']),
        gallery=tuple(str(item) for item in json.loads(row['gallery_json'] or '[]')),
        caption_html=str(row['caption_html']), keyboard=keyboard, content_hash=str(row['content_hash']), frozen=True,
    )
    return int(row['id']), package


def _inspect_hash(gateway: Any, record: ManifestRecord) -> str:
    inspect = getattr(gateway, 'inspect_migration_post', None)
    if not callable(inspect):
        raise ApplyBlocked('migration_gateway_requires_inspect')
    result = inspect(channel_id=record.channel_id, message_id=record.message_id)
    if isinstance(result, Mapping):
        value = result.get('content_hash')
    else:
        value = result
    current = str(value or '')
    if not current:
        raise ApplyBlocked(f'missing_live_content_hash:{record.target_key}')
    return current


def _finalize_mapping(conn: sqlite3.Connection, record: ManifestRecord, package_row_id: int) -> None:
    try:
        conn.execute(
            '''INSERT INTO v3_channel_posts(
                   idempotency_key,channel_id,message_id,listing_id,offer_id,current_package_id,
                   publication_kind,content_hash,post_status,last_synced_at,published_at,updated_at
               ) VALUES (?,?,?,?,?,?,'telegram_rent',?,'migration_applied',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
               ON CONFLICT(channel_id,message_id) DO UPDATE SET
                   idempotency_key=excluded.idempotency_key,
                   listing_id=excluded.listing_id,
                   offer_id=excluded.offer_id,
                   current_package_id=excluded.current_package_id,
                   publication_kind='telegram_rent',
                   content_hash=excluded.content_hash,
                   post_status='migration_applied',
                   last_synced_at=CURRENT_TIMESTAMP,
                   published_at=COALESCE(v3_channel_posts.published_at,CURRENT_TIMESTAMP),
                   updated_at=CURRENT_TIMESTAMP''',
            (
                f'migration:{record.frozen_package_id}:{record.channel_id}:{record.message_id}',
                record.channel_id, record.message_id, record.listing_id, record.offer_id,
                package_row_id, record.after_content_hash,
            ),
        )
        conn.execute(
            "UPDATE v3_publication_packages SET status='published', published_at=COALESCE(published_at,CURRENT_TIMESTAMP) WHERE id=? AND status IN ('frozen','published')",
            (package_row_id,),
        )
        conn.commit()
    except sqlite3.DatabaseError as exc:
        conn.rollback()
        raise ApplyBlocked(f'mapping_commit_failed:{record.target_key}:{exc}') from exc


def apply_preview(
    conn: sqlite3.Connection,
    preview: RebuildPreview,
    *,
    gateway: Any,
    approved_preview_hash: str,
) -> ApplyResult:
    """Phase-11 exact-message migration apply with fail-fast preflight and crash-safe recovery."""
    if str(approved_preview_hash or '') != preview.preview_hash:
        raise ApplyBlocked('explicit_approved_preview_hash_required')
    if _stable_hash(_preview_payload(preview)) != preview.preview_hash:
        raise ApplyBlocked('PREVIEW_INVALID:preview_hash_mismatch')
    expected_targets = tuple(item.target_key for item in preview.manifest) + tuple(item.target_key for item in preview.failures)
    if tuple(sorted(expected_targets)) != tuple(sorted(preview.target_set)):
        raise ApplyBlocked('PREVIEW_INVALID:target_set_drift')

    preflight: list[tuple[ManifestRecord, int, FrozenPublicationPackage, str, str]] = []
    conflicts: list[str] = []
    for record in preview.manifest:
        if normalize_public_id(record.public_ql_id) != record.public_ql_id:
            conflicts.append(f'invalid_public_ql_id:{record.target_key}')
            continue
        if len(record.buttons) != 3 or tuple(text for text, _ in record.buttons) != BUTTON_TEXTS:
            conflicts.append(f'invalid_three_button_contract:{record.target_key}')
            continue
        package_row_id, package = _load_apply_package(conn, record)
        live_hash = _inspect_hash(gateway, record)
        if live_hash == record.before_content_hash:
            action = 'edit'
        elif live_hash == record.after_content_hash:
            action = 'recover'
        else:
            conflicts.append(f'before_hash_conflict:{record.target_key}:{live_hash}')
            continue
        preflight.append((record, package_row_id, package, live_hash, action))

    if conflicts:
        raise ApplyBlocked('APPLY_BLOCKED:' + '|'.join(conflicts))

    checkpoints: list[ApplyCheckpoint] = []
    writes = 0
    mappings = 0
    edit = getattr(gateway, 'edit_migration_post', None)
    if any(item[4] == 'edit' for item in preflight) and not callable(edit):
        raise ApplyBlocked('migration_gateway_requires_exact_edit')

    for record, package_row_id, package, live_hash, action in preflight:
        if action == 'edit':
            result = edit(
                channel_id=record.channel_id,
                message_id=record.message_id,
                cover=package.cover_path,
                caption=package.caption_html,
                keyboard=[{'text': text, 'url': url} for text, url in package.keyboard],
                expected_before_hash=record.before_content_hash,
                expected_after_hash=record.after_content_hash,
            )
            if isinstance(result, Mapping) and result.get('ok') is False:
                raise ApplyBlocked(f'gateway_edit_failed:{record.target_key}')
            writes += 1
        _finalize_mapping(conn, record, package_row_id)
        mappings += 1
        checkpoints.append(ApplyCheckpoint(
            target_key=record.target_key,
            action='edited' if action == 'edit' else 'recovered',
            package_id=record.frozen_package_id,
            listing_id=record.listing_id,
            offer_id=record.offer_id,
            channel_id=record.channel_id,
            message_id=record.message_id,
            before_content_hash=live_hash,
            after_content_hash=record.after_content_hash,
        ))

    return ApplyResult(
        preview_hash=preview.preview_hash,
        checkpoints=tuple(checkpoints),
        failures=(),
        telegram_writes=writes,
        mapping_mutations=mappings,
    )


def _load_gateway(factory_path: str) -> Any:
    module_name, sep, function_name = str(factory_path or '').partition(':')
    if not sep or not module_name or not function_name:
        raise SystemExit('gateway_factory_must_be_module:function')
    factory = getattr(importlib.import_module(module_name), function_name)
    return factory()


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description='V3 channel rebuild preview/apply')
    parser.add_argument('--db', required=True, help='SQLite DB containing V3 frozen packages')
    parser.add_argument('--output', required=True, help='Destination JSON report')
    parser.add_argument('--apply', action='store_true', help='Phase 11 exact-message migration apply')
    parser.add_argument('--slots', help='Preview mode: JSON explicit TEST slot mappings')
    parser.add_argument('--preview', help='Apply mode: approved Phase-10 preview JSON')
    parser.add_argument('--approved-preview-hash', help='Apply mode: exact manually approved preview hash')
    parser.add_argument('--gateway-factory', help='Apply mode: module:function returning migration gateway')
    args = parser.parse_args(argv)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    if args.apply:
        if not args.preview or not args.approved_preview_hash or not args.gateway_factory:
            raise SystemExit('phase11_apply_requires_preview_hash_and_gateway')
        preview = _preview_from_dict(json.loads(Path(args.preview).read_text(encoding='utf-8')))
        result = apply_preview(
            conn, preview,
            gateway=_load_gateway(args.gateway_factory),
            approved_preview_hash=args.approved_preview_hash,
        )
        Path(args.output).write_text(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + '\n', encoding='utf-8')
        return 0

    if not args.slots:
        raise SystemExit('preview_mode_requires_slots')
    mappings = json.loads(Path(args.slots).read_text(encoding='utf-8'))
    preview = generate_preview(load_candidates_from_db(conn, mappings))
    Path(args.output).write_text(json.dumps(preview.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + '\n', encoding='utf-8')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


__all__ = [
    'ApplyBlocked', 'ApplyCheckpoint', 'ApplyResult', 'ManifestRecord', 'PreviewFailure',
    'PreviewInvalid', 'RebuildCandidate', 'RebuildPreview', 'apply_preview', 'generate_preview',
    'load_candidates_from_db', 'main', 'validate_preview',
]
