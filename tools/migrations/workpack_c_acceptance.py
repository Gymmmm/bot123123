from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from qiaolian_v3.db.migration_runner import MigrationRunner
from qiaolian_v3.db.repositories.canonical import CanonicalRecordRepository
from qiaolian_v3.ingest.source_service import SourceIngestService
from qiaolian_v3.listing.materializer import CanonicalListingMaterializer
from qiaolian_v3.listing.public_id import normalize_public_id
from qiaolian_v3.parser.canonical import canonicalize_source
from qiaolian_v3.publishing.package import FrozenPublicationPackage, build_frozen_rent_package
from tools.migrations.rebuild_channel import RebuildPreview, generate_preview, load_candidates_from_db


FIXTURE_PATH = 'tests/v3/fixtures/phase5_channel_house_groups.tsv'
TARGET_CHANNEL_ID = '-100123'
BOT_USERNAME = 'QiaoLianBot'


@dataclass(frozen=True)
class ProductionDerivedSource:
    source_name: str
    external_post_id: str
    raw_text: str
    fixture_row: str
    historical_status: str


REAL_RENT_SOURCES = (
    ProductionDerivedSource(
        source_name='SuxingFC',
        external_post_id='phase5:SuxingFC:太子幸福广场:430',
        raw_text='金边公寓 出租 1居室 55㎡ $430 太子幸福广场 精装修 优选单身公寓\n区域：俄罗斯市场',
        fixture_row='SuxingFC\t金边公寓 出租 1居室 55㎡ $430 太子幸福广场 精装修 优选单身公寓\t俄罗斯市场\t1房\t430\t1\t0\t0\t1',
        historical_status='published=1',
    ),
    ProductionDerivedSource(
        source_name='SuxingFC',
        external_post_id='phase5:SuxingFC:时代广场:1000',
        raw_text='金边公寓 出租 2居室 2卫 80㎡ $1000 时代广场 家具家电配齐 首次\n区域：俄罗斯市场',
        fixture_row='SuxingFC\t金边公寓 出租 2居室 2卫 80㎡ $1000 时代广场 家具家电配齐 首次\t俄罗斯市场\t2房\t1000\t1\t0\t0\t1',
        historical_status='published=1',
    ),
)

UNFROZEN_RENT_SOURCE = ProductionDerivedSource(
    source_name='jinbianfangchanzushou',
    external_post_id='phase5:jinbianfangchanzushou:penthouse:800',
    raw_text='公寓出租 Penthouse\n区域：金边\n户型：2房\n租金：$800/月',
    fixture_row='jinbianfangchanzushou\t公寓出租】Penthouse\t金边\t2房\t800\t1\t1\t0\t0',
    historical_status='pending=1',
)

SALE_SOURCE = ProductionDerivedSource(
    source_name='SuxingFC',
    external_post_id='phase5:SuxingFC:sale:78000',
    raw_text='金边公寓 出售 1居室 1厅 52㎡ $7.8万 时代 现房 装修家具家电齐\n区域：俄罗斯市场',
    fixture_row='SuxingFC\t金边公寓 出售 1居室 1厅 52㎡ $7.8万 时代 现房 装修家具家电齐 硬卡\t俄罗斯市场\t1房\t7\t1\t1\t0\t0',
    historical_status='production-derived sale negative control',
)

UNKNOWN_SOURCE = ProductionDerivedSource(
    source_name='SuxingFC',
    external_post_id='phase5:SuxingFC:unknown:apartment',
    raw_text='公寓\n区域：金边',
    fixture_row='SuxingFC\t公寓\t金边\t公寓\t\t11\t11\t0\t0',
    historical_status='production-derived unknown negative control',
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def _caption(facts: dict[str, Any], public_id: str) -> str:
    project = str(facts.get('project_name') or facts.get('public_location_display') or '金边房源')
    layout = str(facts.get('layout') or facts.get('property_type_display') or facts.get('property_type') or '房源')
    size = facts.get('size_sqm')
    price = int(facts.get('monthly_rent_usd') or 0)
    line = f'🏠 {project}｜{layout}'
    if isinstance(size, (int, float)) and float(size) > 0:
        line += f'｜{int(size)}㎡'
    return f'{line}\n💰 ${price}/月\n🆔 {public_id}'


def _write_cover(root: Path, public_id: str, facts: dict[str, Any]) -> str:
    ql = normalize_public_id(public_id)
    if ql is None:
        raise ValueError('invalid_public_ql_id')
    root.mkdir(parents=True, exist_ok=True)
    path = root / f'{ql}.svg'
    project = str(facts.get('project_name') or facts.get('public_location_display') or '金边房源')
    layout = str(facts.get('layout') or facts.get('property_type') or '房源')
    price = int(facts.get('monthly_rent_usd') or 0)
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="800" viewBox="0 0 1200 800">'
        '<rect width="1200" height="800" fill="#0f172a"/>'
        '<text x="90" y="180" fill="white" font-size="68" font-family="sans-serif">侨联地产 · PREVIEW</text>'
        f'<text x="90" y="330" fill="white" font-size="58" font-family="sans-serif">{project}</text>'
        f'<text x="90" y="440" fill="white" font-size="48" font-family="sans-serif">{layout} · ${price}/月</text>'
        f'<text x="90" y="650" fill="white" font-size="40" font-family="monospace">{ql}</text>'
        '</svg>'
    )
    path.write_text(svg, encoding='utf-8')
    return path.as_posix()


def _persist_source(conn: sqlite3.Connection, source: ProductionDerivedSource) -> tuple[int, dict[str, Any]]:
    ingest = SourceIngestService(conn).ingest_telegram(
        source_name=source.source_name,
        source_external_identity=f'phase5-export:{source.source_name}',
        external_post_id=source.external_post_id,
        raw_text=source.raw_text,
        media=(),
        source_created_at='2026-04-19T00:00:00+00:00',
        fetched_at='2026-09-09T00:00:00+00:00',
        source_type='production_derived_export',
        source_mode='collector',
        source_url='',
        source_author=source.source_name,
        raw_payload={
            'provenance': 'production-derived',
            'fixture_path': FIXTURE_PATH,
            'fixture_row': source.fixture_row,
            'historical_status': source.historical_status,
        },
    )
    facts = canonicalize_source(
        raw_text=source.raw_text,
        sanitized_text=ingest.sanitized_text,
        source_identity={
            'source_post_id': ingest.source_post_id,
            'source_type': 'production_derived_export',
            'source_name': source.source_name,
            'external_post_id': source.external_post_id,
        },
        media_summary={'source_media_count': 0, 'historical_export_has_no_media_evidence': True},
    )
    canonical = CanonicalRecordRepository(conn).append(
        source_post_id=ingest.source_post_id,
        source_post_revision_id=ingest.revision_id,
        schema_version=str(facts['schema_version']),
        parser_revision=str(facts['parser_revision']),
        facts=facts,
        facts_hash=str(facts['canonical_facts_hash']),
        deal_type=str(facts.get('deal_type') or 'unknown'),
        deal_type_candidates=list(facts.get('deal_type_candidates') or []),
        quality=dict(facts.get('quality') or {}),
    )
    conn.commit()
    return canonical.id, facts


def _insert_package(conn: sqlite3.Connection, package: FrozenPublicationPackage, *, status: str) -> None:
    conn.execute(
        '''INSERT INTO v3_publication_packages(
            package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,
            target_channel_id,status,approval_mode,approved_by,approved_at,
            cover_path,gallery_json,caption_html,keyboard_json,content_hash,canonical_hash
        ) VALUES (?,?,?,?,?,1,?,?, 'admin','workpack-c-acceptance',CURRENT_TIMESTAMP,?,?,?,?,?,?)''',
        (
            package.package_id, package.package_id, package.listing_id, package.offer_id,
            package.canonical_record_id, package.target_channel_id, status,
            package.cover_path, json.dumps(list(package.gallery), ensure_ascii=False), package.caption_html,
            json.dumps([{'text': text, 'url': url} for text, url in package.keyboard], ensure_ascii=False),
            package.content_hash, package.canonical_hash,
        ),
    )


def _materialize_rent_package(
    conn: sqlite3.Connection,
    canonical_id: int,
    facts: dict[str, Any],
    cover_root: Path,
    *,
    frozen: bool,
) -> FrozenPublicationPackage:
    materialized = CanonicalListingMaterializer(conn).materialize(canonical_id)
    public_id = materialized.public_listing_id
    if normalize_public_id(public_id) is None:
        raise AssertionError('materializer_must_assign_formal_ql')
    offer_id = materialized.offer_ids[0]
    conn.execute(
        "UPDATE listing_offers SET publication_policy='telegram_rent' WHERE id=? AND offer_type='rent'",
        (offer_id,),
    )
    cover = _write_cover(cover_root, public_id, facts)
    package = build_frozen_rent_package(
        listing_id=materialized.listing_id,
        offer_id=offer_id,
        canonical_record_id=canonical_id,
        target_channel_id=TARGET_CHANNEL_ID,
        canonical=facts,
        offer={'offer_type': 'rent', 'publication_policy': 'telegram_rent'},
        source_mode='collector',
        quality_result='ADMIN_APPROVED',
        canonical_hash=str(facts['canonical_facts_hash']),
        cover_path=cover,
        gallery=(),
        caption_html=_caption(facts, public_id),
        bot_username=BOT_USERNAME,
        public_listing_id=public_id,
    )
    _insert_package(conn, package, status='frozen' if frozen else 'draft')
    conn.commit()
    return package


def build_official_preview(workdir: Path) -> RebuildPreview:
    """Build the official Workpack-C preview from production-derived DB state only."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    MigrationRunner(conn).migrate()
    cover_root = workdir / 'covers'

    rent_packages: list[FrozenPublicationPackage] = []
    for source in REAL_RENT_SOURCES:
        canonical_id, facts = _persist_source(conn, source)
        if str(facts.get('deal_type')) != 'rent':
            raise AssertionError('real_rent_fixture_must_parse_as_rent')
        rent_packages.append(_materialize_rent_package(conn, canonical_id, facts, cover_root, frozen=True))

    sale_id, sale_facts = _persist_source(conn, SALE_SOURCE)
    if str(sale_facts.get('deal_type')) != 'sale':
        raise AssertionError('sale_negative_control_must_parse_as_sale')
    CanonicalListingMaterializer(conn).materialize(sale_id)

    unknown_id, unknown_facts = _persist_source(conn, UNKNOWN_SOURCE)
    if str(unknown_facts.get('deal_type')) != 'unknown':
        raise AssertionError('unknown_negative_control_must_stay_unknown')

    unfrozen_id, unfrozen_facts = _persist_source(conn, UNFROZEN_RENT_SOURCE)
    if str(unfrozen_facts.get('deal_type')) != 'rent':
        raise AssertionError('unfrozen_negative_control_must_parse_as_rent')
    unfrozen_package = _materialize_rent_package(conn, unfrozen_id, unfrozen_facts, cover_root, frozen=False)

    mappings = [
        {'package_id': rent_packages[0].package_id, 'channel_id': TARGET_CHANNEL_ID, 'message_id': 5001, 'current_content_hash': _sha('TEST_SLOT_5001')},
        {'package_id': rent_packages[1].package_id, 'channel_id': TARGET_CHANNEL_ID, 'message_id': 5002, 'current_content_hash': _sha('TEST_SLOT_5002')},
        {'canonical_record_id': sale_id, 'channel_id': TARGET_CHANNEL_ID, 'message_id': 5003, 'current_content_hash': _sha('TEST_SLOT_5003')},
        {'canonical_record_id': unknown_id, 'channel_id': TARGET_CHANNEL_ID, 'message_id': 5004, 'current_content_hash': _sha('TEST_SLOT_5004')},
        {'package_id': unfrozen_package.package_id, 'channel_id': TARGET_CHANNEL_ID, 'message_id': 5005, 'current_content_hash': _sha('TEST_SLOT_5005')},
    ]
    before = conn.total_changes
    candidates = load_candidates_from_db(conn, mappings)
    preview = generate_preview(candidates)
    if conn.total_changes != before:
        raise AssertionError('preview_loader_must_be_read_only')
    return preview


def write_official_preview(output: Path) -> RebuildPreview:
    output.parent.mkdir(parents=True, exist_ok=True)
    preview = build_official_preview(output.parent)
    output.write_text(json.dumps(preview.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + '\n', encoding='utf-8')
    return preview


__all__ = [
    'FIXTURE_PATH', 'REAL_RENT_SOURCES', 'SALE_SOURCE', 'UNKNOWN_SOURCE', 'UNFROZEN_RENT_SOURCE',
    'build_official_preview', 'write_official_preview',
]
