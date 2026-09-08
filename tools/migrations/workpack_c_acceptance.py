from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from qiaolian_v3.db.migration_runner import MigrationRunner
from qiaolian_v3.db.repositories.canonical import CanonicalRecordRepository
from qiaolian_v3.ingest.source_service import SourceIngestService
from qiaolian_v3.listing import public_id as public_id_module
from qiaolian_v3.listing.materializer import CanonicalListingMaterializer
from qiaolian_v3.listing.public_id import normalize_public_id
from qiaolian_v3.parser.canonical import canonicalize_source
from qiaolian_v3.parser.quality_gate import DedupeDecision, evaluate_quality_gate
from qiaolian_v3.publishing.package import FrozenPublicationPackage, build_frozen_rent_package
from tools.migrations.rebuild_channel import RebuildPreview, generate_preview, load_candidates_from_db


FIXTURE_PATH = 'docs/channel_captions_full_2026-04-19.md@7f9865a73a28e7953211f6d16436791f866b734d'
TARGET_CHANNEL_ID = '-100123'
BOT_USERNAME = 'QiaoLianBot'
OFFICIAL_COVER_ROOT = Path('artifacts/v3/workpack_c/covers')
_ACCEPTANCE_PUBLIC_ID_ENTROPY = tuple('H4K7M5N8P6R4T7V5')


@dataclass(frozen=True)
class ProductionDerivedSource:
    source_name: str
    external_post_id: str
    raw_text: str
    fixture_row: str
    historical_status: str


# Exact historical channel-export bodies from the locked repository history.
REAL_RENT_SOURCES = (
    ProductionDerivedSource(
        source_name='Jinbianzufanz', external_post_id='148',
        raw_text=(
            '<b>🏠【侨联实拍 · 出租】Chamkar Mon · 1房1卫</b>\n\n'
            '📍 位置：Chamkar Mon\n📐 面积：73m² | 13楼\n💰 租金：$500/月\n\n'
            '✨ 房源亮点：\n· 近永旺1\n· 高楼层视野好\n\n'
            '📸 更多实拍图片👇\n💬 咨询/预约看房：@侨联客服'
        ),
        fixture_row='message_id=148 | listing_id=LST_01D5932FC4B4 | published_at=2026-04-12 18:27:09',
        historical_status='published',
    ),
    ProductionDerivedSource(
        source_name='Jinbianzufanz', external_post_id='161',
        raw_text=(
            '<b>🏠【侨联实拍 · 出租】BKK1 · 1房1卫</b>\n\n'
            '📍 位置：BKK1\n📐 面积：85m² | 14楼\n💰 租金：$1300/月\n\n'
            '✨ 房源亮点：\n· BKK1核心地段\n· 14楼景观视野\n· 85平大空间\n\n'
            '📸 更多实拍图片👇\n💬 咨询/预约看房：@侨联客服'
        ),
        fixture_row='message_id=161 | listing_id=LST_EDBADA0AB074 | published_at=2026-04-12 21:00:10',
        historical_status='published',
    ),
)

UNFROZEN_RENT_SOURCE = ProductionDerivedSource(
    source_name='Jinbianzufanz', external_post_id='117',
    raw_text=(
        '🏠【侨联地产实拍】香格里拉 · 2+1房\n\n💰 <b>租金：$900/月</b>\n🛏 2+1房\n'
        '🔑 押付：押一付一\n\n✨ <b>房源亮点</b>\n• 包物业费\n\n'
        '💎 <b>侨联地产 · 您在金边的自己人</b>\n实拍房源，细节真实可核。'
    ),
    fixture_row='message_id=117 | listing_id=LST_C2E19EF763C7 | published_at=2026-04-11 22:46:27',
    historical_status='published_source_for_unfrozen_negative_control',
)

# Negative controls are also existing production-derived rows; sale/unknown are
# intentionally not forced through Listing materialization when canonical facts
# do not contain a materializable offer.
SALE_SOURCE = ProductionDerivedSource(
    source_name='SuxingFC', external_post_id='phase5:SuxingFC:sale:78000',
    raw_text='金边公寓 出售 1居室 1厅 52㎡ $7.8万 时代 现房 装修家具家电齐\n区域：俄罗斯市场',
    fixture_row='phase5_channel_house_groups.tsv | SuxingFC | 金边公寓 出售 1居室 1厅 52㎡ $7.8万 时代 现房',
    historical_status='production-derived sale negative control',
)
UNKNOWN_SOURCE = ProductionDerivedSource(
    source_name='SuxingFC', external_post_id='phase5:SuxingFC:unknown:apartment',
    raw_text='公寓\n区域：金边',
    fixture_row='phase5_channel_house_groups.tsv | SuxingFC | 公寓 | 金边',
    historical_status='production-derived unknown negative control',
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


@contextmanager
def _deterministic_public_ids() -> Iterator[None]:
    values = iter(_ACCEPTANCE_PUBLIC_ID_ENTROPY)
    original = public_id_module.secrets.choice

    def deterministic_choice(_: str) -> str:
        try:
            return next(values)
        except StopIteration as exc:
            raise AssertionError('acceptance_public_id_entropy_exhausted') from exc

    public_id_module.secrets.choice = deterministic_choice
    try:
        yield
    finally:
        public_id_module.secrets.choice = original


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
        source_external_identity=f'production-derived:{source.source_name}',
        external_post_id=source.external_post_id,
        raw_text=source.raw_text, media=(),
        source_created_at='2026-04-19T00:00:00+00:00', fetched_at='2026-09-09T00:00:00+00:00',
        source_type='production_derived_export', source_mode='collector', source_url='', source_author=source.source_name,
        raw_payload={'provenance': 'production-derived', 'fixture_path': FIXTURE_PATH,
                     'fixture_row': source.fixture_row, 'historical_status': source.historical_status},
    )
    facts = canonicalize_source(
        raw_text=source.raw_text, sanitized_text=ingest.sanitized_text,
        source_identity={'source_post_id': ingest.source_post_id, 'source_type': 'production_derived_export',
                         'source_name': source.source_name, 'external_post_id': source.external_post_id},
        media_summary={'source_media_count': 0, 'historical_export_has_no_media_evidence': True},
    )
    canonical = CanonicalRecordRepository(conn).append(
        source_post_id=ingest.source_post_id, source_post_revision_id=ingest.revision_id,
        schema_version=str(facts['schema_version']), parser_revision=str(facts['parser_revision']),
        facts=facts, facts_hash=str(facts['canonical_facts_hash']), deal_type=str(facts.get('deal_type') or 'unknown'),
        deal_type_candidates=list(facts.get('deal_type_candidates') or []), quality=dict(facts.get('quality') or {}),
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
        (package.package_id, package.package_id, package.listing_id, package.offer_id, package.canonical_record_id,
         package.target_channel_id, status, package.cover_path, json.dumps(list(package.gallery), ensure_ascii=False),
         package.caption_html, json.dumps([{'text': t, 'url': u} for t, u in package.keyboard], ensure_ascii=False),
         package.content_hash, package.canonical_hash),
    )


def _materialize_rent_package(conn: sqlite3.Connection, canonical_id: int, facts: dict[str, Any], *, frozen: bool) -> FrozenPublicationPackage:
    materialized = CanonicalListingMaterializer(conn).materialize(canonical_id)
    public_id = materialized.public_listing_id
    if normalize_public_id(public_id) is None:
        raise AssertionError('materializer_must_assign_formal_ql')
    offer_id = materialized.offer_ids[0]
    # Quality is explicitly evaluated before migration acceptance. Historical
    # exports do not invent missing media; they therefore remain review/admin
    # approved rather than being misrepresented as collector AUTO_PUBLISH.
    quality = evaluate_quality_gate(
        facts, source_mode='collector',
        media_summary={'usable_count': 0, 'cover_candidates': [], 'source_identity_complete': True},
        dedupe_result=DedupeDecision.NEW,
    )
    if quality.routing_decision is None:
        raise AssertionError('rent_quality_must_have_routing_decision')
    conn.execute("UPDATE listing_offers SET publication_policy='telegram_rent' WHERE id=? AND offer_type='rent'", (offer_id,))
    cover = _write_cover(OFFICIAL_COVER_ROOT, public_id, facts)
    package = build_frozen_rent_package(
        listing_id=materialized.listing_id, offer_id=offer_id, canonical_record_id=canonical_id,
        target_channel_id=TARGET_CHANNEL_ID, canonical=facts,
        offer={'offer_type': 'rent', 'publication_policy': 'telegram_rent'},
        source_mode='collector', quality_result='ADMIN_APPROVED', canonical_hash=str(facts['canonical_facts_hash']),
        cover_path=cover, gallery=(), caption_html=_caption(facts, public_id),
        bot_username=BOT_USERNAME, public_listing_id=public_id,
    )
    _insert_package(conn, package, status='frozen' if frozen else 'prepared')
    conn.commit()
    return package


def build_official_preview(workdir: Path | None = None) -> RebuildPreview:
    del workdir
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    MigrationRunner(conn).migrate()
    with _deterministic_public_ids():
        rent_packages: list[FrozenPublicationPackage] = []
        for source in REAL_RENT_SOURCES:
            canonical_id, facts = _persist_source(conn, source)
            if str(facts.get('deal_type')) != 'rent':
                raise AssertionError('real_rent_fixture_must_parse_as_rent')
            rent_packages.append(_materialize_rent_package(conn, canonical_id, facts, frozen=True))

        sale_id, sale_facts = _persist_source(conn, SALE_SOURCE)
        if str(sale_facts.get('deal_type')) != 'sale':
            raise AssertionError('sale_negative_control_must_parse_as_sale')

        unknown_id, unknown_facts = _persist_source(conn, UNKNOWN_SOURCE)
        if str(unknown_facts.get('deal_type')) != 'unknown':
            raise AssertionError('unknown_negative_control_must_stay_unknown')

        unfrozen_id, unfrozen_facts = _persist_source(conn, UNFROZEN_RENT_SOURCE)
        if str(unfrozen_facts.get('deal_type')) != 'rent':
            raise AssertionError('unfrozen_negative_control_must_parse_as_rent')
        unfrozen_package = _materialize_rent_package(conn, unfrozen_id, unfrozen_facts, frozen=False)

    mappings = [
        {'package_id': rent_packages[0].package_id, 'channel_id': TARGET_CHANNEL_ID, 'message_id': 5001, 'current_content_hash': _sha('TEST_SLOT_5001')},
        {'package_id': rent_packages[1].package_id, 'channel_id': TARGET_CHANNEL_ID, 'message_id': 5002, 'current_content_hash': _sha('TEST_SLOT_5002')},
        {'canonical_record_id': sale_id, 'channel_id': TARGET_CHANNEL_ID, 'message_id': 5003, 'current_content_hash': _sha('TEST_SLOT_5003')},
        {'canonical_record_id': unknown_id, 'channel_id': TARGET_CHANNEL_ID, 'message_id': 5004, 'current_content_hash': _sha('TEST_SLOT_5004')},
        {'package_id': unfrozen_package.package_id, 'channel_id': TARGET_CHANNEL_ID, 'message_id': 5005, 'current_content_hash': _sha('TEST_SLOT_5005')},
    ]
    before = conn.total_changes
    preview = generate_preview(load_candidates_from_db(conn, mappings))
    if conn.total_changes != before:
        raise AssertionError('preview_loader_must_be_read_only')
    return preview


def write_official_preview(output: Path) -> RebuildPreview:
    output.parent.mkdir(parents=True, exist_ok=True)
    preview = build_official_preview()
    output.write_text(json.dumps(preview.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + '\n', encoding='utf-8')
    return preview


__all__ = ['FIXTURE_PATH','OFFICIAL_COVER_ROOT','REAL_RENT_SOURCES','SALE_SOURCE','UNKNOWN_SOURCE','UNFROZEN_RENT_SOURCE','build_official_preview','write_official_preview']
