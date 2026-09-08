from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.db.migration_runner import MigrationRunner
from qiaolian_v3.publishing.package import FrozenPublicationPackage, build_frozen_rent_package
from qiaolian_v3.publishing.channel_editor import ChannelEditor, ChannelEditBlocked
from qiaolian_v3.publishing.sync_service import SyncService, SyncBlocked
from qiaolian_v3.publishing.auto_publish import AutoPublishPlanner
from tests.v3.fakes import FakeTelegramGateway


def package(**overrides):
    data = dict(
        listing_id=11, offer_id=12, canonical_record_id=13, target_channel_id='-100123',
        canonical={'deal_type':'rent'}, offer={'offer_type':'rent','publication_policy':'telegram_rent'},
        source_mode='collector', quality_result='AUTO_PUBLISH', canonical_hash='canon',
        cover_path='/tmp/cover.jpg', gallery=['1','2','3','4'], caption_html='ok',
        bot_username='QiaoLianBot', public_listing_id='QL000011',
    )
    data.update(overrides)
    return build_frozen_rent_package(**data)


def mapping(pkg):
    return {
        'channel_id': pkg.target_channel_id,
        'message_id': 555,
        'listing_id': pkg.listing_id,
        'offer_id': pkg.offer_id,
        'content_hash': 'previous-hash',
    }


def populate_sync_db(conn: sqlite3.Connection, pkg: FrozenPublicationPackage) -> None:
    conn.row_factory = sqlite3.Row
    MigrationRunner(conn).migrate()
    conn.execute("INSERT INTO v3_sources(source_type,source_name,external_identity) VALUES ('telegram','s','s')")
    conn.execute("INSERT INTO v3_source_posts(source_id,source_identity_key,source_mode,source_type,source_name,external_post_id) VALUES (1,'sp','collector','telegram','s','1')")
    conn.execute("INSERT INTO source_post_revisions(source_post_id,revision_no,source_content_hash,raw_text,sanitized_text) VALUES (1,1,'rch','x','x')")
    conn.execute("INSERT INTO canonical_records(id,source_post_id,source_post_revision_id,schema_version,parser_revision,facts_json,facts_hash,deal_type) VALUES (13,1,1,'v3','p','{}','canon','rent')")
    conn.execute("INSERT INTO v3_listings(id,public_listing_id,current_canonical_record_id,property_identity_key,listing_status) VALUES (11,'QL000011',13,'prop-11','active')")
    conn.execute("INSERT INTO listing_offers(id,listing_id,canonical_record_id,offer_type,monthly_rent_usd,publication_policy) VALUES (12,11,13,'rent',800,'telegram_rent')")
    conn.execute(
        """INSERT INTO v3_publication_packages(
        id,package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,target_channel_id,status,approval_mode,
        caption_html,keyboard_json,content_hash,canonical_hash)
        VALUES (1,'OLD','OLD',11,12,13,1,?,'published','auto','old','{}','previous-hash','canon')""",
        (pkg.target_channel_id,),
    )
    conn.execute(
        """INSERT INTO v3_publication_packages(
        id,package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,target_channel_id,status,approval_mode,
        cover_path,gallery_json,caption_html,keyboard_json,content_hash,canonical_hash)
        VALUES (2,?,?,11,12,13,2,?,'frozen','auto',?,'[]',?,'{}',?,'canon')""",
        (pkg.package_id, pkg.package_id, pkg.target_channel_id, pkg.cover_path, pkg.caption_html, pkg.content_hash),
    )
    conn.execute(
        """INSERT INTO v3_channel_posts(
        idempotency_key,channel_id,message_id,listing_id,offer_id,current_package_id,publication_kind,content_hash,post_status)
        VALUES ('post-555',?,555,11,12,1,'telegram_rent','previous-hash','published')""",
        (pkg.target_channel_id,),
    )
    conn.commit()


def test_channel_editor_requires_explicit_message_and_exact_mapping_hash_channel():
    pkg = package()
    editor = ChannelEditor(gateway=FakeTelegramGateway())
    m = mapping(pkg)
    with pytest.raises(ChannelEditBlocked, match='explicit_message_id_required'):
        editor.plan_edit(package=pkg, mapping=m, channel_id=pkg.target_channel_id, message_id=0, expected_previous_hash='previous-hash')
    with pytest.raises(ChannelEditBlocked, match='mapping_message_mismatch'):
        editor.plan_edit(package=pkg, mapping=m, channel_id=pkg.target_channel_id, message_id=556, expected_previous_hash='previous-hash')
    with pytest.raises(ChannelEditBlocked, match='channel_mismatch'):
        editor.plan_edit(package=pkg, mapping=m, channel_id='-100999', message_id=555, expected_previous_hash='previous-hash')
    with pytest.raises(ChannelEditBlocked, match='previous_hash_conflict'):
        editor.plan_edit(package=pkg, mapping=m, channel_id=pkg.target_channel_id, message_id=555, expected_previous_hash='wrong')


def test_editor_default_dry_run_zero_writes_and_sale_is_blocked():
    pkg = package()
    fake = FakeTelegramGateway()
    editor = ChannelEditor(gateway=fake)
    plan = editor.plan_edit(package=pkg, mapping=mapping(pkg), channel_id=pkg.target_channel_id, message_id=555, expected_previous_hash='previous-hash')
    result = editor.execute(plan, pkg)
    assert plan.dry_run is True and result['writes'] == 0
    assert fake.write_count == 0
    sale = FrozenPublicationPackage(**{**pkg.__dict__, 'deal_type':'sale', 'offer_type':'sale', 'publication_policy':'store_only'})
    with pytest.raises(ChannelEditBlocked, match='frozen_rent_package'):
        editor.plan_edit(package=sale, mapping=mapping(pkg), channel_id=pkg.target_channel_id, message_id=555, expected_previous_hash='previous-hash')


def test_two_independent_sync_services_share_durable_single_owner(tmp_path):
    pkg = package(caption_html='new caption')
    db_path = str(tmp_path / 'sync-owner.db')
    setup = sqlite3.connect(db_path)
    populate_sync_db(setup, pkg)
    setup.close()

    fake = FakeTelegramGateway()
    conn_a = sqlite3.connect(db_path)
    conn_b = sqlite3.connect(db_path)
    service_a = SyncService(ChannelEditor(gateway=fake, dry_run=False), conn_a)
    service_b = SyncService(ChannelEditor(gateway=fake, dry_run=False), conn_b)
    lease_a = service_a.acquire(key='channel:-100123:555', owner='worker-a', expected_content_hash='previous-hash')
    with pytest.raises(SyncBlocked, match='single_owner'):
        service_b.acquire(key='channel:-100123:555', owner='worker-b', expected_content_hash='previous-hash')

    plan = service_a.plan_sync(
        lease=lease_a, package=pkg, channel_id=pkg.target_channel_id,
        message_id=555, expected_previous_hash='previous-hash',
    )
    result = service_a.execute_sync(lease=lease_a, plan=plan, package=pkg)
    assert result['ok'] is True
    assert fake.write_count == 1
    assert fake.calls[0][0] == 'edit'
    assert conn_a.execute("SELECT content_hash FROM v3_channel_posts WHERE message_id=555").fetchone()[0] == pkg.content_hash
    service_a.release(lease_a)
    assert service_b.acquire(key='channel:-100123:555', owner='worker-b', expected_content_hash=pkg.content_hash).owner == 'worker-b'
    conn_a.close()
    conn_b.close()


def test_sync_same_hash_is_noop_not_fallback_send(tmp_path):
    pkg = package()
    db_path = str(tmp_path / 'sync-noop.db')
    conn = sqlite3.connect(db_path)
    populate_sync_db(conn, pkg)
    conn.execute("UPDATE v3_channel_posts SET content_hash=? WHERE message_id=555", (pkg.content_hash,))
    conn.commit()
    fake = FakeTelegramGateway()
    service = SyncService(ChannelEditor(gateway=fake, dry_run=False), conn)
    lease = service.acquire(key='channel:-100123:555', owner='worker-a', expected_content_hash=pkg.content_hash)
    plan = service.plan_sync(lease=lease, package=pkg, channel_id=pkg.target_channel_id, message_id=555, expected_previous_hash=pkg.content_hash)
    assert plan.action == 'noop'
    result = service.execute_sync(lease=lease, plan=plan, package=pkg)
    assert result['action'] == 'noop' and result['writes'] == 0
    assert fake.write_count == 0


def test_plan_a_package_b_mismatch_blocks_before_edit():
    pkg_a = package(caption_html='package A')
    pkg_b = package(caption_html='package B')
    fake = FakeTelegramGateway()
    editor = ChannelEditor(gateway=fake, dry_run=False)
    plan_a = editor.plan_edit(
        package=pkg_a, mapping=mapping(pkg_a), channel_id=pkg_a.target_channel_id,
        message_id=555, expected_previous_hash='previous-hash',
    )
    with pytest.raises(ChannelEditBlocked, match='plan_package_mismatch|plan_content_hash_mismatch'):
        editor.execute(plan_a, pkg_b)
    assert fake.write_count == 0


def test_collector_good_rent_exactly_one_planned_publish_and_bad_inputs_zero():
    pkg = package()
    planner = AutoPublishPlanner()
    plans = planner.plan([
        (pkg, {'source_mode':'collector','quality_result':'AUTO_PUBLISH'}),
        (pkg, {'source_mode':'collector','quality_result':'AUTO_PUBLISH'}),
    ])
    assert len(plans) == 1
    assert plans[0].package_id == pkg.package_id and plans[0].dry_run is True

    admin = package(source_mode='admin_import')
    assert planner.plan([(admin, {'source_mode':'admin_import','quality_result':'AUTO_PUBLISH'})]) == []
    review = package(quality_result='NEEDS_REVIEW')
    assert planner.plan([(review, {'source_mode':'collector','quality_result':'NEEDS_REVIEW'})]) == []
