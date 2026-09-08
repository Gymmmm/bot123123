from __future__ import annotations

import pytest

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


def test_sync_concurrent_worker_has_single_owner():
    service = SyncService(ChannelEditor(gateway=FakeTelegramGateway()))
    lease = service.acquire(key='channel:-100123:555', owner='worker-a')
    with pytest.raises(SyncBlocked, match='single_owner'):
        service.acquire(key='channel:-100123:555', owner='worker-b')
    service.release(lease)
    assert service.acquire(key='channel:-100123:555', owner='worker-b').owner == 'worker-b'


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
