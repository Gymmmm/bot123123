from __future__ import annotations

import sqlite3
import pytest

from qiaolian_v3.db.migration_runner import MigrationRunner
from qiaolian_v3.publishing.package import BUTTON_TEXTS, FrozenPublicationPackage, PackageBlocked, build_frozen_rent_package
from qiaolian_v3.publishing.eligibility import evaluate_rent_publication_eligibility
from qiaolian_v3.publishing.publisher import RentPublisher, PublicationBlocked
from qiaolian_v3.publishing.delivery import DeliveryCoordinator, DeliveryBlocked
from qiaolian_v3.publishing.discussion import DiscussionPublisher
from tests.v3.fakes import FakeTelegramGateway


def package(**overrides):
    data = dict(
        listing_id=1, offer_id=1, canonical_record_id=1, target_channel_id='-100123',
        canonical={'deal_type':'rent'}, offer={'offer_type':'rent','publication_policy':'telegram_rent'},
        source_mode='collector', quality_result='AUTO_PUBLISH', canonical_hash='canon-hash',
        cover_path='/tmp/cover.jpg', gallery=['a.jpg','b.jpg','c.jpg','d.jpg'],
        caption_html='🏠 <b>测试｜1房</b>\n💰 <b>$800/月</b>', bot_username='QiaoLianBot', public_listing_id='QL000001',
    )
    data.update(overrides)
    return build_frozen_rent_package(**data)


def db_with_package(pkg: FrozenPublicationPackage) -> sqlite3.Connection:
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    MigrationRunner(conn).migrate()
    conn.execute("INSERT INTO v3_sources(source_type,source_name,external_identity) VALUES ('telegram','s','s')")
    conn.execute("INSERT INTO v3_source_posts(source_id,source_identity_key,source_mode,source_type,source_name,external_post_id) VALUES (1,'sp','collector','telegram','s','1')")
    conn.execute("INSERT INTO source_post_revisions(source_post_id,revision_no,source_content_hash,raw_text,sanitized_text) VALUES (1,1,'rch','x','x')")
    conn.execute("INSERT INTO canonical_records(source_post_id,source_post_revision_id,schema_version,parser_revision,facts_json,facts_hash,deal_type) VALUES (1,1,'v3','p','{}','canon-hash','rent')")
    conn.execute("INSERT INTO v3_listings(id,public_listing_id,current_canonical_record_id,property_identity_key,listing_status) VALUES (1,'QL000001',1,'prop-1','active')")
    conn.execute("INSERT INTO listing_offers(id,listing_id,canonical_record_id,offer_type,monthly_rent_usd,publication_policy) VALUES (1,1,1,'rent',800,'telegram_rent')")
    conn.execute(
        """INSERT INTO v3_publication_packages(
        package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,target_channel_id,status,approval_mode,
        cover_path,gallery_json,caption_html,keyboard_json,content_hash,canonical_hash)
        VALUES (?,?,1,1,1,1,?,'frozen','auto',?,'[]',?,'{}',?,?)""",
        (pkg.package_id, pkg.package_id, pkg.target_channel_id, pkg.cover_path, pkg.caption_html, pkg.content_hash, pkg.canonical_hash),
    )
    conn.commit()
    return conn


def test_frozen_package_has_exact_three_buttons_and_caption_limit():
    pkg = package()
    assert tuple(text for text, _ in pkg.keyboard) == BUTTON_TEXTS
    assert len(pkg.keyboard) == 3
    assert len(pkg.caption_html) <= 1024
    with pytest.raises(PackageBlocked, match='caption_out_of_range'):
        package(caption_html='x' * 1025)


def test_sale_isolation_is_locked_at_five_layers():
    eligibility = evaluate_rent_publication_eligibility(
        canonical={'deal_type':'sale'}, offer={'offer_type':'sale','publication_policy':'store_only'},
        source_mode='collector', quality_result='AUTO_PUBLISH', frozen=True,
    )
    assert not eligibility.allowed
    with pytest.raises(PackageBlocked):
        package(canonical={'deal_type':'sale'}, offer={'offer_type':'sale','publication_policy':'store_only'})

    good = package()
    bad = FrozenPublicationPackage(**{**good.__dict__, 'deal_type':'sale', 'offer_type':'sale', 'publication_policy':'store_only'})
    fake = FakeTelegramGateway()
    conn = db_with_package(good)
    with pytest.raises(PublicationBlocked):
        RentPublisher(gateway=fake, delivery=DeliveryCoordinator(conn)).publish(bad)
    with pytest.raises(DeliveryBlocked):
        DeliveryCoordinator(conn).begin(bad)
    assert fake.write_count == 0


def test_dry_run_and_discussion_false_are_zero_writes():
    pkg = package()
    conn = db_with_package(pkg)
    fake = FakeTelegramGateway()
    result = RentPublisher(gateway=fake, delivery=DeliveryCoordinator(conn), dry_run=True).publish(pkg)
    assert result['writes'] == 0
    assert fake.write_count == 0
    assert DiscussionPublisher(gateway=fake).publish(text='x')['writes'] == 0
    assert fake.write_count == 0


def test_admin_source_auto_block():
    pkg = package(source_mode='admin_import')
    fake = FakeTelegramGateway()
    conn = db_with_package(pkg)
    with pytest.raises(PublicationBlocked, match='source_not_collector'):
        RentPublisher(gateway=fake, delivery=DeliveryCoordinator(conn), dry_run=False).publish(pkg)
    assert fake.write_count == 0


def test_delivery_retry_idempotency_and_unknown_never_blind_resends():
    pkg = package()
    conn = db_with_package(pkg)
    fake = FakeTelegramGateway()
    pub = RentPublisher(gateway=fake, delivery=DeliveryCoordinator(conn), dry_run=False)
    first = pub.publish(pkg)
    second = pub.publish(pkg)
    assert first['message_id'] == second['message_id'] == 1
    assert fake.write_count == 1

    pkg2 = package(listing_id=1, offer_id=1, canonical_record_id=1, target_channel_id='-100124')
    conn2 = db_with_package(pkg2)
    class AmbiguousGateway:
        calls = 0
        def send(self, **payload):
            self.calls += 1
            raise TimeoutError('unknown after send boundary')
    ambiguous = AmbiguousGateway()
    pub2 = RentPublisher(gateway=ambiguous, delivery=DeliveryCoordinator(conn2), dry_run=False)
    with pytest.raises(PublicationBlocked, match='unknown_reconcile_required'):
        pub2.publish(pkg2)
    with pytest.raises(DeliveryBlocked, match='unknown_requires_reconcile'):
        pub2.publish(pkg2)
    assert ambiguous.calls == 1
    state = DeliveryCoordinator(conn2).reconcile_unknown(pkg2, channel_id='-100124', message_id=77, observed_content_hash=pkg2.content_hash)
    assert state.state == 'published' and state.message_id == 77
