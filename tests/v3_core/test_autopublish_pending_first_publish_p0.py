from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from v3_core.publishing.autopilot import AutoPublishRepository, AutoPublishService
from v3_core.publishing.delivery_state import DeliveryBlocked
from v3_core.publishing.package_store import FrozenPackage
from v3_core.storage.appointment_repository import V3ListingBookabilityChecker
from v3_core.storage.bootstrap import initialize_v3_storage
from tests.v3_core.test_autopublish_simplified_contract import CHANNEL_ID, _insert_listing


def _package(listing_id: str, offer_id: str, review_id: str) -> FrozenPackage:
    return FrozenPackage(
        package_id="PKG_AUTO_1",
        listing_id=listing_id,
        offer_id=offer_id,
        canonical_record_id="CAN_1",
        package_version=1,
        status="approved",
        cover_style="classic_blue",
        cover_path="/frozen/cover.jpg",
        gallery=("/frozen/cover.jpg",),
        post_text="caption-active",
        actions={},
        snapshot={"inventory_status": "active"},
        frozen_file_hashes={},
        source_identity={},
        public_token="qltoken",
        canonical_facts_hash="hash_1",
        content_hash="content-hash",
    )


class _FakeWorkflow:
    def __init__(self, listing_id: str, offer_id: str, review_id: str, *, facts_hash: str = "hash_1"):
        self.listing_id = listing_id
        self.offer_id = offer_id
        self.review_id = review_id
        self.facts_hash = facts_hash
        self.delivery = object()
        self.approved_review = False
        self.built = 0

    def review_detail(self, review_id: str):
        return SimpleNamespace(
            review={"review_id": review_id, "review_status": "pending"},
            listing={"listing_id": self.listing_id, "inventory_status": "pending"},
            offer={"offer_id": self.offer_id, "offer_type": "rent"},
            canonical={
                "facts": {
                    "deal_type": "rent",
                    "project_name": "炳发城",
                    "public_location_display": "BKK1",
                    "layout": "2房2卫",
                    "quality": {"blocking_flags": []},
                },
                "facts_hash": self.facts_hash,
            },
        )

    def review_media(self, *, review_id: str):
        return SimpleNamespace(
            gallery_paths=("/tmp/a.jpg", "/tmp/b.jpg", "/tmp/c.jpg", "/tmp/d.jpg"),
            cover_source_path="/tmp/a.jpg",
        )

    def approve_review(self, *, review_id: str, operator_user_id: str) -> None:
        self.approved_review = True

    def build_package_for_review(self, *, review_id: str) -> FrozenPackage:
        self.built += 1
        return _package(self.listing_id, self.offer_id, review_id)

    def approve_package(self, *, package_id: str, approved_by: str) -> FrozenPackage:
        return _package(self.listing_id, self.offer_id, self.review_id)


def _seed_media(tmp_path) -> None:
    for name in ("a.jpg", "b.jpg", "c.jpg", "d.jpg"):
        path = tmp_path / name
        path.write_bytes(b"img")


def _status(db, listing_id: str) -> str:
    import sqlite3

    with sqlite3.connect(db) as conn:
        return str(
            conn.execute(
                "SELECT inventory_status FROM listings_v3 WHERE listing_id=?",
                (listing_id,),
            ).fetchone()[0]
        )


def _publication_count(db) -> int:
    import sqlite3

    with sqlite3.connect(db) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM publication_instances").fetchone()[0])


def _record_publication(db, *, listing_id: str, offer_id: str, message_id: str = "99") -> None:
    import sqlite3

    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT OR IGNORE INTO publication_packages_v3
               (package_id,listing_id,offer_id,canonical_record_id,package_version,status,
                cover_style,cover_path,gallery_json,post_text,actions_json,snapshot_json,
                frozen_file_hashes_json,source_identity_json,public_token,
                canonical_facts_hash,content_hash)
               VALUES ('PKG_AUTO_1',?,?, 'CAN_1',1,'published',
                       'classic_blue','/frozen/cover.jpg','[]','caption','{}','{}','{}','{}',
                       'qltoken','hash_1','content-hash')""",
            (listing_id, offer_id),
        )
        conn.execute(
            """INSERT OR IGNORE INTO publication_instances
               (instance_id,package_id,listing_id,offer_id,platform,channel_chat_id,
                channel_message_id,publish_status)
               VALUES ('PUB_AUTO_1','PKG_AUTO_1',?,?,'telegram',?,?,'published')""",
            (listing_id, offer_id, CHANNEL_ID, message_id),
        )
        conn.commit()


@pytest.fixture
def pending_env(tmp_path, monkeypatch):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    _seed_media(tmp_path)
    listing_id, offer_id, review_id = _insert_listing(
        db, suffix="1", inventory_status="pending"
    )
    repo = AutoPublishRepository(db)
    repo.ensure_defaults()
    repo.sync_candidates()
    workflow = _FakeWorkflow(listing_id, offer_id, review_id)
    service = AutoPublishService(
        workflow=workflow,
        repository=repo,
        channel_chat_id=CHANNEL_ID,
    )
    return SimpleNamespace(
        db=db,
        tmp_path=tmp_path,
        listing_id=listing_id,
        offer_id=offer_id,
        review_id=review_id,
        repo=repo,
        workflow=workflow,
        service=service,
        monkeypatch=monkeypatch,
    )


def test_pending_auto_first_publish_success_activates_and_is_bookable(pending_env):
    calls = []

    async def fake_deliver(**kwargs):
        calls.append(kwargs)
        assert kwargs["inventory_status_override"] == "active"
        assert _status(pending_env.db, pending_env.listing_id) == "pending"
        _record_publication(
            pending_env.db,
            listing_id=pending_env.listing_id,
            offer_id=pending_env.offer_id,
        )
        return SimpleNamespace(publication=SimpleNamespace(channel_message_id="99"))

    pending_env.monkeypatch.setattr(
        "v3_core.publishing.autopilot.deliver_approved_package", fake_deliver
    )
    pending_env.monkeypatch.setattr(
        "v3_core.publishing.autopilot.Path.is_file", lambda self: True
    )

    result = asyncio.run(pending_env.service.process_one(bot=object(), force_offer_id=pending_env.offer_id))

    assert result.status == "published"
    assert _publication_count(pending_env.db) == 1
    assert _status(pending_env.db, pending_env.listing_id) == "active"
    assert V3ListingBookabilityChecker(pending_env.db).is_bookable(pending_env.listing_id) is True
    assert calls and calls[0]["inventory_status_override"] == "active"


def test_pending_auto_first_publish_failed_stays_pending(pending_env):
    async def fake_deliver(**kwargs):
        assert _status(pending_env.db, pending_env.listing_id) == "pending"
        raise DeliveryBlocked("telegram send failed")

    pending_env.monkeypatch.setattr(
        "v3_core.publishing.autopilot.deliver_approved_package", fake_deliver
    )
    pending_env.monkeypatch.setattr(
        "v3_core.publishing.autopilot.Path.is_file", lambda self: True
    )

    result = asyncio.run(pending_env.service.process_one(bot=object(), force_offer_id=pending_env.offer_id))

    assert result.status == "exception"
    assert result.reason_code == "telegram_failed"
    assert _publication_count(pending_env.db) == 0
    assert _status(pending_env.db, pending_env.listing_id) == "pending"
    assert V3ListingBookabilityChecker(pending_env.db).is_bookable(pending_env.listing_id) is False


def test_pending_auto_first_publish_unknown_stays_pending(pending_env):
    async def fake_deliver(**kwargs):
        assert _status(pending_env.db, pending_env.listing_id) == "pending"
        raise RuntimeError("socket timeout after send boundary")

    pending_env.monkeypatch.setattr(
        "v3_core.publishing.autopilot.deliver_approved_package", fake_deliver
    )
    pending_env.monkeypatch.setattr(
        "v3_core.publishing.autopilot.Path.is_file", lambda self: True
    )

    result = asyncio.run(pending_env.service.process_one(bot=object(), force_offer_id=pending_env.offer_id))

    assert result.status == "exception"
    assert result.reason_code == "telegram_unknown"
    assert _publication_count(pending_env.db) == 0
    assert _status(pending_env.db, pending_env.listing_id) == "pending"


def test_pending_auto_first_publish_replay_does_not_create_second_publication(pending_env):
    send_calls = []

    async def fake_deliver(**kwargs):
        send_calls.append(kwargs)
        _record_publication(
            pending_env.db,
            listing_id=pending_env.listing_id,
            offer_id=pending_env.offer_id,
        )
        return SimpleNamespace(publication=SimpleNamespace(channel_message_id="99"))

    pending_env.monkeypatch.setattr(
        "v3_core.publishing.autopilot.deliver_approved_package", fake_deliver
    )
    pending_env.monkeypatch.setattr(
        "v3_core.publishing.autopilot.Path.is_file", lambda self: True
    )

    first = asyncio.run(pending_env.service.process_one(bot=object(), force_offer_id=pending_env.offer_id))
    second = asyncio.run(pending_env.service.process_one(bot=object(), force_offer_id=pending_env.offer_id))

    assert first.status == "published"
    assert second.status == "already_published"
    assert second.channel_message_id == "99"
    assert len(send_calls) == 1
    assert _publication_count(pending_env.db) == 1
    assert _status(pending_env.db, pending_env.listing_id) == "active"
    assert V3ListingBookabilityChecker(pending_env.db).is_bookable(pending_env.listing_id) is True
