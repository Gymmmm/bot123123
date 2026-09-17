import asyncio
from pathlib import Path
from types import SimpleNamespace

from v3_core.publishing.autopilot_policy import ProductionAutoPublishService


class FakeInventory:
    def __init__(self):
        self.calls = []

    def set_offer_publication(self, *, offer_id, publishable, block_reason=""):
        self.calls.append((offer_id, bool(publishable), block_reason))
        return {"offer_id": offer_id, "publishable": int(bool(publishable)), "publish_block_reason": block_reason}


class FakeWorkflow:
    def __init__(self, *, facts, media, review_status="pending", media_error=None):
        self.inventory = FakeInventory()
        self._facts = facts
        self._media = media
        self._review_status = review_status
        self._media_error = media_error

    def review_detail(self, review_id):
        return SimpleNamespace(
            review={"review_id": review_id, "review_status": self._review_status},
            canonical={"facts": self._facts},
        )

    def review_media(self, *, review_id):
        if self._media_error:
            raise self._media_error
        return self._media


class FakeRepo:
    def __init__(self, *, existing=None, unsafe="", duplicate=False, min_media=4):
        self.existing = existing
        self.unsafe = unsafe
        self.duplicate = duplicate
        self.min_media = min_media
        self.items = []
        self.validated = []
        self.readiness = []

    def ensure_defaults(self):
        pass

    def config(self):
        return SimpleNamespace(min_media=self.min_media)

    def publication_for_offer(self, offer_id, channel_chat_id):
        return self.existing

    def unsafe_delivery_state(self, offer_id, channel_chat_id):
        return self.unsafe

    def probable_duplicate(self, listing_id, offer_id):
        return self.duplicate

    def set_item(self, offer_id, *, state, reason_code="", reason_text="", **kwargs):
        self.items.append((offer_id, state, reason_code, reason_text))

    def set_offer_readiness(self, offer_id, *, publishable, block_reason=""):
        self.readiness.append((offer_id, bool(publishable), block_reason))

    def mark_validated(self, offer_id, revision_token):
        self.validated.append((offer_id, revision_token))


def _media(tmp_path: Path, count=4, readable=True):
    cover = tmp_path / "cover.jpg"
    if readable:
        cover.write_bytes(b"cover")
    gallery = []
    for index in range(count):
        path = tmp_path / f"gallery-{index}.jpg"
        path.write_bytes(b"img")
        gallery.append(str(path))
    return SimpleNamespace(gallery_paths=tuple(gallery), cover_source_path=str(cover))


def _facts(*, blocking=()):
    return {
        "deal_type": "rent",
        "project_name": "The Peak 香格里拉",
        "public_location_display": "The Peak 香格里拉",
        "layout": "2+1",
        "bedrooms": None,
        "quality": {"blocking_flags": list(blocking)},
    }


def _item(**changes):
    item = {
        "offer_id": "OFF_1",
        "listing_id": "l_1",
        "review_id": "REV_1",
        "offer_type": "rent",
        "monthly_rent_usd": 1100,
        "project_name": "The Peak 香格里拉",
        "public_location_display": "The Peak 香格里拉",
        "layout": "2+1",
        "inventory_status": "pending",
        "revision_token": "facts:source",
    }
    item.update(changes)
    return item


def _run(tmp_path, *, item=None, facts=None, media=None, repo=None, review_status="pending", media_error=None):
    repo = repo or FakeRepo()
    workflow = FakeWorkflow(
        facts=facts or _facts(),
        media=media or _media(tmp_path),
        review_status=review_status,
        media_error=media_error,
    )
    service = ProductionAutoPublishService(workflow=workflow, repository=repo, channel_chat_id="-1001")
    result = asyncio.run(service._validate_item(item or _item()))
    return result, workflow, repo


def test_complete_pending_rental_becomes_candidate_ready_but_review_stays_pending(tmp_path):
    result, workflow, repo = _run(tmp_path)
    assert result.status == "ready"
    assert workflow._review_status == "pending"
    assert repo.readiness == [("OFF_1", True, "")]
    assert repo.items[-1][:3] == ("OFF_1", "queued", "")
    assert repo.validated == [("OFF_1", "facts:source")]


def test_missing_rent_fails_and_stays_non_publishable(tmp_path):
    result, workflow, repo = _run(tmp_path, item=_item(monthly_rent_usd=0))
    assert result.reason_code == "missing_rent"
    assert repo.readiness[-1] == ("OFF_1", False, "missing_rent")


def test_missing_layout_fails(tmp_path):
    result, workflow, repo = _run(tmp_path, item=_item(layout=""), facts={**_facts(), "layout": "", "bedrooms": None})
    assert result.reason_code == "missing_listing_info"
    assert repo.readiness[-1] == ("OFF_1", False, "missing_listing_info")


def test_canonical_blocking_flag_fails(tmp_path):
    result, workflow, repo = _run(tmp_path, facts=_facts(blocking=("ambiguous_project",)))
    assert result.reason_code == "canonical_error"
    assert repo.readiness[-1] == ("OFF_1", False, "canonical_error")


def test_insufficient_and_unreadable_media_fail(tmp_path):
    result, workflow, repo = _run(tmp_path, media=_media(tmp_path, count=3))
    assert result.reason_code == "insufficient_media"
    assert repo.readiness[-1] == ("OFF_1", False, "insufficient_media")

    other = tmp_path / "other"
    other.mkdir()
    result, workflow, repo = _run(other, media=_media(other, count=4, readable=False))
    assert result.reason_code == "unreadable_media"
    assert repo.readiness[-1] == ("OFF_1", False, "unreadable_media")


def test_sale_only_fails(tmp_path):
    result, workflow, repo = _run(tmp_path, item=_item(offer_type="sale"), facts={**_facts(), "deal_type": "sale"})
    assert result.reason_code == "sale_store_only"
    assert repo.readiness[-1] == ("OFF_1", False, "sale_store_only")


def test_existing_publication_does_not_reenter_pending(tmp_path):
    repo = FakeRepo(existing={"channel_message_id": "123"})
    result, workflow, repo = _run(tmp_path, repo=repo)
    assert result.status == "already_published"
    assert repo.readiness == []
    assert repo.items[-1][1] == "published"


def test_telegram_unknown_is_not_recovered(tmp_path):
    repo = FakeRepo(unsafe="unknown")
    result, workflow, repo = _run(tmp_path, repo=repo)
    assert result.reason_code == "telegram_unknown"
    assert repo.readiness == []
    assert repo.items[-1][1:3] == ("exception", "telegram_unknown")


def test_sync_candidates_recovers_only_stale_listing_not_publishable(tmp_path):
    import sqlite3
    from v3_core.ingest.source_repository import SourceRepository
    from v3_core.inventory.service import InventoryMaterializationService
    from v3_core.publishing.autopilot_policy import ProductionAutoPublishRepository
    from v3_core.storage.bootstrap import initialize_v3_storage
    from v3_core.storage.inventory_repository import InventoryRepository

    db = tmp_path / "sync.sqlite3"
    initialize_v3_storage(db)
    sources = SourceRepository(str(db))
    inventory = InventoryRepository(str(db))
    source_id = sources.save_source_post(
        source_id=None,
        source_type="telegram_channel",
        source_name="collector",
        source_post_id="r2-sync",
        source_url="",
        source_author="",
        raw_text="富力城 公寓 2房1厅 租金 $800/月",
        raw_images=[],
        raw_videos=[],
        raw_contact="",
        raw_meta={},
        dedupe_hash="r2-source-hash",
    )
    facts = {
        "schema_version": "canonical_facts.v1",
        "parser_revision": "v1.3",
        "canonical_facts_hash": "r2-facts-hash",
        "deal_type": "rent",
        "project_name": "富力城",
        "project_alias": "",
        "project_brand": "",
        "property_type": "公寓",
        "property_subtype": "",
        "public_location_key": "rf_city",
        "public_location_display": "富力城",
        "publication_location_level": "level_1_project_confirmed",
        "canonical_area_key": "",
        "canonical_area_display": "",
        "layout": "2房1厅",
        "bedrooms": 2,
        "bathrooms": None,
        "size_sqm": None,
        "floor": "",
        "display_title": "富力城｜2房1厅｜公寓",
        "monthly_rent_usd": 800,
        "sale_price_usd": None,
        "quality": {"score": 100, "all_flags": [], "blocking_flags": []},
    }
    canonical = inventory.store_canonical(source_post_id=source_id, facts=facts)
    materialized = InventoryMaterializationService(inventory).materialize(
        canonical_record_id=str(canonical["canonical_record_id"]),
        listing_id="l_1",
        public_listing_id="QL-RF-A2B3",
        create_review=True,
    )
    offer_id = materialized.offer_ids[0]
    repo = ProductionAutoPublishRepository(str(db))
    repo.ensure_defaults()
    repo.sync_candidates()
    repo.set_item(
        offer_id,
        state="exception",
        reason_code="listing_not_publishable",
        reason_text="房源状态不可发布",
    )
    repo.sync_candidates()
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT state,reason_code FROM publisher_auto_items_v3 WHERE offer_id=?",
            (offer_id,),
        ).fetchone()
    assert row == ("queued", "")

    repo.set_item(
        offer_id,
        state="exception",
        reason_code="missing_listing_info",
        reason_text="房源信息不完整",
    )
    repo.sync_candidates()
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT state,reason_code FROM publisher_auto_items_v3 WHERE offer_id=?",
            (offer_id,),
        ).fetchone()
    assert row == ("exception", "missing_listing_info")
