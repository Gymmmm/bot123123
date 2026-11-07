"""Offline production-controller repair integration; OCR case runs on CI's real binary.

Vision uses the recorded 6735 real-photo fixture, never a live model/Telegram.
"""

from __future__ import annotations

import asyncio
import hashlib
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from v3_core.ingest.intake_service import SourceIntake
from v3_core.media.vision import FixtureVisionClient
from v3_core.publishing.admin_bot import PublisherAdminSettings
from v3_core.publishing.publisher_app import V3PublisherApplication
from v3_core.publishing.simple_admin import NEW_LISTING_STATE_KEY
from v3_core.storage.bootstrap import initialize_v3_storage

FIXTURES = Path(__file__).parent / "fixtures" / "cover_vision"


def _application(tmp_path):
    db = tmp_path / "data" / "test.db"
    initialize_v3_storage(db)
    app = V3PublisherApplication(
        PublisherAdminSettings(
            token="TEST_OFFLINE",
            admin_ids=frozenset({7}),
            db_path=str(db),
            user_bot_username="OfflineTestBot",
            channel_chat_id="-1000000000",
            cover_output_dir=str(tmp_path / "covers"),
            advisor_url="https://t.me/offline_test",
        )
    )
    app.workflow.media.vision_client = FixtureVisionClient(FIXTURES)
    app.workflow.media.prepared_dir = tmp_path / "prepared"
    return app


def _photo(tmp_path, name):
    target = tmp_path / name
    target.write_bytes((FIXTURES / "photos" / "6735" / name).read_bytes())
    return {
        "local_path": str(target),
        "file_hash": hashlib.sha256(target.read_bytes()).hexdigest(),
    }


def _initial_exception(app, tmp_path):
    controller = (
        app.simple
    )  # Actual final production MRO, not the overridden base class.
    initial = controller.intake.persist(
        SourceIntake(
            source_type="telegram_channel",
            source_name="fixture_source",
            source_post_id="6735",
            source_url="https://t.me/c/1/6735",
            source_author="channel",
            raw_text="BKK1 独栋别墅 4房4厅 月租 $2000/月 押1付1 一年起租",
            raw_images=[_photo(tmp_path, f"{i:02}.jpg") for i in range(3, 8)],
        )
    )
    result = controller.worker.process_one(initial.source_post_pk)
    assert result.status == "materialized", result.error
    controller.repository.sync_candidates()
    with sqlite3.connect(controller.db_path) as conn:
        offer_id, review_id = conn.execute(
            "SELECT offer_id,review_id FROM publisher_auto_items_v3 WHERE listing_id=?",
            (result.listing_id,),
        ).fetchone()
    controller.repository.set_item(
        offer_id, state="exception", reason_code="villa_exterior_review_required"
    )
    return initial.source_post_pk, result.listing_id, offer_id, review_id


def _repair(app, tmp_path, offer_id):
    controller = app.simple
    message = SimpleNamespace(reply_text=AsyncMock(), reply_photo=AsyncMock())
    context = SimpleNamespace(user_data={})
    asyncio.run(controller.append_exception_media(message, context, offer_id))
    context.user_data[NEW_LISTING_STATE_KEY]["images"].append(
        _photo(tmp_path, "02.jpg")
    )
    query = SimpleNamespace(data="v3smp|manual_finish", message=message)
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=7))
    assert asyncio.run(controller.handle_callback(update, context))
    return message, context


def test_final_controller_repairs_original_source_instead_of_creating_manual_duplicate(
    tmp_path,
):
    app = _application(tmp_path)
    source_id, listing_id, offer_id, _ = _initial_exception(app, tmp_path)
    # This test isolates persistence/materialization, not media acceptance.
    app.simple.show_manual_confirmation = AsyncMock()
    _, context = _repair(app, tmp_path, offer_id)
    assert context.user_data[NEW_LISTING_STATE_KEY]["source_post_pk"] == source_id
    with sqlite3.connect(app.settings.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM source_posts").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM listings_v3").fetchone()[0] == 1
        assert (
            conn.execute("SELECT listing_id FROM listings_v3").fetchone()[0]
            == listing_id
        )
        assert conn.execute(
            "SELECT source_type,source_name,source_post_id FROM source_posts"
        ).fetchone() == ("telegram_channel", "fixture_source", "6735")
    assert len(app.simple.source_reader.source_post(source_id)["raw_images"]) == 6


@pytest.mark.skipif(
    shutil.which("tesseract") is None,
    reason="real OCR executable required; CI installs eng+chi_sim",
)
def test_villa_supplement_rechecks_real_media_and_builds_preview_without_publishing(
    tmp_path,
):
    app = _application(tmp_path)
    source_id, listing_id, offer_id, review_id = _initial_exception(app, tmp_path)
    with pytest.raises(ValueError, match="villa_exterior_review_required"):
        app.workflow.review_media(review_id=review_id)
    message, context = _repair(app, tmp_path, offer_id)
    state = context.user_data[NEW_LISTING_STATE_KEY]
    assert state["source_post_pk"] == source_id
    media = app.workflow.review_media(review_id=state["review_id"])
    assert media.cover_template == "villa_4x5"
    assert media.cover_plan[
        "hero"
    ]  # A real safe exterior was selected after supplement.
    asyncio.run(
        app.simple.prepare_manual_preview(
            message, context, review_id=state["review_id"], offer_id=state["offer_id"]
        )
    )
    assert state.get("package_id"), [
        call.args[0] for call in message.reply_text.call_args_list
    ]
    message.reply_photo.assert_awaited_once()
    with sqlite3.connect(app.settings.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM source_posts").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM listings_v3").fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM publication_instances").fetchone()[0]
            == 0
        )
        row = conn.execute(
            "SELECT state FROM publisher_auto_items_v3 WHERE offer_id=?",
            (state["offer_id"],),
        ).fetchone()
        assert row[0] == "preview_ready"
