from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from v3_core.publishing.inventory_dashboard_ui import PublisherInventoryDashboardController
from v3_core.publishing.publisher_adviser_ui import PublisherAdviserAdminController
from v3_core.publishing.simple_admin import NEW_LISTING_STATE_KEY


class _Message:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def reply_text(self, text, **kwargs):
        self.calls.append({"kind": "text", "text": text, **kwargs})

    async def reply_photo(self, photo, **kwargs):
        self.calls.append({"kind": "photo", **kwargs})


class _ManualRepository:
    def __init__(self) -> None:
        self.status = "pending"
        self.items: list[dict] = []

    def set_listing_status(self, listing_id: str, status: str) -> None:
        clean = str(status).strip().lower()
        if clean not in {"active", "reserved", "rented", "inactive", "offline"}:
            raise ValueError("unsupported listing status")
        self.status = clean

    def mark_listing_pending_for_auto_publish(self, listing_id: str) -> None:
        if self.status in {"active", "reserved"}:
            self.status = "pending"

    def set_item(self, offer_id: str, **kwargs) -> None:
        self.items.append({"offer_id": offer_id, **kwargs})


class _ManualWorkflow:
    def __init__(self, cover_path: Path) -> None:
        self.review = {"review_id": "REV_1", "review_status": "pending"}
        self.listing = {
            "listing_id": "l_manual",
            "public_listing_id": "QL-RF-TEST",
            "inventory_status": "pending",
            "layout": "1房1厅",
        }
        self.cover_path = cover_path
        self.package_status = ""
        self.publication_count = 0
        self.channel_message_id = None
        self.delivery = object()

    def review_detail(self, review_id: str):
        return SimpleNamespace(
            review=dict(self.review),
            listing=dict(self.listing),
            offer={"offer_id": "OFF_1", "listing_id": "l_manual"},
            canonical={"facts": {}},
        )

    def review_media(self, *, review_id: str, manual_cover_path=None):
        return SimpleNamespace(
            cover_source_path=str(self.cover_path),
            gallery_paths=(str(self.cover_path),),
            ranking=({"file": str(self.cover_path), "reject": False},),
        )

    def approve_review(self, *, review_id: str, operator_user_id: str):
        self.review["review_status"] = "approved"
        return self.review_detail(review_id)

    def build_package_for_review(self, **kwargs):
        self.package_status = "package_ready"
        return SimpleNamespace(
            package_id="PKG_1",
            listing_id="l_manual",
            cover_path=str(self.cover_path),
            post_text="preview",
            status="package_ready",
        )

    def package(self, package_id: str):
        return SimpleNamespace(package_id=package_id, listing_id="l_manual", offer_id="OFF_1")

    def approve_package(self, *, package_id: str, approved_by: str):
        self.package_status = "approved"
        return SimpleNamespace(
            package_id=package_id,
            listing_id="l_manual",
            offer_id="OFF_1",
            cover_path=str(self.cover_path),
            post_text="preview",
            status="approved",
        )


@pytest.mark.asyncio
async def test_pending_manual_preview_restores_pending_and_only_send_activates(tmp_path: Path, monkeypatch):
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    workflow = _ManualWorkflow(cover)
    repository = _ManualRepository()
    controller = object.__new__(PublisherAdviserAdminController)
    controller.workflow = workflow
    controller.repository = repository
    controller.channel_chat_id = "-100123"

    async def no_blockers(detail):
        return [], SimpleNamespace(cover_source_path=str(cover), gallery_paths=(str(cover),), ranking=({"file": str(cover), "reject": False},))

    controller._manual_blockers = no_blockers
    message = _Message()
    context = SimpleNamespace(
        user_data={NEW_LISTING_STATE_KEY: {"review_id": "REV_1", "offer_id": "OFF_1"}},
        bot=object(),
    )

    await controller.prepare_manual_preview(
        message,
        context,
        review_id="REV_1",
        offer_id="OFF_1",
    )

    assert workflow.review["review_status"] == "approved"
    assert workflow.package_status == "package_ready"
    assert repository.status == "pending"
    assert repository.items[-1]["state"] == "preview_ready"
    assert workflow.publication_count == 0
    assert workflow.channel_message_id is None
    assert context.user_data[NEW_LISTING_STATE_KEY]["package_id"] == "PKG_1"
    assert message.calls[-1]["kind"] == "photo"
    preview_markup = message.calls[-1]["reply_markup"]
    callbacks = [button.callback_data for row in preview_markup.inline_keyboard for button in row if button.callback_data]
    assert callbacks == [
        "v3smp|manual_send",
        "v3smp|manual_adviser",
        "v3smp|manual_cover",
        "v3smp|manual_templates",
        "v3smp|manual_back_confirm",
    ]
    assert all(len(data.encode("utf-8")) <= 64 for data in callbacks)

    async def fake_deliver(**kwargs):
        workflow.publication_count += 1
        workflow.channel_message_id = "777"
        return SimpleNamespace(publication=SimpleNamespace(channel_message_id="777"))

    monkeypatch.setattr("v3_core.publishing.publisher_adviser_ui.deliver_approved_package", fake_deliver)
    callback_message = _Message()
    update = SimpleNamespace(
        callback_query=SimpleNamespace(
            data="v3smp|manual_send",
            message=callback_message,
        )
    )

    assert await controller.handle_callback(update, context) is True
    assert workflow.package_status == "approved"
    assert repository.status == "active"
    assert workflow.publication_count == 1
    assert workflow.channel_message_id == "777"
    assert repository.items[-1]["state"] == "published"

    assert await controller.handle_callback(update, context) is True
    assert workflow.publication_count == 1


@pytest.mark.asyncio
async def test_manual_preview_short_callbacks_recover_real_length_session_ids(tmp_path: Path, monkeypatch):
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    workflow = _ManualWorkflow(cover)
    repository = _ManualRepository()
    controller = object.__new__(PublisherAdviserAdminController)
    controller.workflow = workflow
    controller.repository = repository
    controller.channel_chat_id = "-100123"
    real_ids = {
        "listing_id": "l_273",
        "review_id": "REV_" + "b" * 32,
        "offer_id": "OFF_" + "6" * 32,
        "package_id": "PKG3_" + "8" * 32,
        "mode": "preview",
    }
    workflow.review["review_id"] = real_ids["review_id"]
    workflow.listing["listing_id"] = real_ids["listing_id"]
    workflow.review_detail = lambda review_id: SimpleNamespace(
        review={"review_id": review_id, "review_status": "approved"},
        listing={"listing_id": real_ids["listing_id"], "inventory_status": "pending"},
        offer={"offer_id": real_ids["offer_id"], "listing_id": real_ids["listing_id"]},
        canonical={"facts": {}},
    )
    workflow.package = lambda package_id: SimpleNamespace(
        package_id=package_id, listing_id=real_ids["listing_id"], offer_id=real_ids["offer_id"]
    )
    calls = []
    async def fake_preview(message, context, **kwargs):
        calls.append(kwargs)
    controller.prepare_manual_preview = fake_preview
    context = SimpleNamespace(user_data={NEW_LISTING_STATE_KEY: dict(real_ids)}, bot=object())

    cover_message = _Message()
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_cover", message=cover_message))
    assert await controller.handle_callback(update, context) is True
    cover_callbacks = [
        b.callback_data
        for call in cover_message.calls if call.get("kind") == "photo"
        for row in call["reply_markup"].inline_keyboard for b in row if b.callback_data
    ]
    assert cover_callbacks == ["v3smp|manual_cover_pick|0"]
    assert all(len(data.encode("utf-8")) <= 64 for data in cover_callbacks)

    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_style|black_gold", message=_Message()))
    assert await controller.handle_callback(update, context) is True
    assert calls[-1] == {"review_id": real_ids["review_id"], "offer_id": real_ids["offer_id"], "style": "black_gold"}

    templates_message = _Message()
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_templates", message=templates_message))
    assert await controller.handle_callback(update, context) is True
    template_callbacks = [b.callback_data for row in templates_message.calls[-1]["reply_markup"].inline_keyboard for b in row if b.callback_data]
    assert all(len(data.encode("utf-8")) <= 64 for data in template_callbacks)
    assert "v3smp|manual_style|black_gold" in template_callbacks

    expired = SimpleNamespace(user_data={}, bot=object())
    expired_message = _Message()
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_cover", message=expired_message))
    assert await controller.handle_callback(update, expired) is True
    assert calls[-1] == {"review_id": real_ids["review_id"], "offer_id": real_ids["offer_id"], "style": "black_gold"}
    assert "已经失效" in expired_message.calls[-1]["text"]

    old = [
        f"v3smp|manual_send|{real_ids['package_id']}|{real_ids['offer_id']}",
        f"v3smp|manual_cover|{real_ids['review_id']}|{real_ids['offer_id']}",
        f"v3smp|manual_templates|{real_ids['review_id']}|{real_ids['offer_id']}",
    ]
    assert [len(x.encode("utf-8")) for x in old] == [92, 92, 96]

    sent = []
    async def fake_deliver(**kwargs):
        sent.append(kwargs["package_id"])
        return SimpleNamespace(publication=SimpleNamespace(channel_message_id="9001"))
    monkeypatch.setattr("v3_core.publishing.publisher_adviser_ui.deliver_approved_package", fake_deliver)
    workflow.approve_package = lambda *, package_id, approved_by: SimpleNamespace(
        package_id=package_id, listing_id=real_ids["listing_id"], offer_id=real_ids["offer_id"]
    )
    send_context = SimpleNamespace(user_data={NEW_LISTING_STATE_KEY: dict(real_ids)}, bot=object())
    send_update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|manual_send", message=_Message()))
    assert await controller.handle_callback(send_update, send_context) is True
    assert sent == [real_ids["package_id"]]
    assert repository.items[-1]["offer_id"] == real_ids["offer_id"]
    assert repository.items[-1]["package_id"] == real_ids["package_id"]


def _build_pending_dashboard_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE listings_v3 (
                listing_id TEXT PRIMARY KEY,
                public_listing_id TEXT,
                display_title TEXT,
                project_name TEXT,
                inventory_status TEXT,
                updated_at TEXT
            );
            CREATE TABLE listing_offers (
                offer_id TEXT PRIMARY KEY,
                listing_id TEXT,
                offer_type TEXT,
                offer_status TEXT,
                monthly_rent_usd REAL,
                publication_policy TEXT,
                publishable INTEGER,
                publish_block_reason TEXT
            );
            CREATE TABLE publication_instances (
                id INTEGER PRIMARY KEY,
                listing_id TEXT,
                offer_id TEXT,
                platform TEXT,
                publish_status TEXT,
                published_at TEXT
            );
            CREATE TABLE publication_packages_v3 (
                id INTEGER PRIMARY KEY,
                package_id TEXT,
                listing_id TEXT,
                offer_id TEXT,
                status TEXT
            );
            CREATE TABLE publisher_auto_items_v3 (
                offer_id TEXT PRIMARY KEY,
                listing_id TEXT,
                state TEXT
            );
            """
        )
        for index in range(1, 18):
            listing_id = f"l_{index:02d}"
            conn.execute(
                "INSERT INTO listings_v3 VALUES (?,?,?,?,?,?)",
                (listing_id, f"QL-{index:04d}", f"房源{index}", "测试项目", "pending", f"2026-09-16 00:{index:02d}:00"),
            )
            conn.execute(
                "INSERT INTO listing_offers VALUES (?,?,?,?,?,?,?,?)",
                (f"off_{index:02d}", listing_id, "rent", "active", 500 + index, "telegram_rent", 1, ""),
            )

        conn.execute("UPDATE listing_offers SET publishable=0,publish_block_reason='missing_layout' WHERE offer_id='off_12'")
        conn.execute(
            "INSERT INTO publication_instances(listing_id,offer_id,platform,publish_status,published_at) VALUES ('l_13','off_13','telegram','published','2026-09-16 01:00:00')"
        )
        conn.execute("UPDATE listing_offers SET offer_type='sale',publication_policy='store_only',publishable=0 WHERE offer_id='off_14'")
        conn.execute("INSERT INTO publisher_auto_items_v3 VALUES ('off_15','l_15','exception')")
        conn.execute("UPDATE listing_offers SET offer_status='inactive' WHERE offer_id='off_16'")
        conn.execute("UPDATE listing_offers SET publication_policy='store_only' WHERE offer_id='off_17'")
        conn.commit()


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


@pytest.mark.asyncio
async def test_dashboard_pending_uses_existing_eligible_ten_at_a_time_queue(tmp_path: Path):
    db = tmp_path / "dashboard.sqlite3"
    _build_pending_dashboard_db(db)
    controller = object.__new__(PublisherInventoryDashboardController)
    controller.db_path = str(db)
    message = _Message()

    raw_pending = sqlite3.connect(db).execute(
        "SELECT COUNT(*) FROM listings_v3 WHERE inventory_status='pending'"
    ).fetchone()[0]
    assert raw_pending == 17
    assert controller._pending_count() == 11

    await controller.show_listing_categories(message)
    labels = _labels(message.calls[-1]["reply_markup"])
    callbacks = _callbacks(message.calls[-1]["reply_markup"])
    pending_label = next(label for label in labels if label.startswith("🔵 待确认 "))
    assert pending_label == "🔵 待确认 11｜10套一组"
    assert "🔵 待确认 17" not in labels
    assert callbacks[0] == "v3smp|pbat|0"

    first = controller._pending_batch_rows(0)
    second = controller._pending_batch_rows(1)
    assert [row["listing_id"] for row in first] == [f"l_{i:02d}" for i in range(1, 11)]
    assert [row["listing_id"] for row in second] == ["l_11"]
    assert not ({"l_12", "l_13", "l_14", "l_15", "l_16", "l_17"} & {row["listing_id"] for row in first + second})

    callback_message = _Message()
    update = SimpleNamespace(callback_query=SimpleNamespace(data="v3smp|inv_rows|pending", message=callback_message))
    context = SimpleNamespace(user_data={})
    assert await controller.handle_callback(update, context) is True
    assert "待确认共 11 套" in callback_message.calls[-1]["text"]
    assert "第 1 组｜1–10" in callback_message.calls[-1]["text"]
