"""Offline acceptance regressions for confirmations and independent sync writers."""

import asyncio
import importlib.util
import sqlite3
from pathlib import Path
from types import SimpleNamespace as NS
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock
import pytest
from telegram.error import BadRequest
from v3_core.publishing.publisher_app import V3PublisherApplication
from v3_core.publishing.pending_batch_admin import PENDING_BATCH_CONFIRM_KEY
from v3_core.publishing.manual_status_sync import PublisherManualStatusSynchronizer
from v3_core.user_bot.channel_status_sync import V3AppointmentChannelSynchronizer
from v3_core.user_bot.appointment_runtime_effects import (
    AppointmentRuntimeEffectExecutor,
)
from v3_core.user_bot.lead_service import LeadUser
from v3_core.user_bot.public_appointment import PublicAppointmentDraft

ROOT = Path(__file__).parent


def load_fixture_module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / ("test_" + name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


status_fixture = load_fixture_module("publisher_status_confirmation_gate")
batch_fixture = load_fixture_module("pending_batch_admin")
channel_fixture = load_fixture_module("user_bot_channel_status_sync")
effects_fixture = load_fixture_module("appointment_channel_alert_gate")


def controller(tmp_path):
    db = tmp_path / "single.sqlite"
    status_fixture._db(db)
    return db, status_fixture._controller(db)


def test_single_consumed_token_roundtrip(tmp_path):
    db, ctl = controller(tmp_path)
    token = ctl._issue_status_token("123", "l_01", "active", "offline")
    assert ctl._apply_confirmed_status(token, "123")
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE listings_v3 SET inventory_status='offline'")
    assert not ctl._apply_confirmed_status(token, "123")
    assert status_fixture._status(db) == "offline"


def test_single_wrong_admin_does_not_consume(tmp_path):
    db, ctl = controller(tmp_path)
    token = ctl._issue_status_token("123", "l_01", "active", "offline")
    assert not ctl._apply_confirmed_status(token, "999")
    assert ctl._apply_confirmed_status(token, "123")


def test_single_token_is_atomic_under_threads(tmp_path):
    db, ctl = controller(tmp_path)
    token = ctl._issue_status_token("123", "l_01", "active", "offline")
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(lambda _: ctl._apply_confirmed_status(token, "123"), range(8))
        )
    assert results.count(True) == 1
    assert status_fixture._status(db) == "active"


def test_single_unknown_token_rejected(tmp_path):
    db, ctl = controller(tmp_path)
    ctl._issue_status_token("123", "l_01", "active", "offline")
    assert not ctl._apply_confirmed_status("fake", "123")


@pytest.mark.asyncio
async def test_batch_slash_cancel_revokes_actual_state(tmp_path):
    db = tmp_path / "batch.sqlite"
    batch_fixture._build_db(db, pending_count=2)
    ctl = batch_fixture._controller(db)
    msg = batch_fixture._Message()
    ctx = NS(user_data={})
    await ctl._confirm_pending_batch(msg, ctx, "offline", 0, "123")
    token = ctx.user_data[PENDING_BATCH_CONFIRM_KEY]["token"]
    app = NS(
        _require_admin=AsyncMock(return_value=True), _home_button=lambda: [], simple=ctl
    )
    await V3PublisherApplication.cancel(
        app, NS(effective_message=msg, effective_user=NS(id=123)), ctx
    )
    assert PENDING_BATCH_CONFIRM_KEY not in ctx.user_data
    await ctl.handle_callback(
        NS(
            callback_query=NS(data="v3smp|pbcommit|" + token, message=msg),
            effective_user=NS(id=123),
        ),
        ctx,
    )
    with sqlite3.connect(db) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM listings_v3 WHERE inventory_status='pending'"
            ).fetchone()[0]
            == 2
        )


@pytest.mark.asyncio
async def test_batch_concurrent_callback_consumes_once(tmp_path):
    db = tmp_path / "batch.sqlite"
    batch_fixture._build_db(db, pending_count=2)
    ctl = batch_fixture._controller(db)
    msg = batch_fixture._Message()
    ctx = NS(user_data={})
    await ctl._confirm_pending_batch(msg, ctx, "offline", 0, "123")
    token = ctx.user_data[PENDING_BATCH_CONFIRM_KEY]["token"]
    original = ctl._apply_pending_ids
    calls = []

    def apply(*args):
        calls.append(1)
        return original(*args)

    ctl._apply_pending_ids = apply
    update = NS(
        callback_query=NS(data="v3smp|pbcommit|" + token, message=msg),
        effective_user=NS(id=123),
    )
    await asyncio.gather(
        ctl.handle_callback(update, ctx), ctl.handle_callback(update, ctx)
    )
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_not_modified_does_not_raise_admin_alert(tmp_path):
    db = tmp_path / "noop.sqlite"
    channel_fixture._seed(db, appointment_count=2)
    bot = NS(
        edit_message_caption=AsyncMock(
            side_effect=BadRequest("Message is not modified")
        )
    )
    sync = V3AppointmentChannelSynchronizer(
        db,
        publisher_bot_token="fake",
        user_bot_username="testbot",
        advisor_url="https://t.me/test",
        bot_factory=lambda _: bot,
    )
    admins = effects_fixture._Admins()
    executor = AppointmentRuntimeEffectExecutor(
        availability=None,
        channel=sync,
        admins=admins,
        inventory=effects_fixture._Inventory(),
    )
    execution = NS(
        effects=effects_fixture._Effects(),
        listing_id="l_1",
        public_listing_id="QL-RF-A2B3",
        submission=NS(appointment_id=42, kind="created"),
    )
    result = await executor.execute(
        bot=bot,
        user=LeadUser(user_id=123, username="test", display_name="Test"),
        execution=execution,
        draft=PublicAppointmentDraft(
            public_listing_id="QL-RF-A2B3", mode="offline", date="11-10", time="pm"
        ),
    )
    assert result.channel.synced
    assert result.channel_alert is None
    assert len(admins.notifications) == 1


@pytest.mark.asyncio
async def test_sync_reads_legacy_appointments_table(tmp_path):
    db = tmp_path / "legacy.sqlite"
    channel_fixture._seed(db, appointment_count=0)
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE appointments AS SELECT * FROM appointments_v3")
        conn.execute(
            "INSERT INTO appointments(user_id,listing_id,appointment_date,appointment_time,status) VALUES (123,'l_1','11-10','pm','pending')"
        )
    bot = channel_fixture.FakeBot()
    sync = V3AppointmentChannelSynchronizer(
        db,
        publisher_bot_token="fake",
        user_bot_username="testbot",
        advisor_url="https://t.me/test",
        bot_factory=lambda _: bot,
        appointment_table="appointments",
    )
    result = await sync.sync("l_1")
    assert result.synced
    assert result.active_appointment_count == 1


@pytest.mark.asyncio
async def test_delayed_user_sync_cannot_overwrite_newer_publisher_state(tmp_path):
    db = tmp_path / "race.sqlite"
    channel_fixture._seed(db, appointment_count=1, inventory_status="reserved")
    publisher = PublisherManualStatusSynchronizer(
        db, user_bot_username="testbot", advisor_url="https://t.me/test"
    )
    started = asyncio.Event()
    release = asyncio.Event()
    remote = {}

    class Bot:
        async def edit_message_caption(self, **kwargs):
            if "已有预约" in kwargs["caption"]:
                started.set()
                await release.wait()
            remote.update(kwargs)

    bot = Bot()
    user = V3AppointmentChannelSynchronizer(
        db,
        publisher_bot_token="fake",
        user_bot_username="testbot",
        advisor_url="https://t.me/test",
        bot_factory=lambda _: bot,
    )
    task = asyncio.create_task(user.sync("l_1"))
    await asyncio.wait_for(started.wait(), 2)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE listings_v3 SET inventory_status='rented' WHERE listing_id='l_1'"
        )
    await publisher.scheduled_tick(NS(bot=bot))
    assert "已租出" in remote["caption"]
    release.set()
    await task
    with sqlite3.connect(db) as conn:
        queued = conn.execute(
            "SELECT count(*) FROM publisher_status_sync_outbox_v3"
        ).fetchone()[0]
    assert queued == 1
    await publisher.scheduled_tick(NS(bot=bot))
    assert "已租出" in remote["caption"]
    with sqlite3.connect(db) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM publisher_status_sync_outbox_v3"
            ).fetchone()[0]
            == 0
        )


def test_single_cancel_return_revokes_confirmation(tmp_path):
    db, ctl = controller(tmp_path)
    msg = status_fixture._Message()
    assert status_fixture._callback(ctl, msg, "v3smp|status|active|l_01")
    old = msg.calls[-1][1]["reply_markup"].inline_keyboard[0][0].callback_data
    assert status_fixture._callback(ctl, msg, "v3smp|listing|l_01")
    assert not status_fixture._callback(ctl, msg, old)
    assert status_fixture._status(db) == "offline"


@pytest.mark.asyncio
async def test_single_slash_cancel_revokes_confirmation(tmp_path):
    db, ctl = controller(tmp_path)
    token = ctl._issue_status_token("123", "l_01", "active", "offline")
    app = NS(
        _require_admin=AsyncMock(return_value=True), _home_button=lambda: [], simple=ctl
    )
    await V3PublisherApplication.cancel(
        app,
        NS(effective_message=status_fixture._Message(), effective_user=NS(id=123)),
        NS(user_data={}),
    )
    assert not ctl._apply_confirmed_status(token, "123")
    assert status_fixture._status(db) == "offline"


def test_new_confirmation_revokes_previous_token_for_same_operator(tmp_path):
    db, ctl = controller(tmp_path)
    old = ctl._issue_status_token("123", "l_01", "active", "offline")
    new = ctl._issue_status_token("123", "l_01", "rented", "offline")
    assert not ctl._apply_confirmed_status(old, "123")
    assert ctl._apply_confirmed_status(new, "123")
    assert status_fixture._status(db) == "rented"


@pytest.mark.asyncio
async def test_home_callback_revokes_confirmation_before_compat_dispatch(
    tmp_path, monkeypatch
):
    db, ctl = controller(tmp_path)
    token = ctl._issue_status_token("123", "l_01", "active", "offline")
    app = object.__new__(V3PublisherApplication)
    app.simple = ctl
    app._require_admin = AsyncMock(return_value=True)
    fallback = AsyncMock()
    monkeypatch.setattr(
        "v3_core.publishing.admin_bot.PublisherAdminBot.on_callback", fallback
    )
    update = NS(
        callback_query=NS(data="v3h", answer=AsyncMock()), effective_user=NS(id=123)
    )
    await app.on_callback(update, NS(user_data={}))
    fallback.assert_awaited_once()
    assert not ctl._apply_confirmed_status(token, "123")
    assert status_fixture._status(db) == "offline"


@pytest.mark.asyncio
async def test_abandoned_edit_keeps_retry_until_lease_expires(tmp_path):
    db = tmp_path / "abandoned.sqlite"
    channel_fixture._seed(db, appointment_count=1, inventory_status="reserved")
    publisher = PublisherManualStatusSynchronizer(db, user_bot_username="testbot")
    user = V3AppointmentChannelSynchronizer(
        db,
        publisher_bot_token="fake",
        user_bot_username="testbot",
        advisor_url="https://t.me/test",
    )
    operation_id, _revision = user._begin_edit("l_1")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE listings_v3 SET inventory_status='rented' WHERE listing_id='l_1'"
        )
    bot = channel_fixture.FakeBot()
    await publisher.scheduled_tick(NS(bot=bot))
    assert "已租出" in bot.calls[-1]["caption"]
    with sqlite3.connect(db) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM publisher_status_sync_outbox_v3"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                "SELECT operation_id FROM channel_status_edits_inflight_v3"
            ).fetchone()[0]
            == operation_id
        )
        conn.execute("UPDATE channel_status_edits_inflight_v3 SET expires_at=0")
        conn.execute("UPDATE publisher_status_sync_outbox_v3 SET next_attempt=0")
    restarted = PublisherManualStatusSynchronizer(db, user_bot_username="testbot")
    await restarted.scheduled_tick(NS(bot=bot))
    with sqlite3.connect(db) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM publisher_status_sync_outbox_v3"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM channel_status_edits_inflight_v3"
            ).fetchone()[0]
            == 0
        )


@pytest.mark.asyncio
async def test_edit_intent_is_committed_before_telegram_and_normal_ack_clears_it(
    tmp_path,
):
    db = tmp_path / "before-io.sqlite"
    channel_fixture._seed(db, appointment_count=1)

    class IntentCheckingBot:
        async def edit_message_caption(self, **kwargs):
            with sqlite3.connect(db) as conn:
                assert (
                    conn.execute(
                        "SELECT count(*) FROM channel_status_edits_inflight_v3"
                    ).fetchone()[0]
                    == 1
                )
                assert (
                    conn.execute(
                        "SELECT count(*) FROM publisher_status_sync_outbox_v3"
                    ).fetchone()[0]
                    == 1
                )
            assert kwargs["read_timeout"] == 30
            assert kwargs["write_timeout"] == 30

    sync = V3AppointmentChannelSynchronizer(
        db,
        publisher_bot_token="fake",
        user_bot_username="testbot",
        advisor_url="https://t.me/test",
        bot_factory=lambda _: IntentCheckingBot(),
    )
    assert (await sync.sync("l_1")).synced
    with sqlite3.connect(db) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM publisher_status_sync_outbox_v3"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM channel_status_edits_inflight_v3"
            ).fetchone()[0]
            == 0
        )
