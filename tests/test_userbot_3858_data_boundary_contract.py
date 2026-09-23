from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from qiaolian_dual.adapters.appointment import AppointmentAdapter
from qiaolian_dual.adapters.repair import RepairAdapter, persist_repair_ticket_v3, update_repair_ticket_v3_status
from qiaolian_dual import appointments_view
from qiaolian_dual.attribution_store import list_today_appointments
from qiaolian_dual.db import Database
from v3_core.storage.bootstrap import initialize_v3_storage


def _db(tmp_path):
    path = tmp_path / "combined.db"
    initialize_v3_storage(path)
    Database(path)
    return path


def test_repair_unbound_rejected_without_v3_binding_or_ticket(tmp_path):
    path = _db(tmp_path)
    dual = Database(path)
    adapter = RepairAdapter(
        get_active_binding=dual.get_active_binding,
        persist_ticket=lambda **kwargs: persist_repair_ticket_v3(path, **kwargs),
    )
    with pytest.raises(ValueError, match="tenant_binding_required"):
        adapter.create(
            user_id=7001, issue_key="repair_ac", issue_type="空调",
            description="不制冷", time_slot="today", day="2026-09-23",
            created_at="2026-09-23 10:00:00",
        )
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM repair_tickets_v3").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM tenant_bindings_v3").fetchone()[0] == 0


def test_repair_dual_binding_writes_v3_and_replay_is_idempotent(tmp_path):
    path = _db(tmp_path)
    dual = Database(path)
    binding_id = dual.create_binding(
        7002, "dual-7002", "富力城 A1208", "2027-09-23", 1,
        "2026-09-23 09:00:00",
    )
    adapter = RepairAdapter(
        get_active_binding=dual.get_active_binding,
        persist_ticket=lambda **kwargs: persist_repair_ticket_v3(path, **kwargs),
    )
    kwargs = dict(
        user_id=7002, issue_key="repair_ac", issue_type="空调",
        description="空调  不制冷", time_slot="today", day="2026-09-23",
        created_at="2026-09-23 10:00:00",
    )
    first = adapter.create(**kwargs)
    second = adapter.create(**{**kwargs, "description": "空调 不制冷", "created_at": "2026-09-23 10:05:00"})
    assert first["ticket_id"] == second["ticket_id"]
    assert first["request_token"] == second["request_token"]
    assert first["created"] is True and second["created"] is False
    with sqlite3.connect(path) as conn:
        row = conn.execute("SELECT binding_id,property_name,request_token FROM repair_tickets_v3").fetchone()
        assert row == (binding_id, "富力城 A1208", first["request_token"])
        assert conn.execute("SELECT COUNT(*) FROM tenant_bindings_v3").fetchone()[0] == 0
    assert update_repair_ticket_v3_status(path, first["ticket_id"], "accepted")["status"] == "accepted"


def test_repair_runtime_uses_adapter_and_never_v3_service_ui_or_legacy_write():
    repair = Path("qiaolian_dual/adapters/repair.py").read_text(encoding="utf-8")
    service = Path("qiaolian_dual/callback_service.py").read_text(encoding="utf-8")
    admin = Path("qiaolian_dual/callback_admin.py").read_text(encoding="utf-8")
    assert "db.create_repair_ticket(" not in service
    assert "RepairAdapter(" in service and "db.get_active_binding" in service
    assert "tenant_bindings_v3" not in repair
    assert "v3_core.user_bot" not in repair + service
    assert "adminrepairv3:" in service + admin
    assert "✅ <b>报修信息已收到</b>" in service


class Inventory:
    def __init__(self, *, published=True, bookable=True):
        self.published = published
        self.bookable = bookable

    def normalize_public_id(self, value):
        return "QL-BK-A2B3" if str(value).upper() == "QL-BK-A2B3" else None

    def get_listing(self, value):
        if not self.published:
            return None
        return {"listing_id": value, "internal_listing_id": "LST_1", "bookable": self.bookable}


def test_appointment_normalize_resolve_guards_duplicate_and_internal_persistence():
    created = []
    adapter = AppointmentAdapter(
        inventory=Inventory(),
        create_appointment=lambda payload: created.append(payload) or 10,
        list_appointments=lambda user_id, limit=20: [],
    )
    assert adapter.submit({"user_id": 8, "listing_id": "ql-bk-a2b3"}) == 10
    assert created[0]["listing_id"] == "LST_1"
    assert created[0]["status"] == "pending"

    with pytest.raises(ValueError, match="listing_not_published"):
        AppointmentAdapter(inventory=Inventory(published=False), create_appointment=lambda p: 1).submit(
            {"user_id": 8, "listing_id": "QL-BK-A2B3"}
        )
    with pytest.raises(ValueError, match="listing_not_bookable"):
        AppointmentAdapter(inventory=Inventory(bookable=False), create_appointment=lambda p: 1).submit(
            {"user_id": 8, "listing_id": "QL-BK-A2B3"}
        )
    with pytest.raises(ValueError, match="appointment_duplicate"):
        AppointmentAdapter(
            inventory=Inventory(),
            create_appointment=lambda p: 1,
            list_appointments=lambda user_id, limit=20: [{"listing_id": "LST_1", "status": "assigned"}],
        ).submit({"user_id": 8, "listing_id": "QL-BK-A2B3"})


def test_dual_appointment_status_and_admin_source_of_truth(tmp_path, monkeypatch):
    path = _db(tmp_path)
    dual = Database(path)
    appointment_id = dual.create_appointment({
        "user_id": 9001, "username": "u", "display_name": "U",
        "listing_id": "LST_1", "viewing_mode": "offline",
        "appointment_date": "2026-09-23", "appointment_time": "pm",
        "status": "pending", "created_at": "2026-09-23 10:00:00",
    })
    assert dual.update_appointment_status(appointment_id, "assigned")
    assert dual.update_appointment_status(appointment_id, "contacted")
    assert dual.list_appointments(9001, 1)[0]["status"] == "contacted"


def test_appointment_ui_and_routes_do_not_expose_internal_identity():
    view = Path("qiaolian_dual/appointments_view.py").read_text(encoding="utf-8")
    routes = Path("qiaolian_dual/start_routes.py").read_text(encoding="utf-8")
    flow = Path("qiaolian_dual/appointment_flow.py").read_text(encoding="utf-8")
    store = Path("qiaolian_dual/attribution_store.py").read_text(encoding="utf-8")
    assert "public_id_for_internal(raw) or raw" not in view
    assert "if action in {'appoint', 'book', 'video'}:" in routes
    assert "normalize_public_id(target)" in routes
    assert "persisted_listing_id" in flow
    assert 'FROM appointments a' in store
    assert 'appointments_v3" if' not in store


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("arg", "expected_mode"),
    [
        ("property_QL-BK-A2B3_book", ""),
        ("book_video_QL-BK-A2B3", "video"),
    ],
)
async def test_property_book_and_book_video_keep_ql_until_appointment_adapter(arg, expected_mode, monkeypatch):
    from types import SimpleNamespace
    from qiaolian_dual import start_routes
    import qiaolian_dual.flows as flows
    import qiaolian_dual.listing as listing_mod

    captured = {}

    async def fake_start_appointment(update, context, listing_id, **kwargs):
        captured["listing_id"] = listing_id
        captured["mode"] = kwargs.get("initial_mode", "")
        return 321

    monkeypatch.setattr(flows, "start_appointment", fake_start_appointment)
    monkeypatch.setattr(listing_mod, "listing_is_available", lambda listing_id: (True, "active"))
    monkeypatch.setattr(
        listing_mod,
        "listing_context",
        lambda listing_id: {"listing_id": listing_id, "public_listing_id": listing_id, "status": "active"},
    )
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=9100, username="tester", first_name="T"),
        effective_message=SimpleNamespace(chat_id=9100),
    )
    context = SimpleNamespace(user_data={})
    result = await start_routes.route_start_arg(
        update,
        context,
        arg,
        create_lead_fn=lambda *args, **kwargs: 1,
    )
    assert result == 321
    assert captured == {"listing_id": "QL-BK-A2B3", "mode": expected_mode}


def test_my_appointment_display_never_returns_internal_id(monkeypatch):
    monkeypatch.setattr(
        appointments_view.db,
        "list_appointments",
        lambda user_id, limit=20: [{
            "id": 1,
            "listing_id": "LST_PRIVATE_1",
            "viewing_mode": "offline",
            "appointment_date": "2026-09-23",
            "appointment_time": "pm",
            "status": "pending",
        }],
    )
    monkeypatch.setattr(appointments_view, "_appointment_public_listing_id", lambda value: "QL-BK-A2B3")
    import qiaolian_dual.listing as listing_mod
    monkeypatch.setattr(
        listing_mod,
        "listing_context",
        lambda listing_id: {"project": "富力城", "layout": "2房1厅", "property_type": "公寓"},
    )
    text = appointments_view.list_recent_appointments(1)
    assert "QL-BK-A2B3" in text
    assert "LST_PRIVATE_1" not in text


def test_admin_today_appointments_reads_dual_table_even_when_v3_exists(tmp_path, monkeypatch):
    import qiaolian_dual.attribution_store as store

    path = _db(tmp_path)
    dual = Database(path)
    dual.create_appointment({
        "user_id": 9200,
        "username": "u",
        "display_name": "U",
        "listing_id": "LST_ONLY_DUAL",
        "viewing_mode": "video",
        "appointment_date": "2026-09-23",
        "appointment_time": "pm",
        "status": "pending",
        "created_at": "2026-09-23 10:00:00",
    })
    monkeypatch.setattr(store, "db", dual)
    rows = store.list_today_appointments("2026-09-23", 20)
    assert len(rows) == 1
    assert rows[0]["user_id"] == 9200
    assert rows[0]["listing_id"] == ""
    assert "LST_ONLY_DUAL" not in str(rows[0])
