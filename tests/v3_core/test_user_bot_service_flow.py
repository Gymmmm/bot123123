from __future__ import annotations

import sqlite3

from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.service_repository import SQLiteTenantServiceRepository
from v3_core.user_bot.service_flow import TenantService


def _insert_binding(db, *, user_id=123, property_name="富力城 A3-1208"):
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            """INSERT INTO tenant_bindings_v3
               (user_id,binding_code,property_name,status,created_at)
               VALUES (?,?,?,?,?)""",
            (user_id, "BIND-001", property_name, "active", "2026-09-09 03:00:00"),
        )
        conn.commit()


def test_bootstrap_adds_v3_service_tables_without_legacy_service_tables(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)

    with sqlite3.connect(str(db)) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

    assert "tenant_bindings_v3" in tables
    assert "repair_tickets_v3" in tables
    assert "tenant_bindings" not in tables
    assert "repair_tickets" not in tables


def test_repair_submission_reuses_same_request_token_and_keeps_binding(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _insert_binding(db)

    service = TenantService(
        SQLiteTenantServiceRepository(db),
        now=lambda: "2026-09-09 03:14:00",
    )
    draft = service.begin_request("repair_ac", request_token="req-abc")
    draft = service.with_detail(draft, "空调可以启动，但一直不制冷。")

    first = service.submit_repair(user_id=123, draft=draft, slot="tomorrow_am")
    second = service.submit_repair(user_id=123, draft=draft, slot="tomorrow_am")

    assert first.created is True
    assert second.created is False
    assert second.ticket.id == first.ticket.id
    assert first.ticket.property_name == "富力城 A3-1208"
    assert first.ticket.binding_id is not None
    assert first.ticket.issue_key == "repair_ac"
    assert first.ticket.issue_type == "空调"
    assert first.ticket.time_slot == "tomorrow_am"
    assert "明天上午" in first.ticket.description

    with sqlite3.connect(str(db)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM repair_tickets_v3").fetchone()[0]
    assert count == 1


def test_repair_submission_without_binding_still_creates_ticket(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    service = TenantService(
        SQLiteTenantServiceRepository(db),
        now=lambda: "2026-09-09 03:14:00",
    )
    draft = service.with_detail(
        service.begin_request("repair_door", request_token="req-door"),
        "门禁刷卡没有反应。",
    )

    result = service.submit_repair(user_id=456, draft=draft, slot="today")

    assert result.created is True
    assert result.urgent is True
    assert result.ticket.binding_id is None
    assert result.ticket.property_name == ""
