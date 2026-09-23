from __future__ import annotations

import json
from pathlib import Path
import sqlite3

from v3_core.user_bot.legacy_routes import legacy_home_action
from v3_core.user_bot.takeover_storage import (
    ProductionAdminAppointmentReader,
    ProductionAppointmentHistoryReader,
    ProductionAppointmentRepository,
    ProductionLeadRepository,
    ProductionTenantServiceRepository,
)


def _db(tmp_path):
    path = tmp_path / "takeover.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE listings_v3(
                listing_id TEXT PRIMARY KEY,
                public_listing_id TEXT,
                display_title TEXT,
                project_name TEXT,
                inventory_status TEXT,
                updated_at TEXT
            );
            INSERT INTO listings_v3 VALUES
                ('l_15','QL-PP-D2W2','炳发城 4房5卫','炳发城','active','2026-09-23 00:00:00');

            CREATE TABLE appointments(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '',
                listing_id TEXT NOT NULL DEFAULT '',
                viewing_mode TEXT NOT NULL DEFAULT '',
                appointment_date TEXT NOT NULL DEFAULT '',
                appointment_time TEXT NOT NULL DEFAULT '',
                contact_value TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL
            );
            CREATE TABLE appointments_v3(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER, listing_id TEXT, status TEXT, created_at TEXT
            );

            CREATE TABLE leads(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER, username TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '', source TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL DEFAULT '', listing_id TEXT NOT NULL DEFAULT '',
                area TEXT NOT NULL DEFAULT '', property_type TEXT NOT NULL DEFAULT '',
                budget_min INTEGER, budget_max INTEGER,
                payload_json TEXT NOT NULL DEFAULT '{}', message_id INTEGER,
                post_token TEXT NOT NULL DEFAULT '', caption_variant TEXT NOT NULL DEFAULT '',
                agent_id TEXT NOT NULL DEFAULT '', response_at TEXT NOT NULL DEFAULT '',
                conversion_value REAL NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
                advisor_id TEXT NOT NULL DEFAULT '', advisor_name TEXT NOT NULL DEFAULT '',
                assigned_at TEXT NOT NULL DEFAULT '', intent TEXT NOT NULL DEFAULT '',
                lead_status TEXT NOT NULL DEFAULT 'new',
                source_type TEXT NOT NULL DEFAULT '', source_detail TEXT NOT NULL DEFAULT '',
                first_source_type TEXT NOT NULL DEFAULT '', first_source_detail TEXT NOT NULL DEFAULT '',
                first_entry_at TEXT NOT NULL DEFAULT '', latest_touch_at TEXT NOT NULL DEFAULT '',
                entry_action TEXT NOT NULL DEFAULT '', deep_link_payload TEXT NOT NULL DEFAULT '',
                channel_message_id INTEGER
            );
            CREATE TABLE leads_v3(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER);

            CREATE TABLE tenant_bindings(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL, binding_code TEXT NOT NULL,
                property_name TEXT NOT NULL DEFAULT '', lease_end_date TEXT NOT NULL DEFAULT '',
                rent_day INTEGER, monthly_rent REAL NOT NULL DEFAULT 0,
                contract_start_date TEXT NOT NULL DEFAULT '', contract_end_date TEXT NOT NULL DEFAULT '',
                deposit_months INTEGER NOT NULL DEFAULT 2, contract_notes TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL
            );
            CREATE TABLE tenant_bindings_v3(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL, binding_code TEXT NOT NULL,
                property_name TEXT NOT NULL DEFAULT '', lease_end_date TEXT NOT NULL DEFAULT '',
                rent_day INTEGER, monthly_rent REAL NOT NULL DEFAULT 0,
                contract_start_date TEXT NOT NULL DEFAULT '', contract_end_date TEXT NOT NULL DEFAULT '',
                deposit_months INTEGER NOT NULL DEFAULT 2, contract_notes TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL
            );
            CREATE TABLE repair_tickets_v3(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_token TEXT NOT NULL UNIQUE, user_id INTEGER NOT NULL,
                binding_id INTEGER, property_name TEXT NOT NULL DEFAULT '',
                issue_key TEXT NOT NULL DEFAULT '', issue_type TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '', time_slot TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'new', created_at TEXT NOT NULL
            );

            INSERT INTO tenant_bindings(
                user_id,binding_code,property_name,status,created_at
            ) VALUES (123,'B1','富力城 B2-2904','active','2026-09-23 00:00:00');
            INSERT INTO tenant_bindings_v3(
                user_id,binding_code,property_name,status,created_at
            ) VALUES (123,'B2','错误 V3 绑定','active','2026-09-23 00:00:00');
            """
        )
    return path


def test_takeover_appointment_write_and_history_use_production_table(tmp_path):
    path = _db(tmp_path)
    repo = ProductionAppointmentRepository(path)
    appointment_id = repo.create(
        {
            "user_id": 123,
            "username": "alice",
            "display_name": "Alice",
            "listing_id": "l_15",
            "viewing_mode": "offline",
            "appointment_date": "09-24",
            "appointment_time": "pm",
            "contact_value": "@alice",
            "status": "pending",
            "created_at": "2026-09-23 12:00:00",
        }
    )
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM appointments").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM appointments_v3").fetchone()[0] == 0

    records = ProductionAppointmentHistoryReader(path).list_for_user(123)
    assert records[0].appointment_id == appointment_id
    assert records[0].public_listing_id == "QL-PP-D2W2"

    admin = ProductionAdminAppointmentReader(path)
    assert admin.get(appointment_id)["public_listing_id"] == "QL-PP-D2W2"


def test_takeover_leads_write_existing_advisor_console_table(tmp_path):
    path = _db(tmp_path)
    repo = ProductionLeadRepository(path)
    lead_id = repo.create(
        {
            "user_id": 123,
            "username": "alice",
            "display_name": "Alice",
            "source": "channel_listing",
            "action": "appointment_submit",
            "listing_id": "QL-PP-D2W2",
            "payload": {"start_payload": "property_QL-PP-D2W2_book"},
            "created_at": "2026-09-23 12:01:00",
        }
    )
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
        assert row["listing_id"] == "QL-PP-D2W2"
        assert row["source_type"] == "channel_listing"
        assert row["entry_action"] == "appointment_submit"
        assert row["deep_link_payload"] == "property_QL-PP-D2W2_book"
        assert conn.execute("SELECT COUNT(*) FROM leads_v3").fetchone()[0] == 0


def test_takeover_tenant_service_reads_live_binding_not_v3_shadow(tmp_path):
    path = _db(tmp_path)
    binding = ProductionTenantServiceRepository(path).get_active_binding(123)
    assert binding is not None
    assert binding.property_name == "富力城 B2-2904"


def test_takeover_legacy_home_callbacks_cover_old_six_button_messages():
    expected = {
        "home": "home",
        "home_smart_search": "search",
        "home_brand": "rental",
        "home_appoint": "book",
        "home_consult": "contact",
        "home_living": "service",
        "home_nearby": "local",
        "appointment_menu:list": "appointments",
        "appointment_menu:contact": "contact",
        "service:hub": "service",
    }
    assert {key: legacy_home_action(key) for key in expected} == expected


def test_takeover_entry_wires_single_scheduler_and_compatibility_bridges():
    source = Path("run_v3_user_bot.py").read_text(encoding="utf-8")
    app = Path("v3_core/user_bot/app.py").read_text(encoding="utf-8")
    assert "build_takeover_user_bot_dependencies" in source
    assert "ProductionAdminAppointmentReader" in source
    assert "compat_start_handler=handle_legacy_start" in source
    assert "compat_callback_handler=handle_legacy_callback" in source
    assert "lease_reminder_handler=lease_reminder_job" in source
    assert 'CommandHandler("favorites", find)' in app
    assert 'name="lease_reminder_job"' in app
