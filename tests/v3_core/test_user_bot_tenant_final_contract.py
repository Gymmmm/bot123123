from __future__ import annotations

import sqlite3

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.lead_repository import SQLiteLeadRepository
from v3_core.storage.service_repository import SQLiteTenantServiceRepository
from v3_core.user_bot.admin_notifications import AdminNotificationResult
from v3_core.user_bot.lead_service import LeadService, LeadUser
from v3_core.user_bot.service_effects import ServiceEffectExecutor
from v3_core.user_bot.service_flow import TenantService
from v3_core.user_bot.tenant_v1 import lease_view, tenant_home_view


class CountingAdmins:
    def __init__(self):
        self.calls = []

    async def send(self, bot, notification):
        self.calls.append((bot, notification))
        return AdminNotificationResult((10,), (10,), ())


def _service(tmp_path, *, with_binding=True, unknowns=False):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    if with_binding:
        with sqlite3.connect(str(db)) as conn:
            conn.execute(
                """INSERT INTO tenant_bindings_v3
                   (user_id,binding_code,property_name,lease_end_date,rent_day,monthly_rent,
                    contract_end_date,deposit_months,status,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    123,
                    "ACTIVE-123",
                    "" if unknowns else "富力城 A3-1208",
                    "1970-01-01" if unknowns else "2027-09-30",
                    None if unknowns else 5,
                    0 if unknowns else 800,
                    "",
                    0 if unknowns else 2,
                    "active",
                    "2026-09-09 03:00:00",
                ),
            )
            conn.commit()
    return db, TenantService(SQLiteTenantServiceRepository(db))


def test_unbound_tenant_home_exposes_public_actions_only(tmp_path):
    _, service = _service(tmp_path, with_binding=False)
    view = tenant_home_view(service, 123)
    labels = [choice.label for row in view.rows for choice in row]
    assert labels == ["📄 租赁服务", "🔍 开始找房", "💬 中文顾问", "🏠 返回首页"]
    forbidden = ("租约", "报修", "物业", "续租", "退租")
    assert not any(any(word in label for word in forbidden) for label in labels)


def test_active_tenant_home_exposes_current_tenant_actions(tmp_path):
    _, service = _service(tmp_path)
    view = tenant_home_view(service, 123)
    labels = [choice.label for row in view.rows for choice in row]
    for label in ("📋 我的租约", "🔧 报修", "🏢 物业协调"):
        assert label in labels
    assert "🔄 续租" not in labels
    assert "🚪 退租" not in labels
    assert "我想换房" not in " ".join(labels)


def test_lease_unknown_values_are_never_rendered_as_null_or_1970(tmp_path):
    _, service = _service(tmp_path, unknowns=True)
    text = lease_view(service, 123).text
    assert "待补全" in text
    for forbidden in ("None", "null", "undefined", "1970"):
        assert forbidden not in text


@pytest.mark.asyncio
async def test_renewal_and_termination_are_idempotent_per_active_binding(tmp_path):
    db, service = _service(tmp_path)
    binding = service.require_active_binding(123)
    admins = CountingAdmins()
    effects = ServiceEffectExecutor(
        leads=LeadService(SQLiteLeadRepository(db)),
        admins=admins,
        now=lambda: "2026-09-16 01:00:00",
    )
    user = LeadUser(123, "alice", "Alice")

    first_renew = await effects.tenancy_request(bot=object(), user=user, binding=binding, kind="renew")
    second_renew = await effects.tenancy_request(bot=object(), user=user, binding=binding, kind="renew")
    first_terminate = await effects.tenancy_request(bot=object(), user=user, binding=binding, kind="terminate")
    second_terminate = await effects.tenancy_request(bot=object(), user=user, binding=binding, kind="terminate")

    assert first_renew.lead.status == "recorded"
    assert second_renew.lead.status == "skipped"
    assert first_terminate.lead.status == "recorded"
    assert second_terminate.lead.status == "skipped"
    assert len(admins.calls) == 2
    with sqlite3.connect(str(db)) as conn:
        rows = conn.execute("SELECT action,payload_json FROM leads_v3 WHERE user_id=123 ORDER BY id").fetchall()
    assert [row[0] for row in rows] == ["tenant_renewal_request", "tenant_termination_request"]
    assert all(f'"binding_id": {binding.id}' in row[1] for row in rows)
