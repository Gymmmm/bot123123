from __future__ import annotations

import pytest

from v3_core.user_bot.admin_notifications import AdminNotificationResult
from v3_core.user_bot.contact_effects import ContactEffectExecutor
from v3_core.user_bot.lead_effects import LeadEffectResult
from v3_core.user_bot.lead_service import LeadUser


class FakeLeadEffects:
    def __init__(self, status="recorded"):
        self.status = status
        self.calls = []

    def record_general_contact(self, *, user, source="hub"):
        self.calls.append((user, source))
        return LeadEffectResult(status=self.status, error="lead_failed" if self.status == "failed" else "")


class FakeAdmins:
    def __init__(self, failed=()):
        self.failed = tuple(failed)
        self.calls = []

    async def send(self, bot, notification):
        self.calls.append((bot, notification))
        return AdminNotificationResult(
            attempted_admin_ids=(10, 20),
            sent_admin_ids=tuple(value for value in (10, 20) if value not in self.failed),
            failed_admin_ids=self.failed,
        )


def _user():
    return LeadUser(user_id=123, username="alice", display_name="Alice")


@pytest.mark.asyncio
async def test_general_contact_runs_lead_then_admin_and_returns_both_results():
    leads = FakeLeadEffects()
    admins = FakeAdmins()
    effects = ContactEffectExecutor(leads=leads, admins=admins)
    bot = object()

    result = await effects.execute_general(bot=bot, user=_user(), source="hub")

    assert result.lead.status == "recorded"
    assert result.admin.ok
    assert leads.calls[0][1] == "hub"
    note = admins.calls[0][1]
    assert note.title == "用户联系我们"
    assert "@alice" in note.text


@pytest.mark.asyncio
async def test_general_contact_keeps_admin_delivery_even_if_lead_failed():
    leads = FakeLeadEffects(status="failed")
    admins = FakeAdmins(failed=(20,))
    effects = ContactEffectExecutor(leads=leads, admins=admins)

    result = await effects.execute_general(bot=object(), user=_user(), source="menu")

    assert result.lead.status == "failed"
    assert result.admin.sent_admin_ids == (10,)
    assert result.admin.failed_admin_ids == (20,)
    assert len(admins.calls) == 1
