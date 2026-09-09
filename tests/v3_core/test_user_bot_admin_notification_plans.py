from __future__ import annotations

from v3_core.user_bot.admin_notification_plans import (
    appointment_notification,
    general_contact_notification,
    user_contact_text,
    user_mention_html,
)
from v3_core.user_bot.appointment_service import AppointmentSubmissionResult
from v3_core.user_bot.appointment_side_effects import AppointmentEffectPlan
from v3_core.user_bot.appointment_submit_executor import AppointmentSubmitExecution
from v3_core.user_bot.lead_service import LeadUser
from v3_core.user_bot.public_appointment import PublicAppointmentDraft


def _user(username="alice"):
    return LeadUser(user_id=123, username=username, display_name="Alice & Bob")


def _execution(kind="created"):
    return AppointmentSubmitExecution(
        public_listing_id="QL-RF-A2B3",
        listing_id="l_1001",
        submission=AppointmentSubmissionResult(
            kind=kind,
            appointment_id=51,
            lead_action="appointment_time_update" if kind == "updated" else "appointment_submit",
            lead_source="appointment_edit" if kind == "updated" else "listing_callback",
        ),
        effects=AppointmentEffectPlan(("record_lead", "notify_admin")),
    )


def _draft():
    return PublicAppointmentDraft(
        public_listing_id="QL-RF-A2B3",
        mode="offline",
        date="09-10",
        time="pm",
        source="listing_callback",
    )


def test_user_admin_identity_helpers_match_fixed_sha_shape():
    user = _user()
    assert user_mention_html(user) == '<a href="tg://user?id=123">Alice &amp; Bob</a>'
    assert user_contact_text(user) == "@alice"
    assert user_contact_text(_user(username="")) == "tg://user?id=123"


def test_general_contact_plan_preserves_title_and_uses_readable_source():
    note = general_contact_notification(_user(), source="hub")
    assert note.title == "用户联系我们"
    assert note.show_bell is True
    assert "用户：<a href=\"tg://user?id=123\">Alice &amp; Bob</a>" in note.lines
    assert "联系方式：@alice" in note.lines
    assert "入口：首页联系我们" in note.lines
    assert "入口：hub" not in note.lines


def test_appointment_plan_uses_public_id_fallback_and_no_bell():
    note = appointment_notification(_user(), _execution(), _draft())
    assert note.title == "📅 新预约 #51"
    assert note.show_bell is False
    assert note.lines[0] == "🏠 <b>QL-RF-A2B3</b>"
    assert "Alice &amp; Bob" in "\n".join(note.lines)
    assert "l_1001" not in note.text


def test_updated_appointment_plan_uses_locked_updated_title():
    note = appointment_notification(_user(), _execution(kind="updated"), _draft(), subject="BKK1｜1房")
    assert note.title == "📅 预约时间已修改 #51"
    assert note.lines[0] == "🏠 <b>BKK1｜1房</b>"
