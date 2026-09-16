from __future__ import annotations

from datetime import date
import json

import pytest

from v3_core.user_bot.public_appointment import PublicAppointmentDraft
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.similar_intent import SimilarSearchIntent
from v3_core.user_bot.transition_plan import (
    BookTransition,
    ChangeSearchTransition,
    SimilarTransition,
    TransitionPlan,
)
from v3_core.user_bot.transition_views import TransitionViewService


PUBLIC_ID = "QL-RF-A2B3"


class InventoryStub:
    def __init__(self, view):
        self.view = view
        self.calls = []

    def resolve(self, public_listing_id):
        self.calls.append(public_listing_id)
        return self.view


def _published_view(*, listing_status="active", offer_status="active"):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": "富力城",
            "size_sqm": 95,
            "floor": "19楼",
        },
        "offer": {
            "monthly_rent_usd": 800,
            "payment_terms": "押1付1",
            "contract_term": "1年",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": PUBLIC_ID,
            "inventory_status": listing_status,
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "publication_policy": "telegram_rent",
            "offer_status": offer_status,
        },
        publication={"instance_id": "PUB_1"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": "[]",
        },
    )


def _book_plan(*, mode="offline"):
    return TransitionPlan(
        kind="book",
        next_step="appointment_date",
        effects=("render_appointment_date",),
        book=BookTransition(
            draft=PublicAppointmentDraft(
                public_listing_id=PUBLIC_ID,
                mode=mode,
                source="listing_callback",
            )
        ),
    )


def test_book_view_matches_fixed_sha_date_entry_and_uses_public_identity_only():
    inventory = InventoryStub(_published_view())
    service = TransitionViewService(inventory)

    view = service.build(_book_plan(), today=date(2026, 9, 8))

    assert view.kind == "appointment_date"
    assert "预约看房｜QL-RF-A2B3" in view.text
    assert "富力城" in view.text
    assert "$800/月" in view.text
    assert "哪天方便看房" in view.text
    assert "LST_1" not in view.text
    assert inventory.calls == [PUBLIC_ID]
    assert [button.label for row in view.rows for button in row] == [
        "今天",
        "明天",
        "后天",
        "📅 其他日期",
        "🎥 改为视频看房",
        "⬅️ 返回租赁详情",
        "🏠 返回首页",
    ]
    assert view.rows[0][0].value == "09-08"
    assert view.rows[0][1].value == "09-09"
    assert view.rows[1][0].value == "09-10"
    assert view.rows[-1][0].public_listing_id == PUBLIC_ID


def test_video_mode_date_view_switches_fixed_sha_heading_question_and_toggle():
    service = TransitionViewService(InventoryStub(_published_view()))

    view = service.build(_book_plan(mode="video"), today=date(2026, 9, 8))

    assert "视频看房｜QL-RF-A2B3" in view.text
    assert "哪天方便视频看房" in view.text
    toggle = view.rows[2][0]
    assert toggle.label == "🚶 改为实地看房"
    assert toggle.kind == "appointment_mode"
    assert toggle.value == "offline"


def test_appointment_time_view_matches_fixed_sha_time_order():
    service = TransitionViewService(InventoryStub(_published_view()))
    draft = PublicAppointmentDraft(
        public_listing_id=PUBLIC_ID,
        mode="offline",
        date="09-10",
        source="listing_callback",
    )

    view = service.appointment_time(draft)

    assert view.kind == "appointment_time"
    assert "选择时间" in view.text
    assert "9月10日" in view.text
    assert "富力城" in view.text
    assert "LST_1" not in view.text
    assert [(row[0].label, row[0].kind, row[0].value) for row in view.rows[:4]] == [
        ("上午 09:00–12:00", "appointment_time", "am"),
        ("下午 14:00–17:00", "appointment_time", "pm"),
        ("晚上 17:00–19:00", "appointment_time", "evening"),
        ("✍️ 其他时间", "appointment_other_time", ""),
    ]
    assert [choice.label for choice in view.rows[-1]] == ["⬅️ 修改日期", "🏠 返回首页"]


def test_custom_appointment_prompts_match_fixed_sha_and_need_no_keyboard():
    service = TransitionViewService(InventoryStub(None))

    date_prompt = service.custom_date_prompt()
    time_prompt = service.custom_time_prompt()

    assert date_prompt.rows == ()
    assert date_prompt.text == (
        "📅 <b>请输入日期</b>\n\n"
        "例如：<code>0905</code>、<code>9月5日</code> 或 <code>下周三</code>"
    )
    assert time_prompt.rows == ()
    assert time_prompt.text == (
        "🕐 <b>其他时间</b>\n\n"
        "直接输入，例如：<code>20:00</code> 或 <code>晚上8点</code>"
    )


def test_book_view_rechecks_live_bookability_before_rendering_date_step():
    inventory = InventoryStub(_published_view(listing_status="rented", offer_status="inactive"))
    service = TransitionViewService(inventory)

    with pytest.raises(ValueError, match="listing_not_bookable"):
        service.build(_book_plan(), today=date(2026, 9, 8))


def test_similar_view_preserves_fixed_sha_budget_order_and_frozen_area_only():
    service = TransitionViewService(InventoryStub(None))
    plan = TransitionPlan(
        kind="similar",
        next_step="search_budget",
        effects=("render_search_budget",),
        similar=SimilarTransition(
            intent=SimilarSearchIntent(
                listing_id="LST_1",
                public_listing_id=PUBLIC_ID,
                source="similar_listing",
                goal="any",
                location_keys=("BKK1",),
                area_display="BKK1",
                next_step="budget",
            )
        ),
    )

    view = service.build(plan)

    assert view.kind == "search_budget"
    assert "每月预算大概多少" in view.text
    assert "已选：BKK1" in view.text
    assert "LST_1" not in view.text
    assert [button.label for row in view.rows for button in row] == [
        "$400以内",
        "$400–600",
        "$600–800",
        "$800–1200",
        "$1200–1500",
        "$1500+",
        "✍️ 自己输入",
        "⬅️ 返回",
    ]
    assert (view.rows[0][0].budget_min, view.rows[0][0].budget_max) == (None, 400)
    assert (view.rows[2][1].budget_min, view.rows[2][1].budget_max) == (1500, None)


def test_change_search_view_matches_locked_entry_copy_and_button_order():
    service = TransitionViewService(InventoryStub(None))
    plan = TransitionPlan(
        kind="change_search",
        next_step="search_entry",
        effects=("render_search_entry",),
        change_search=ChangeSearchTransition(),
    )

    view = service.build(plan)

    assert view.kind == "search_entry"
    assert "想找什么样的房子" in view.text
    assert "BKK1 一房，预算 $600" in view.text
    assert [button.label for row in view.rows for button in row] == [
        "📍 按区域",
        "💰 按预算",
        "🏠 按户型",
        "🏘 当前可约",
        "🏠 返回首页",
    ]


def test_consult_view_is_blocked_until_effect_executor_exists():
    service = TransitionViewService(InventoryStub(None))
    plan = TransitionPlan(
        kind="consult",
        next_step="contact_handoff",
        effects=("record_lead", "notify_admin", "render_contact_handoff"),
    )

    with pytest.raises(ValueError, match="consult_transition_requires_effect_executor"):
        service.build(plan)
