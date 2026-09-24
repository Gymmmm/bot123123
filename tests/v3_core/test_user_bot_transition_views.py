
from datetime import date
import json
import pytest
from v3_core.user_bot.public_appointment import PublicAppointmentDraft
from v3_core.user_bot.appointment_confirmation_view import build_appointment_confirmation_view
from v3_core.user_bot.appointment_success_view import build_appointment_success_view
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.similar_intent import SimilarSearchIntent
from v3_core.user_bot.transition_plan import BookTransition, ChangeSearchTransition, SimilarTransition, TransitionPlan
from v3_core.user_bot.transition_views import TransitionViewService

PUBLIC_ID="QL-RF-A2B3"

class InventoryStub:
    def __init__(self, view): self.view=view; self.calls=[]
    def resolve(self, public_listing_id): self.calls.append(public_listing_id); return self.view

def _published_view(*, listing_status="active", offer_status="active"):
    snapshot={"schema":"v3_publication_snapshot.v1","listing":{"project_name":"富力城","layout":"2房1厅","public_location_display":"富力城","size_sqm":95,"floor":"19楼"},"offer":{"monthly_rent_usd":800,"payment_terms":"押1付1","contract_term":"1年"}}
    return PublishedListingView(
        listing={"listing_id":"LST_1","public_listing_id":PUBLIC_ID,"inventory_status":listing_status},
        offer={"offer_id":"OFF_1","offer_type":"rent","publication_policy":"telegram_rent","offer_status":offer_status},
        publication={"instance_id":"PUB_1"},
        package={"snapshot_json":json.dumps(snapshot,ensure_ascii=False),"gallery_json":"[]"},
    )

def _book_plan():
    return TransitionPlan(kind="book",next_step="appointment_mode",effects=("render_appointment_mode",),book=BookTransition(draft=PublicAppointmentDraft(public_listing_id=PUBLIC_ID)))

def _labels(view): return [c.label for row in view.rows for c in row]

def test_booking_starts_with_mode_surface():
    service=TransitionViewService(InventoryStub(_published_view()))
    view=service.build(_book_plan(),today=date(2026,9,19))
    assert view.kind=="appointment_mode"
    assert "预约看房" in view.text
    assert "富力城" in view.text
    assert "🏡 富力城｜2房1厅" in view.text
    assert "💵 $800/月" in view.text
    assert "之后再选日期和时间" in view.text
    assert _labels(view)==["🚶 实地看房","🎥 视频代看","⬅️ 返回房源"]
    assert "LST_1" not in view.text

def test_date_and_time_steps_are_light_and_supported():
    service=TransitionViewService(InventoryStub(_published_view()))
    draft=PublicAppointmentDraft(public_listing_id=PUBLIC_ID,mode="offline")
    date_view=service.appointment_date(draft,today=date(2026,9,19))
    assert "🏡 富力城｜2房1厅" in date_view.text
    assert _labels(date_view)==["今天 · 9月19日","明天 · 9月20日","后天 · 9月21日","其他日期","⬅️ 返回上一步"]
    timed=draft.with_date("09-19")
    time_view=service.appointment_time(timed)
    assert "🏡 富力城｜2房1厅" in time_view.text
    assert _labels(time_view)==["上午 09:00–12:00","下午 14:00–17:00","🕐 其他时间","⬅️ 返回上一步"]
    assert "晚上" not in time_view.text + repr(_labels(time_view))


def test_villa_without_project_keeps_location_and_layout_through_booking():
    published = _published_view()
    snapshot = published.snapshot
    snapshot["listing"].update(project_name="", layout="6+2房8卫", public_location_display="50米路附近")
    snapshot["offer"]["monthly_rent_usd"] = 5000
    published.package["snapshot_json"] = json.dumps(snapshot, ensure_ascii=False)
    inventory = InventoryStub(published)
    service = TransitionViewService(inventory)
    draft = PublicAppointmentDraft(public_listing_id=PUBLIC_ID)
    ready = draft.with_date("09-25").with_time("pm")

    for view in (
        service.appointment_mode(draft),
        service.appointment_date(draft, today=date(2026, 9, 24)),
        service.appointment_time(draft.with_date("09-25")),
        build_appointment_confirmation_view(ready, inventory),
        build_appointment_success_view(ready, inventory),
    ):
        assert "50米路附近｜6+2房8卫" in view.text
        assert "LST_1" not in view.text
    assert "💵 $5,000/月" in service.appointment_mode(draft).text
    assert "顾问会联系你确认" in build_appointment_confirmation_view(ready, inventory).text

def test_bookability_rechecked_before_booking_surface():
    service=TransitionViewService(InventoryStub(_published_view(listing_status="rented",offer_status="inactive")))
    with pytest.raises(ValueError,match="listing_not_bookable"):
        service.build(_book_plan())

def test_search_entry_is_final_direct_filter_panel():
    service=TransitionViewService(InventoryStub(None))
    plan=TransitionPlan(kind="change_search",next_step="search_entry",effects=("render_search_entry",),change_search=ChangeSearchTransition())
    view=service.build(plan)
    assert _labels(view)==["📍 按区域","💰 按预算","🏠 按户型","💬 中文顾问","⬅️ 返回首页"]
    assert "BKK1 一房，预算 $600" in view.text
    assert "富力城两房，要能做饭" in view.text
    assert "钻石岛公寓，想看实拍" in view.text

def test_similar_returns_to_same_search_panel():
    service=TransitionViewService(InventoryStub(None))
    plan=TransitionPlan(kind="similar",next_step="search_entry",effects=(),similar=SimilarTransition(intent=SimilarSearchIntent(listing_id="LST_1",public_listing_id=PUBLIC_ID,source="similar_listing",goal="any",location_keys=("BKK1",),area_display="BKK1",next_step="budget")))
    assert service.build(plan).kind=="search_entry"
