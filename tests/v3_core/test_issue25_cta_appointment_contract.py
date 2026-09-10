from __future__ import annotations

import inspect
from types import SimpleNamespace

from v3_core.publishing.channel_contract import (
    CHANNEL_CTA_LABELS,
    official_channel_action_urls,
    official_channel_button_spec,
)
from v3_core.user_bot.appointment_confirmation_view import build_appointment_confirmation_view
from v3_core.user_bot.appointment_service import AppointmentSubmissionService, AppointmentUser
from v3_core.user_bot.appointment_success_view import build_appointment_success_view
from v3_core.user_bot.appointments import AppointmentDraft
from v3_core.user_bot.home_views import build_home_view
from v3_core.user_bot.listing_responses import build_details_response, build_photos_response
from v3_core.user_bot.public_appointment import PublicAppointmentDraft
from v3_core.user_bot.search_no_match_view import build_search_no_match_view
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.telegram_start_handler import _failure_reason, _render_invalid_link
from v3_core.user_bot.transition_actions import SearchSubmitIntent


PUBLIC_ID = "QL-RF-A2B3"


class Inventory:
    def __init__(self, *, bookable=True):
        self.bookable = bookable

    def resolve(self, public_listing_id):
        if public_listing_id != PUBLIC_ID:
            return None
        return SimpleNamespace(
            snapshot={"schema": "v3_publication_snapshot.v1", "canonical_facts": {}},
            frozen_listing={
                "project_name": "富力城",
                "property_type": "公寓",
                "layout": "一房",
                "public_location_display": "富力城",
                "size_sqm": 45,
                "floor": "8",
            },
            frozen_offer={
                "monthly_rent_usd": 680,
                "payment_terms": "押1付1",
                "contract_term": "1年",
            },
            listing={"inventory_status": "active" if self.bookable else "pending"},
            listing_id="LST_INTERNAL_1",
            public_listing_id=PUBLIC_ID,
            bookable=self.bookable,
            gallery=(),
        )


class MemoryAppointments:
    def __init__(self):
        self.rows = {}
        self.next_id = 1

    def find_exact_unfinished(self, *, user_id, listing_id, mode, date, time):
        for row in self.rows.values():
            if row["status"] in {"done", "cancelled"}:
                continue
            if (row["user_id"], row["listing_id"], row["viewing_mode"], row["appointment_date"], row["appointment_time"]) == (user_id, listing_id, mode, date, time):
                return dict(row)
        return None

    def get_for_user(self, appointment_id, user_id):
        row = self.rows.get(int(appointment_id))
        return dict(row) if row and row["user_id"] == user_id else None

    def create(self, values):
        appointment_id = self.next_id
        self.next_id += 1
        self.rows[appointment_id] = {"id": appointment_id, **dict(values)}
        return appointment_id

    def update_schedule(self, **kwargs):
        return False


class Bookability:
    def is_bookable(self, listing_id):
        return True


def _labels(rows):
    return [choice.label for row in rows for choice in row]


def _published(*, bookable=True):
    return Inventory(bookable=bookable).resolve(PUBLIC_ID)


def test_channel_three_ctas_and_sync_contract_share_one_real_listing():
    urls = official_channel_action_urls("QiaoLianBot", PUBLIC_ID)
    rows = official_channel_button_spec(urls, inventory_status="active")
    assert CHANNEL_CTA_LABELS == {
        "details": "📋 租赁详情",
        "photos": "📸 更多实拍",
        "book": "📅 预约看房",
    }
    assert [[label for label, _ in row] for row in rows] == [
        ["📋 租赁详情", "📸 更多实拍"],
        ["📅 预约看房"],
    ]
    assert all(f"property_{PUBLIC_ID}_" in url for url in urls.values())
    synced = official_channel_button_spec(urls, inventory_status="reserved")
    assert synced == rows
    unbookable = official_channel_button_spec(urls, inventory_status="pending")
    assert "📅 预约看房" not in [label for row in unbookable for label, _ in row]


def test_details_and_photos_contract_has_real_fields_three_entries_and_no_internal_id():
    view = _published(bookable=True)
    details = build_details_response(view)
    labels = _labels(details.action_rows)
    assert labels == ["📅 预约看房", "📸 更多实拍", "💬 联系中文顾问"]
    assert "富力城" in details.text and "$680/月" in details.text
    assert PUBLIC_ID in details.text
    assert "LST_INTERNAL_1" not in details.text
    assert not any(token in details.text.lower() for token in ("none", "null", "unknown"))

    photos = build_photos_response(view)
    photo_labels = _labels(photos.action_rows)
    assert "📋 租赁详情" in photo_labels
    assert "📅 预约看房" in photo_labels
    assert "💬 联系中文顾问" in photo_labels
    assert "LST_INTERNAL_1" not in photos.text


def test_contact_entries_have_real_callbacks_when_external_config_is_missing():
    home = build_home_view(channel_url="")
    contact = next(choice for row in home.rows for choice in row if choice.label == "💬 联系中文顾问")
    assert contact.kind == "contact"

    intent = SearchSubmitIntent(
        criteria=SearchCriteria(location_keys=("BKK1",), budget_max=800),
        source="user_search",
        goal="any",
        area_display="BKK1",
        budget_label="$800以内",
        touch_payload={},
    )
    no_match = build_search_no_match_view(intent)
    contact = next(choice for row in no_match.rows for choice in row if choice.label == "💬 联系中文顾问")
    assert contact.kind == "home" and contact.value == "contact"


def test_deeplink_invalid_copy_and_reason_missing_compatibility_are_locked():
    source = inspect.getsource(_render_invalid_link)
    assert "这个链接已经失效或房源信息已更新。\\n\\n您可以重新找房，或直接联系我们。" in source
    assert _failure_reason(SimpleNamespace()) == ""
    assert _failure_reason(SimpleNamespace(reason="listing_not_bookable")) == "listing_not_bookable"


def test_offline_and_video_both_confirm_listing_date_time_before_submit():
    inventory = Inventory(bookable=True)
    for mode, heading, mode_text in (
        ("offline", "确认看房预约", "实地看房"),
        ("video", "确认视频看房", "视频看房"),
    ):
        draft = PublicAppointmentDraft(PUBLIC_ID, mode=mode, date="09-10", time="pm")
        view = build_appointment_confirmation_view(draft, inventory)
        assert heading in view.text
        assert "富力城" in view.text
        assert "9月10日" in view.text
        assert "下午" in view.text
        assert mode_text in view.text
        assert _labels(view.rows) == ["✅ 提交预约", "⬅️ 修改时间"]
        assert "LST_INTERNAL_1" not in view.text


def test_success_copy_requires_actual_success_and_contains_locked_phrase():
    draft = PublicAppointmentDraft(PUBLIC_ID, mode="offline", date="09-10", time="pm")
    success = build_appointment_success_view(draft, Inventory(), submission_kind="created")
    assert "预约申请已提交" in success.text
    assert "LST_INTERNAL_1" not in success.text


def test_duplicate_submission_is_idempotent_but_same_user_two_listings_are_distinct():
    repo = MemoryAppointments()
    service = AppointmentSubmissionService(repository=repo, bookability=Bookability())
    user = AppointmentUser(123, "gym", "Gym")

    first = AppointmentDraft(listing_id="LST_1", mode="offline", date="09-10", time="pm", source="channel_deeplink")
    second = AppointmentDraft(listing_id="LST_2", mode="video", date="09-10", time="pm", source="channel_deeplink")

    a = service.submit(user=user, draft=first)
    duplicate = service.submit(user=user, draft=first)
    b = service.submit(user=user, draft=second)

    assert a.kind == "created"
    assert duplicate.kind == "reused" and duplicate.appointment_id == a.appointment_id
    assert b.kind == "created" and b.appointment_id != a.appointment_id
    assert len(repo.rows) == 2
    assert {row["listing_id"] for row in repo.rows.values()} == {"LST_1", "LST_2"}
