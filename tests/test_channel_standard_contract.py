from __future__ import annotations

from pathlib import Path

from publication_package import (
    _render_cover,
    approve_package,
    approved_package,
    build_package,
    ensure_source_media_assets,
    format_button_post_text,
    render_cover_preview,
)
from qiaolian_dual.channel_post import format_channel_listing_post
from qiaolian_dual.channel_status_sync import (
    APPOINTMENT_LOCK_COUNT,
    _caption_with_status,
    _keyboard,
    _status_label,
)


DIAMOND = {
    "listing_id": "l_89",
    "project": "钻石岛",
    "area": "钻石岛",
    "layout": "2房2卫",
    "property_type": "公寓",
    "size_sqm": 75,
    "floor": "18/35楼",
    "price": 680,
    "deposit": "押2付1",
    "contract_term": "1年",
    "available_date": "随时入住",
    "highlights": ["家具家电齐全", "含物业"],
    "status": "pending",
}


def test_publication_package_lifecycle_surface_is_restored():
    for fn in (
        build_package,
        approve_package,
        approved_package,
        render_cover_preview,
        _render_cover,
        ensure_source_media_assets,
    ):
        assert callable(fn)


def test_locked_caption_is_compact_mobile_layout():
    text = format_button_post_text(DIAMOND, "l_89", ["#钻石岛", "#两房", "#金边租房"])
    assert text.splitlines() == [
        "🏠 <b>钻石岛｜2房2卫</b>",
        "💰 <b>$680/月</b>",
        "",
        "🏢 公寓｜75㎡｜18/35楼",
        "🔑 押2付1｜租期1年",
        "",
        "🟢 当前可预约　QC0089",
        "",
        "#钻石岛 #两房 #租金500至1000",
    ]
    assert "面议" not in text
    assert "随时入住" not in text
    assert "家具家电" not in text
    assert "<code>" not in text
    assert "🆔" not in text
    assert "QJ" not in text
    assert "✨" not in text


def test_first_freeze_treats_empty_and_draft_status_as_active():
    for status in ("", "draft"):
        listing = dict(DIAMOND, status=status)
        text = format_button_post_text(listing, "l_89", [])
        assert "🟢 当前可预约　QC0089" in text
        assert "🔵 房态待确认" not in text


def test_missing_price_does_not_write_mianyi():
    listing = dict(DIAMOND)
    listing["price"] = 0
    text = format_channel_listing_post(listing, "l_5", status="active")
    assert "面议" not in text
    assert "💰" not in text


def test_status_sync_keeps_qc_and_drops_book_button_at_five():
    caption = format_button_post_text(DIAMOND, "l_89", [])
    locked = _caption_with_status(caption, "pending", APPOINTMENT_LOCK_COUNT, "l_89")
    assert "QC0089" in locked
    assert "🔵 已有5份预约看房，房态待确认　QC0089" in locked
    assert _status_label("pending", 2) == "🔵 房态待确认"
    markup = _keyboard("QiaolianBot", "", "l_89", "pending")
    labels = [[button.text for button in row] for row in markup.inline_keyboard]
    assert labels == [["📋 租赁详情", "📸 更多实拍"]]
    bookable = _keyboard("QiaolianBot", "", "l_89", "reserved")
    book_labels = [[button.text for button in row] for row in bookable.inline_keyboard]
    assert book_labels == [["📋 租赁详情", "📸 更多实拍"], ["📅 预约看房"]]


def test_commit_success_sql_does_not_force_every_listing_active():
    src = Path("publication_delivery.py").read_text(encoding="utf-8")
    assert "WHEN status IN ('','pending','draft') THEN 'active'" in src
    assert "UPDATE listings SET status='active',updated_at=CURRENT_TIMESTAMP" not in src
