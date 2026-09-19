
import json
from v3_core.user_bot.listing_responses import build_details_response, build_photos_response
from v3_core.user_bot.public_inventory import PublishedListingView


def _view(*, status="active", offer_status="active", gallery=(), adviser_copy=""):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "listing": {
            "project_name": "富力城", "property_type": "公寓", "layout": "2房1厅",
            "public_location_display": "BKK1", "size_sqm": 95, "floor": "19",
        },
        "offer": {
            "offer_type": "rent", "monthly_rent_usd": 800,
            "payment_terms": "押1付1", "contract_term": "1年",
            "publication_policy": "telegram_rent",
        },
        "adviser_copy_source": "publisher" if adviser_copy else "hidden",
        "adviser_copy": adviser_copy,
    }
    return PublishedListingView(
        listing={"listing_id":"LST_1","public_listing_id":"QL-RF-A2B3","inventory_status":status},
        offer={"offer_id":"OFF_1","offer_type":"rent","offer_status":offer_status,"publication_policy":"telegram_rent"},
        publication={"instance_id":"PUB_1"},
        package={"snapshot_json":json.dumps(snapshot,ensure_ascii=False),"gallery_json":json.dumps(list(gallery),ensure_ascii=False)},
    )


def _labels(rows):
    return [[item.label for item in row] for row in rows]


def test_details_use_clean_fields_and_frozen_adviser_copy():
    response = build_details_response(_view(adviser_copy="高层采光好。"))
    assert "<b>富力城 · 2房1厅</b>" in response.text
    assert "<b>租赁详情</b>" not in response.text
    assert "<b>$800/月</b>" in response.text
    assert "BKK1" in response.text
    assert "19楼 · 95㎡" in response.text
    assert "押1付1 · 1年" in response.text
    assert "<blockquote>高层采光好。</blockquote>" in response.text
    assert "QL-RF-A2B3" in response.text
    for icon in ("🏠","💰","📍","📐","🏢","🔑","🆔","💬"):
        assert icon not in response.text
    assert _labels(response.action_rows) == [["预约看房"], ["咨询这套"]]


def test_nonbookable_details_hide_booking_and_mark_historical_copy():
    response = build_details_response(_view(status="rented", offer_status="inactive", adviser_copy="随时可安排看房。"))
    assert "🔴 已租出" in response.text
    assert "当前状态已更新，以下为发布时的房源说明。" in response.text
    assert _labels(response.action_rows) == [["咨询这套"]]


def test_gallery_deduplicates_cover_and_splits_without_one_item_album(tmp_path):
    files=[]
    for i in range(12):
        p=tmp_path/f"room-{i}.jpg"; p.write_bytes(str(i).encode()); files.append(str(p))
    response=build_photos_response(_view(gallery=files))
    # First usable frozen asset is the P06 cover, so 11 extras remain => 9 + 2.
    assert tuple(len(group) for group in response.media_groups) == (9,2)
    assert [x for g in response.media_groups for x in g] == files[1:]
    assert "更多实拍" in response.text
    assert "QL-RF-A2B3" not in response.text
    assert _labels(response.action_rows) == [["预约看房", "咨询这套"]]


def test_gallery_missing_media_has_no_fake_actions(tmp_path):
    missing=tmp_path/"missing.jpg"
    response=build_photos_response(_view(gallery=[str(missing)]))
    assert response.media_groups == ()
    assert response.action_rows == ()
    assert "暂无更多实拍图片" in response.text
