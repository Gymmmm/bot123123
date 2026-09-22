"""Production 3858 search cards must show deposit / first-payment transparency."""
from __future__ import annotations

from qiaolian_dual.results_admin import _estimated_first_payment, _find_result_card_content, _rent_usd


def test_rent_and_first_payment_helpers():
    assert _rent_usd(800) == 800
    assert _rent_usd("$1,200/月") == 1200
    assert _estimated_first_payment({"deposit": "押1付1"}, 800) == 1600
    assert _estimated_first_payment(
        {"normalized_data": {"deposit_months": 2, "prepay_months": 1}},
        900,
    ) == 2700


def test_find_result_card_shows_deposit_and_first_payment(monkeypatch):
    import qiaolian_dual.listing as listing_mod

    item = {
        "listing_id": "QL-RF-A2B3",
        "public_listing_id": "QL-RF-A2B3",
        "project": "富力城",
        "area": "富力城",
        "layout": "2房1厅",
        "property_type": "公寓",
        "price": 800,
        "deposit": "押1付1",
        "size_sqm": 95,
        "floor": "19",
        "status": "active",
    }
    monkeypatch.setattr(listing_mod, "listing_context", lambda *_: item)

    text, kb, _ = _find_result_card_content(item, 0, 1, ["QL-RF-A2B3"])
    labels = [button.text for row in kb.inline_keyboard for button in row]

    assert "押1付1｜预计首付 <b>$1,600</b>" in text
    assert "💬 咨询这套" in labels
    assert "联系我们" not in " ".join(labels)
