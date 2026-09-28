"""Rent-primary mixed listings, project→location fill, spoken 侨联说."""
from __future__ import annotations

from v3_core.adviser_copy import build_adviser_copy
from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.publishing.autopilot import AutoPublishService


def test_mixed_with_rent_does_not_block_quality() -> None:
    facts = canonicalize_source(
        "炳发城 一号路\n4房别墅 可出租也可出售\n租金：$1300/月\n售价：$220000"
    )
    assert facts["deal_type"] == "mixed"
    assert facts["monthly_rent_usd"] == 1300
    assert "mixed_sale_rent_terms" in facts["quality"]["warning_flags"]
    assert "mixed_sale_rent_terms" not in facts["quality"]["blocking_flags"]
    assert facts["public_location_display"]


def test_autopilot_ignores_legacy_mixed_blocking_flag_when_rent_known() -> None:
    flags = AutoPublishService._critical_quality_flags(
        {
            "monthly_rent_usd": 1300,
            "quality": {"blocking_flags": ["mixed_sale_rent_terms", "missing_layout"]},
        }
    )
    assert "mixed_sale_rent_terms" not in flags
    assert "missing_layout" in flags


def test_太子寰宇_without_explicit_location_gets_customer_anchor() -> None:
    facts = canonicalize_source("太子寰宇 出租 2房1卫 租金800$")
    assert facts["project_name"] == "太子·寰宇中心"
    assert facts["public_location_display"]
    assert "金界" in facts["public_location_display"] or "永旺" in facts["public_location_display"]
    assert "missing_public_location" not in facts["quality"]["blocking_flags"]


def test_common_project_aliases_fill_location() -> None:
    samples = (
        ("太子幸福广场 出租 1房 租金600$", ("莫尼旺", "太子幸福")),
        ("富力城 出租 一房 350$", ("60米", "富力")),
        ("The Peak 香格里拉 3房出租 1200$", ("金街",)),
        ("桥牌 The Bridge 2房出租 900$", ("金街",)),
        ("太子寰宇 出租 2房 租金800$", ("金界", "永旺")),
    )
    for text, needles in samples:
        facts = canonicalize_source(text)
        loc = facts["public_location_display"] or ""
        assert loc, text
        assert any(n in loc for n in needles), (text, loc)


def test_adviser_copy_uses_spoken_variants_not_stiff_boilerplate() -> None:
    copy = build_adviser_copy(
        {
            "adviser_signals_version": "explicit-v1",
            "adviser_signals": ["pet_allowed", "owner_direct"],
        },
        seed="QL-TEST-1",
        max_points=2,
    )
    assert copy
    assert "资料不完整" not in copy
    assert "更灵活" not in copy
    # Prefer human advisor tone over bureaucratic restatement.
    assert any(token in copy for token in ("房东", "宠物", "毛孩", "优先"))


def test_adviser_composes_high_floor_with_cleaning() -> None:
    copy = build_adviser_copy(
        {
            "floor": "39",
            "adviser_signals_version": "explicit-v1",
            "adviser_signals": ["cleaning_included"],
            "services": {"cleaning": "包含"},
        },
        seed="QL-PP-U4S6",
        max_points=2,
    )
    assert "39楼" in copy
    assert "保洁" in copy
    assert "自己少打扫一点" not in copy


def test_detail_hides_region_when_it_only_restates_project() -> None:
    from types import SimpleNamespace

    from v3_core.user_bot.listing_responses import _listing_fact_lines

    details = SimpleNamespace(
        project_name="The Pinnacle 幸福广场",
        location="太子幸福广场",
        property_type="公寓",
        layout="2房2厅｜2卫",
        monthly_rent_usd=850,
        size_sqm=None,
        floor="39",
        deposit_terms="押1付1",
        contract_term="1年",
        inventory_status="active",
        status_icon="🟢",
        status_label="当前可预约",
        public_listing_id="QL-PP-U4S6",
        management_fee="",
        water_rate="",
        electric_rate="",
        building_amenities="",
    )
    text = "\n".join(_listing_fact_lines(details))
    assert "🏠 <b>The Pinnacle 幸福广场" in text
    assert "📍 区域" not in text
    assert "莫尼旺" not in text  # hidden until location is fixed upstream