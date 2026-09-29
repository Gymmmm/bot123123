from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.inventory.canonical_facts import canonicalize_source, draft_projection
from v3_core.inventory.project_knowledge import (
    answer_project_question,
    project_knowledge_stats,
    project_reference_for_key,
    project_reference_location,
    project_reference_summary,
    registry_project_identities,
)


def test_project_knowledge_bundle_has_full_research_coverage():
    assert project_knowledge_stats() == {
        "projects": 114,
        "registry_profiles": 107,
        "living_profiles": 107,
        "v5_profiles": 113,
        "relations": 39,
        "evidence_rows": 53,
        "conflicts": 12,
        "alias_rows": 476,
    }


def test_bare_known_project_gets_detailed_reference_without_source_location():
    facts = canonicalize_source("The Palms别墅出租\n4房\n租金$2500/月")
    assert facts["project_key"] == "the_palms"
    assert facts["public_location_display"] is None

    reference = project_reference_for_key(facts["project_key"])
    assert reference["knowledge_key"] == "the-palms"
    assert reference["reference_only"] is True
    assert reference["registry"]["canonical_geo_display"] == "铁桥头 · 一号路 · 湄公河边"
    assert reference["registry"]["developer"] == "Oxley Worldbridge"
    assert "公共设施与安保较明确" in reference["living"]["living_summary"]
    assert reference["inheritance_policy"]["listing_fact_precedence"] is True


def test_project_reference_is_exposed_in_draft_projection():
    facts = canonicalize_source("The Peak 香格里拉\n2房\n租金$1400/月")
    projected = draft_projection(facts)
    assert projected["project_reference"]["knowledge_key"] == "the-peak"
    assert projected["project_reference"]["living"]["pool"] == "YES"
    assert projected["project_reference"]["evidence"]


def test_listing_specific_fee_is_not_overwritten_by_project_market_reference():
    facts = canonicalize_source("雅居乐天悦\n1房\n租金$900/月\n电费$0.35/度")
    assert facts["electric_rate"] == "$0.35/度"
    reference = project_reference_for_key(facts["project_key"])
    assert reference["knowledge_key"] == "agile-sky-residence"
    assert reference["living"]["electricity_rate_min"] == "0.25"
    assert reference["living"]["electricity_rate_max"] == "0.27"


def test_wealth_mansion_verified_aliases_reconcile_to_one_project():
    wells = canonicalize_source("威尔斯公馆出租\n2房\n租金$1200/月")
    wealth = canonicalize_source("财富大厦出租\n2房\n租金$1200/月")
    assert wells["project_key"] == "wealth_mansion"
    assert wealth["project_key"] == "wealth_mansion"
    assert project_reference_for_key(wells["project_key"])["knowledge_key"] == "wealth-mansion"
    assert project_reference_for_key(wealth["project_key"])["knowledge_key"] == "wealth-mansion"


def test_market_handle_does_not_become_specific_borey_project():
    facts = canonicalize_source("铁桥头一号路炳发\n联排出租\n4房\n租金$2000")
    assert facts["project_key"] is None
    assert facts["project_brand"] == "Peng Huoth"
    assert facts["public_location_display"] == "铁桥头 · 一号路炳发"


def test_registry_expands_recognition_without_inheriting_authoritative_location():
    identities = {item["taxonomy_key"] for item in registry_project_identities()}
    assert "the_palms" in identities
    assert "chip_mong_park_land_598" in identities

    facts = canonicalize_source("Park Land 598别墅出租\n4房\n租金$1800/月")
    assert facts["project_key"] == "chip_mong_park_land_598"
    assert project_reference_for_key(facts["project_key"])["knowledge_key"] == "chip-mong-park-land-598"
    # Registry location is available as reference, but not promoted to an
    # authoritative listing location without an explicit safe location rule.
    assert facts["public_location_display"] is None
    assert project_reference_for_key(facts["project_key"])["registry"]["public_location_display_cn"] == "598路 · Chip Mong"


def test_projectless_listing_does_not_receive_project_reference():
    facts = canonicalize_source("BKK1两房出租\n租金$800/月")
    assert "project_reference" not in facts


def test_verified_alias_registry_adds_new_projects_without_family_false_positive():
    ts6 = canonicalize_source("Time Square 6出租\n1房\n租金$700/月")
    assert ts6["project_key"] == "times_square_6"
    assert project_reference_for_key(ts6["project_key"])["knowledge_key"] == "times-square-6"
    assert ts6["public_location_display"] == "BKK1 · 302路"

    royal_condo = canonicalize_source("奥凯德皇家公寓出租\n2房\n租金$900/月")
    assert royal_condo["project_key"] == "orkide_the_royal_condominium"
    assert project_reference_for_key(royal_condo["project_key"])["knowledge_key"] == "orkide-the-royal-condominium"
    assert royal_condo["public_location_display"] == "2004路 · 森速"

    royal_villa = canonicalize_source("Orkide The Royal 别墅社区\n4房\n租金$2500/月")
    assert royal_villa["project_key"] == "orkide_the_royal"

    generic = canonicalize_source("时代广场出租\n1房\n租金$700/月")
    assert generic["project_key"] is None


def test_public_inventory_attaches_project_reference_without_mutating_canonical_hash_payload():
    from qiaolian_dual.adapters.public_inventory import PublicInventoryAdapter

    canonical = canonicalize_source("The Peak 香格里拉\n2房\n租金$1400/月")
    assert "project_reference" not in canonical
    before_hash = canonical["canonical_facts_hash"]
    view = SimpleNamespace(
        snapshot={"canonical_facts": canonical},
        frozen_listing={},
        frozen_offer={},
        listing={"inventory_status": "active"},
        offer={},
        gallery=(),
        package={},
        public_listing_id="QL-RF-A2B3",
        listing_id="l_test",
        bookable=True,
    )
    item = PublicInventoryAdapter().to_3858_listing(view)
    assert item["normalized_data"]["project_reference"]["knowledge_key"] == "the-peak"
    assert item["normalized_data"]["canonical_facts_hash"] == before_hash
    assert "project_reference" not in canonical


def test_production_penthouse_hashtag_resolves_project_not_unit_subtype():
    facts = canonicalize_source(
        "418🌳 【公寓出租/出售】 #Penthouse\n"
        "💰 售价：$100,000\n💰 租金：$350/月\n🏠 户型：单间"
    )
    assert facts["project_key"] == "the_penthouse_residence"
    assert facts["project_name"] == "The Penthouse Residence"
    assert facts["public_location_display"] == "永旺1附近"
    assert project_reference_for_key(facts["project_key"]) is not None

    # Bare descriptive penthouse language remains unsafe as a project identity.
    generic = canonicalize_source("BKK1 penthouse unit for rent $3000/month")
    assert generic["project_key"] is None


def test_generic_peng_huoth_gets_family_reference_not_fake_child_project_facts():
    facts = canonicalize_source("50米炳发城\n排屋出租\n租金$1500")
    assert facts["project_key"] == "peng_huoth_city"
    reference = project_reference_for_key(facts["project_key"])
    assert reference["reference_kind"] == "project_family"
    assert reference["requires_specific_project"] is True
    assert reference["knowledge_key"] == "family:peng-huoth"
    assert reference["child_project_keys"]
    assert "living" not in reference


def test_project_question_answers_use_verified_reference_and_fee_caveats():
    fee = answer_project_question("agile_sky_residence", "这个项目电费多少？")
    assert "0.25–0.27" in fee
    assert "具体这套以房东和合同为准" in fee

    amenities = answer_project_question("the_peak", "有泳池和健身房吗？")
    assert "泳池" in amenities
    assert "健身" in amenities

    family = answer_project_question("peng_huoth_city", "炳发城有泳池吗？")
    assert "先按道路/地标缩小炳发 family" in family

    assert answer_project_question("the_peak", "这个房东最低多少？") is None


@pytest.mark.asyncio
async def test_listing_question_uses_project_answer_before_advisor_handoff():
    from v3_core.user_bot.telegram_listing_callback import handle_v3_listing_question_text

    class Message:
        text = "这个项目有泳池吗？"
        def __init__(self):
            self.sent = []
        async def reply_text(self, text, **kwargs):
            self.sent.append((text, kwargs))

    class Effects:
        def answer_project_question(self, *, intent, question):
            assert intent.public_listing_id == "QL-PP-A2B3"
            assert question == "这个项目有泳池吗？"
            return "The Peak 香格里拉项目资料显示有：泳池。"
        async def execute_question(self, **kwargs):
            raise AssertionError("verified project answer must not be forwarded")

    message = Message()
    update = SimpleNamespace(effective_message=message)
    context = SimpleNamespace(
        user_data={
            "v3_listing_question": {
                "listing_id": "l_test",
                "public_listing_id": "QL-PP-A2B3",
                "source": "listing_callback",
            }
        },
        bot=None,
    )
    handled = await handle_v3_listing_question_text(
        update,
        context,
        contact_effects=Effects(),
    )
    assert handled is True
    assert message.sent[0][0].startswith("The Peak 香格里拉")
    assert "v3_listing_question" not in context.user_data


def test_peng_huoth_family_reference_has_safe_official_context():
    reference = project_reference_for_key("peng_huoth_city")
    assert reference["developer"] == "Borey Peng Huoth / Peng Huoth Group"
    assert "1号路 / National Road 1" in reference["known_corridors"]
    assert "60米大道 / Samdech Techo Hun Sen Blvd" in reference["known_corridors"]
    assert any("Single Villa" in item for item in reference["property_types"])
    assert reference["requires_specific_project"] is True

    developer = answer_project_question("peng_huoth_city", "开发商是谁？")
    assert "Peng Huoth Group" in developer
    location = answer_project_question("peng_huoth_city", "这个项目在哪里？")
    assert "不是单一地址" in location
    assert "先按道路/地标缩小炳发 family" in location


@pytest.mark.parametrize(
    ("project_key", "expected_developer"),
    [
        ("le_conde_bkk1", "Wangfu International"),
        ("la_vista_one", "YIN YI VENTURE CO., LTD / 柬埔寨银翼创投有限公司"),
        ("picasso_city_garden", "Picasso City Garden Development Plc."),
        ("peninsula_private_residence", "CC Peninsula Co., Ltd.（National 6A Investment + SUN & MOON Group + Saturn Investment 合资）"),
    ],
)
def test_web_verified_profiles_fill_reference_gaps(project_key, expected_developer):
    reference = project_reference_for_key(project_key)
    assert reference["developer"] == expected_developer
    assert reference["source_urls"]


def test_diamond_one_reference_has_safe_project_level_facilities():
    reference = project_reference_for_key("diamond_one")
    assert reference["location"] == "钻石岛 / Koh Pich"
    assert reference["completion_year"] == "2019"
    assert "泳池" in reference["amenities"]
    assert "健身房" in reference["amenities"]


def test_project_question_can_use_web_verified_developer_and_location():
    developer = answer_project_question("le_conde_bkk1", "开发商是谁？")
    assert "Wangfu International" in developer
    location = answer_project_question("picasso_city_garden", "项目在哪里？")
    assert "BKK1" in location
    assert "Street 322" in location


def test_verified_project_location_fallback_is_reference_only():
    facts = canonicalize_source("The Palms别墅出租\n4房\n租金$2500/月")
    assert facts["public_location_display"] is None
    fallback = project_reference_location(facts["project_key"])
    assert fallback == {
        "display": "一号路 / Norea方向｜The Palms",
        "source": "project_v5_verified",
        "confidence": "verified",
    }


def test_public_inventory_uses_verified_project_location_when_post_omits_address():
    from qiaolian_dual.adapters.public_inventory import PublicInventoryAdapter

    canonical = canonicalize_source("The Palms别墅出租\n4房\n租金$2500/月")
    before_hash = canonical["canonical_facts_hash"]
    view = SimpleNamespace(
        snapshot={"canonical_facts": canonical},
        frozen_listing={},
        frozen_offer={},
        listing={"inventory_status": "active"},
        offer={},
        gallery=(),
        package={},
        public_listing_id="QL-PP-A2B3",
        listing_id="l_palms",
        bookable=True,
    )
    item = PublicInventoryAdapter().to_3858_listing(view)
    assert item["area"] == "一号路 / Norea方向｜The Palms"
    assert item["normalized_data"]["project_reference_location"]["confidence"] == "verified"
    assert item["normalized_data"]["canonical_facts_hash"] == before_hash
    assert canonical["public_location_display"] is None
    assert "project_reference_location" not in canonical


@pytest.mark.parametrize(
    ("project_key", "location_token"),
    [
        ("prince_central_plaza", "Norodom"),
        ("prince_international_plaza", "Sen Sok"),
        ("casa_meridian", "Koh Pich"),
        ("sky_villa", "Street 163"),
    ],
)
def test_web_verified_location_fallback_covers_known_projects(project_key, location_token):
    fallback = project_reference_location(project_key)
    assert fallback["confidence"] == "verified"
    assert location_token in fallback["display"]


def test_project_summary_combines_verified_location_developer_amenities_and_nearby():
    summary = project_reference_summary("picasso_city_garden")
    assert "Picasso" in summary or "毕加索" in summary
    assert "Picasso City Garden Development Plc." in summary
    assert "BKK1" in summary
    assert "具体房源的租金、水电、管理费" in summary

    diamond = project_reference_summary("diamond_one")
    assert "泳池" in diamond
    assert "健身房" in diamond
    assert "永旺1" in diamond


def test_project_intro_question_returns_detailed_reference_summary():
    answer = answer_project_question("la_vista_one", "给我这个楼盘详细资料")
    assert "YIN YI VENTURE" in answer
    assert "水净华" in answer
    assert "具体房源的租金" in answer


def test_orkide_royal_condominium_official_profile_is_detailed():
    reference = project_reference_for_key("orkide_the_royal_condominium")
    assert reference["developer"] == "Orkidē Development"
    assert "6栋公寓塔楼" in reference["building_profile"]
    assert "Studio" in reference["unit_types"]
    assert "泳池" in reference["amenities"]
    assert "桑拿" in reference["amenities"]
    assert "2004路" in project_reference_location("orkide_the_royal_condominium")["display"]

    answer = answer_project_question("orkide_the_royal_condominium", "这个项目有哪些户型？")
    assert "Studio" in answer
    assert "3 Bedrooms" in answer
