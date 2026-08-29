from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

import publication_package
from qiaolian_dual import common
from qiaolian_dual.area_admin import set_canonical_area
from qiaolian_dual.area_normalization import CANONICAL_AREAS, normalize_area
from qiaolian_dual.canonical_fact_projection import facts_hash, package_gate, validate_facts
from qiaolian_dual.canonical_facts import canonicalize_source
from qiaolian_dual.canonical_listing_materializer import materialize_draft_facts
from qiaolian_dual.db import SCHEMA as USER_BOT_SCHEMA
from qiaolian_dual.listing_taxonomy import (
    MARKET_LOCATIONS,
    PHYSICAL_AREAS,
    resolve_location_alias,
)
from qiaolian_dual.location_mapping import get_all_location_aliases, normalize_user_input


ROOT = Path(__file__).resolve().parents[1]


def _bootstrap(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(USER_BOT_SCHEMA)
        conn.executescript((ROOT / "schema_core.sql").read_text(encoding="utf-8"))


def _seed_draft(path: Path, *, draft_id: str = "DRF_AREA", listing_id: str = "l_901") -> None:
    facts = canonicalize_source("金边公寓出租\n1房1卫\n租金 $500/月")
    with sqlite3.connect(path) as conn:
        conn.execute(
            """INSERT INTO drafts(
                   draft_id,listing_id,title,area,property_type,price,normalized_data,
                   extracted_data,review_status,canonical_facts_hash,canonical_facts_schema
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                draft_id, listing_id, "1房｜公寓", "", "公寓", 500,
                json.dumps(facts, ensure_ascii=False),
                json.dumps(facts, ensure_ascii=False),
                "pending", facts["canonical_facts_hash"], facts["schema_version"],
            ),
        )
        conn.execute(
            """INSERT INTO listings(
                   listing_id,title,property_type,area,community,price,currency,status,
                   created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,'pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
            (listing_id, "1房｜公寓", "公寓", "", "", 500, "USD"),
        )


def _facts(path: Path, draft_id: str = "DRF_AREA") -> dict:
    with sqlite3.connect(path) as conn:
        raw = conn.execute(
            "SELECT normalized_data FROM drafts WHERE draft_id=?", (draft_id,)
        ).fetchone()[0]
    return json.loads(raw)


def test_manual_aliases_resolve_without_promoting_markets_to_physical_areas(tmp_path: Path) -> None:
    db_path = tmp_path / "area_alias.db"
    _bootstrap(db_path)
    _seed_draft(db_path)

    market = set_canonical_area(
        str(db_path), "l_901", "R&F City", "7", "管理员核对公开位置"
    )
    market_facts = _facts(db_path)
    assert market["new_area"] == "富力城"
    assert market["location_kind"] == "market_location"
    assert market_facts["canonical_area_key"] is None
    assert market_facts["public_location_key"] == "富力城"
    assert market_facts["publication_location_level"] == "level_1_market_confirmed"
    assert validate_facts(market_facts) == []

    physical = set_canonical_area(
        str(db_path), "l_901", "BKK 1", "7", "管理员核对完整地址"
    )
    physical_facts = _facts(db_path)
    assert physical["new_area"] == "BKK1"
    assert physical["location_kind"] == "physical_area"
    assert physical_facts["canonical_area_key"] == "BKK1"
    assert physical_facts["public_location_key"] == "BKK1"
    assert physical_facts["publication_location_level"] == "level_2_physical_confirmed"
    assert validate_facts(physical_facts) == []

    with pytest.raises(ValueError, match="area_not_in_canonical_catalog"):
        set_canonical_area(str(db_path), "l_901", "金边市区", "7", "城市不是区域")


def test_location_catalogs_are_derived_from_one_taxonomy() -> None:
    physical_displays = {item.display for item in PHYSICAL_AREAS}
    market_only = {item.display for item in MARKET_LOCATIONS} - physical_displays
    assert set(CANONICAL_AREAS) == physical_displays
    assert not (set(CANONICAL_AREAS) & {"富力城", "炳发城", "俄罗斯市场", "钻石岛", "金边市区"})
    assert {"富力城", "炳发城", "俄罗斯市场", "钻石岛"} <= market_only
    assert "太子幸福广场" in {item.display for item in MARKET_LOCATIONS}
    assert "太子/幸福" not in {item.display for item in MARKET_LOCATIONS}
    assert normalize_area("R&F City", "R&F City") == ""
    assert resolve_location_alias("Peng Huoth").display == "炳发城"
    assert resolve_location_alias("Peng Huoth").kind == "market_location"
    assert resolve_location_alias("太子/幸福").key == "太子幸福广场"
    assert resolve_location_alias("金边市区") is None
    assert normalize_user_input("想住 R&F City")[0] == "富力城"
    assert {"永旺商圈", "永旺1"} <= set(get_all_location_aliases("永旺1"))
    assert "金边市区" not in common.AREA_HINTS
    assert all("太子/幸福" not in label for _code, label in common.FIND_AREA_OPTIONS)


def test_unlabelled_physical_name_is_level_1_but_explicit_address_is_level_2() -> None:
    unlabelled = canonicalize_source("森速公寓出租\n1房1卫\n租金：$500/月")
    assert unlabelled["canonical_area_key"] is None
    assert unlabelled["public_location_key"] == "森速"
    assert unlabelled["publication_location_level"] == "level_1_market_confirmed"

    explicit = canonicalize_source("位置：森速\n公寓出租\n1房1卫\n租金：$500/月")
    assert explicit["canonical_area_key"] == "森速"
    assert explicit["public_location_key"] == "森速"
    assert explicit["publication_location_level"] == "level_2_physical_confirmed"


def test_equal_priority_market_candidates_require_review_instead_of_silent_choice() -> None:
    facts = canonicalize_source("BKK1 或 BKK2 公寓出租\n1房1卫\n租金：$500/月")
    assert facts["canonical_area_key"] is None
    assert "ambiguous_market_location" in facts["candidate_flags"]
    assert "ambiguous_market_location" in facts["quality"]["blocking_flags"]
    assert package_gate(facts, 4)["ok"] is False


def test_manual_override_recomputes_public_location_after_taxonomy_parse() -> None:
    facts = canonicalize_source(
        "金边公寓出租\n1房1卫\n租金：$500/月",
        manual_overrides={
            "canonical_area_key": "BKK1",
            "canonical_area_display": "BKK1",
            "canonical_area_level": "sangkat",
        },
    )
    assert facts["public_location_key"] == "BKK1"
    assert facts["publication_location_level"] == "level_2_physical_confirmed"
    assert validate_facts(facts) == []


def test_area_admin_recovers_only_provable_legacy_area_hash_shape(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy-area.db"
    _bootstrap(db_path)
    _seed_draft(db_path)
    with sqlite3.connect(db_path) as conn:
        facts = json.loads(conn.execute(
            "SELECT normalized_data FROM drafts WHERE draft_id='DRF_AREA'"
        ).fetchone()[0])
        facts["area"] = "金边市区"
        facts["normalized_area"] = "金边市区"
        conn.execute(
            "UPDATE drafts SET normalized_data=?, extracted_data=? WHERE draft_id='DRF_AREA'",
            (json.dumps(facts, ensure_ascii=False), json.dumps(facts, ensure_ascii=False)),
        )
    result = set_canonical_area(str(db_path), "l_901", "BKK 1", "7", "修复旧 area 工具写入")
    repaired = _facts(db_path)
    assert result["new_area"] == "BKK1"
    assert "area" not in repaired and "normalized_area" not in repaired
    assert validate_facts(repaired) == []


def test_property_description_never_becomes_project_identity() -> None:
    facts = canonicalize_source("联排别墅出租\n位置：森速\n4房5卫\n租金：$2300/月")
    assert facts["project_name"] is None
    assert facts["property_type"] == "排屋"
    assert facts["property_subtype"] == "联排别墅"


@pytest.mark.parametrize(
    "phrase",
    (
        "仅出租，不出售",
        "只用于出租，非出售",
        "出租，暂无计划出售",
        "for rent, not for sale",
        "rental only",
    ),
)
def test_negated_sale_terms_remain_rental_and_pass_deal_gate(phrase: str) -> None:
    facts = canonicalize_source(
        f"区域：BKK1\n公寓 1房1卫\n{phrase}\n租金：$680/月"
    )
    assert facts["deal_type"] == "rent"
    assert "mixed_sale_rent_terms" not in facts["quality"]["blocking_flags"]
    assert package_gate(facts, 4)["ok"] is True


def test_real_rent_and_sale_terms_remain_blocked() -> None:
    facts = canonicalize_source(
        "区域：BKK1\n公寓 1房1卫\n可出租，也可出售\n租金：$680/月\n售价：$100000"
    )
    gate = package_gate(facts, 4)
    assert facts["deal_type"] == "mixed"
    assert gate["ok"] is False
    assert "deal_type_not_rent" in gate["errors"]


def test_gate_rejects_market_or_city_promoted_to_physical_area() -> None:
    facts = canonicalize_source("区域：BKK1\n公寓 1房1卫出租\n租金：$680/月")
    for invalid in ("富力城", "金边市区"):
        tampered = dict(facts)
        tampered.update(
            {
                "canonical_area_key": invalid,
                "canonical_area_display": invalid,
                "canonical_area_level": "manual_confirmed",
                "area_status": "confirmed",
                "public_location_key": invalid,
                "public_location_display": invalid,
                "publication_location_level": "level_2_physical_confirmed",
            }
        )
        tampered["canonical_facts_hash"] = facts_hash(tampered)
        errors = package_gate(tampered, 4)["errors"]
        assert "canonical_area_not_in_physical_catalog" in errors


def test_materialize_draft_reuses_level_2_public_location_key(tmp_path: Path) -> None:
    db_path = tmp_path / "materialize.db"
    _bootstrap(db_path)
    facts = canonicalize_source("位置：森速\n公寓 1房出租\n租金：$680/月")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO drafts(
                   draft_id,title,property_type,price,normalized_data,extracted_data,
                   review_status
               ) VALUES ('DRF_SENSOK','森速｜1房','公寓',680,?,?,'pending')""",
            (json.dumps(facts, ensure_ascii=False), json.dumps(facts, ensure_ascii=False)),
        )
        materialize_draft_facts(conn, draft_id="DRF_SENSOK", facts=facts)
        public_key = conn.execute(
            "SELECT public_location_key FROM drafts WHERE draft_id='DRF_SENSOK'"
        ).fetchone()[0]
    assert facts["canonical_area_key"] == "森速"
    assert facts["public_location_key"] == "森速"
    assert public_key == "森速"


def test_cover_keeps_alias_type_and_never_adds_month_to_sale() -> None:
    captured: list[dict] = []

    def fake_render_html_cover(**kwargs):
        captured.append(kwargs["data"])

    sale = {
        "normalized_data": json.dumps(
            {
                "project_name": "永旺一",
                "project_alias": "Aeon1",
                "property_type_display": "商铺",
                "deal_type": "sale",
                "public_location_display": "BKK1",
                "layout": "开放式",
            },
            ensure_ascii=False,
        ),
        "price": "100000",
        "highlights": [],
    }
    rent = {
        **sale,
        "normalized_data": json.dumps(
            {
                **json.loads(sale["normalized_data"]),
                "deal_type": "rent",
            },
            ensure_ascii=False,
        ),
        "price": "680",
    }
    with patch("html_cover_renderer.render_html_cover", side_effect=fake_render_html_cover):
        publication_package._render_cover(sale, "source.jpg", "sale.png", "minimal_white")
        publication_package._render_cover(rent, "source.jpg", "rent.png", "minimal_white")

    assert captured[0]["project"] == "永旺一 · Aeon1"
    assert captured[0]["project_alias"] == "Aeon1"
    assert captured[0]["property_type"] == "商铺"
    assert captured[0]["deal_type"] == "sale"
    assert captured[0]["layout"] == "商铺 · 开放式"
    assert captured[0]["price"] == "$100000"
    assert captured[0]["price_suffix"] == ""
    assert captured[1]["price"] == "$680/月"
    assert captured[1]["price_suffix"] == "/月"


def test_canonical_facts_has_no_parallel_taxonomy_tables_or_extractors() -> None:
    import qiaolian_dual.canonical_facts as canonical

    for name in (
        "PHYSICAL_AREA_ALIASES",
        "MARKET_LOCATION_ALIASES",
        "PROJECT_IDENTITIES",
        "_extract_area",
        "_extract_market_locations",
        "_extract_project",
        "_extract_property_type",
        "_public_location",
    ):
        assert not hasattr(canonical, name)
