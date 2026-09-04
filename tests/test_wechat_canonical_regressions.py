from source_sanitizer import sanitize_source_text, strip_unicode_noise
from qiaolian_dual.canonical_facts import canonicalize_source


def _facts(text: str):
    sanitized = sanitize_source_text(text).text
    return canonicalize_source(
        text,
        sanitized_text=sanitized,
        source_identity={"source_type": "wechat_note", "source_post_id": 1},
        media_summary={"image_count": 8, "media_type": "image"},
    )


def _pinnacle_text() -> str:
    return (
        "The Pinnacle 幸福广场\n"
        "位置：太子幸福广场\n"
        "户型：2房2厅2卫\n"
        "房间价格：850美金\n"
        "押1付1\n"
        "合同情况：1年\n"
        "楼层：39\n"
    )


def test_pinnacle_project_metadata_and_rental_semantics_are_publishable():
    facts = _facts(_pinnacle_text())
    assert facts["project_key"] == "the_pinnacle"
    assert facts["project_name"] == "The Pinnacle 幸福广场"
    assert facts["property_type"] == "公寓"
    assert facts["property_type_status"] == "inferred"
    assert facts["deal_type"] == "rent"
    assert facts["monthly_rent_usd"] == 850
    assert facts["deposit_payment_terms"] == "押1付1"
    assert facts["contract_term_months"] == 12
    assert "unknown_property_type" not in facts["quality"]["blocking_flags"]
    assert "missing_rental_intent" not in facts["quality"]["blocking_flags"]


def test_wechat_private_use_format_noise_does_not_change_canonical_facts():
    marker = chr(0xF003)
    clean = _pinnacle_text()
    noisy = clean.replace("幸福广场", f"幸{marker}福{marker}广{marker}场{marker}")
    noisy = noisy.replace("2房2厅2卫", f"2房{marker}2厅{marker}2卫{marker}")
    noisy = noisy.replace("押1付1", f"押{marker}1付{marker}1")
    assert ord(marker) == 0xF003
    assert marker not in strip_unicode_noise(noisy)
    clean_facts = _facts(clean)
    noisy_facts = _facts(noisy)
    for key in (
        "project_key", "project_name", "public_location_display", "layout",
        "monthly_rent_usd", "deposit_payment_terms", "contract_term_months",
        "property_type", "deal_type",
    ):
        assert noisy_facts[key] == clean_facts[key]
    assert marker not in noisy_facts["display_title"]


def test_bare_amount_without_rent_or_sale_semantics_stays_unknown():
    facts = _facts(
        "The Pinnacle 幸福广场\n"
        "户型：2房2厅2卫\n"
        "850美金\n"
    )
    assert facts["deal_type"] == "unknown"
    assert "missing_rental_intent" in facts["quality"]["blocking_flags"]


def test_unknown_project_without_property_evidence_stays_unknown_property_type():
    facts = _facts(
        "项目：Random Residence\n"
        "位置：BKK1\n"
        "户型：2房2卫\n"
        "租金：850美金/月\n"
    )
    assert facts["deal_type"] == "rent"
    assert facts["property_type"] == "未知"
    assert facts["property_type_status"] == "unknown"
    assert "unknown_property_type" in facts["quality"]["blocking_flags"]
