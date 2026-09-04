from qiaolian_dual.v2_safe_adapter import enrich_authoritative_facts


def test_adapter_adds_safe_fields_without_overwriting_authoritative_values():
    authoritative = {
        "project_name": "富力城",
        "monthly_rent_usd": 800,
        "layout": "2房1厅",
        "parser_revision": "v1.1",
        "quality": {"score": 88},
    }
    result = enrich_authoritative_facts(
        "富力城 2房1厅 月租800美元，包物业和网络，每周2次保洁，健身房",
        authoritative,
    )

    assert result["project_name"] == "富力城"
    assert result["monthly_rent_usd"] == 800
    assert result["layout"] == "2房1厅"
    assert result["parser_revision"] == "v1.1"
    assert result["quality"] == {"score": 88}
    assert result["services"]["cleaning"] == "每周2次"
    assert "物业费" in result["included"]
    assert "Wi-Fi" in result["included"]
    assert "健身房" in result["amenities"]
    assert result["enrichment_version"] == "v2_safe"


def test_adapter_returns_a_new_mapping():
    authoritative = {"project_name": "钻石岛"}
    result = enrich_authoritative_facts("钻石岛 健身房", authoritative)
    assert result is not authoritative
    assert authoritative == {"project_name": "钻石岛"}
