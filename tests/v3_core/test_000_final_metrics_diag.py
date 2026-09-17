from v3_core.inventory.phnom_penh_location_registry import registry_alias_count, registry_project_count, duplicate_project_aliases
from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.user_bot.search_query import parse_search_criteria

def test_registry_final_metrics_report():
    facts=canonicalize_source("BKK1 雅居乐 公寓出租 2房1厅 租金 $800/月")
    criteria=parse_search_criteria("BKK1 雅居乐")
    raise AssertionError({
        "project_count": registry_project_count(),
        "alias_count": registry_alias_count(),
        "duplicates": duplicate_project_aliases(),
        "bkk1_agile": {k:facts.get(k) for k in ("project_key","project_name","canonical_area_key","canonical_area_display","market_location_keys","public_location_key","public_location_display")},
        "search": {"project_terms":criteria.project_terms,"location_keys":criteria.location_keys},
    })
