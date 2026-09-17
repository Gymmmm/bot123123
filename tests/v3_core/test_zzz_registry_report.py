from v3_core.inventory.phnom_penh_location_registry import (
    PROJECT_IDENTITIES, duplicate_project_aliases, registry_alias_count,
    registry_project_count, project_by_key, resolve_project_alias,
)
from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.user_bot.search_query import parse_search_criteria

print("PROJECT_COUNT", registry_project_count())
print("ALIAS_COUNT", registry_alias_count())
print("DUPLICATES", duplicate_project_aliases())
for alias in ("雅居乐","Agile","Agile Sky","Agile Sky Residence","The Bridge","Bridge","桥牌","世桥","R&F City","R&F","富力城","The Peak","Peak","香格里拉"):
    item=resolve_project_alias(alias)
    print("RESOLVE", alias, item.key if item else None)
facts=canonicalize_source("BKK1 雅居乐 公寓出租 2房1厅 租金 $800/月")
print("BKK1_AGILE", {k:facts.get(k) for k in ("project_key","project_name","canonical_area_key","canonical_area_display","market_location_keys","public_location_key","public_location_display")})
criteria=parse_search_criteria("BKK1 雅居乐")
print("SEARCH", {"project_terms":criteria.project_terms,"location_keys":criteria.location_keys})
