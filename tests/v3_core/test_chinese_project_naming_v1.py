from v3_core.inventory.chinese_project_naming import ALIAS_ROWS,NAMING_ROWS,PROJECT_NAMES,ambiguous_alias_count,family_alias_count,office_only_project_ids,resolution_action_counts,resolve_chinese_project,unique_normalized_alias_count
from v3_core.inventory.project_registry import ACTIVE_PROJECTS,RESEARCH_PROJECTS,canonical_location_projection,project_by_key,project_search_terms,resolve_project
from v3_core.user_bot.search_query import parse_search_criteria
def test_chinese_naming_counts_and_unique_identity():
    assert len(NAMING_ROWS)==114 and len(PROJECT_NAMES)==113 and len(ALIAS_ROWS)==522 and unique_normalized_alias_count()==429
    assert len(RESEARCH_PROJECTS)==107 and len(ACTIVE_PROJECTS)==114 and len({p.entity_id for p in ACTIVE_PROJECTS})==114 and len({p.key for p in ACTIVE_PROJECTS})==114
    counts=resolution_action_counts();assert counts["DIRECT_RESOLVE"]==352 and counts["CANDIDATE_ONLY"]==85 and counts["RESOLVE_WITH_PROJECT_OR_GEO_CONTEXT"]==44 and counts["DEPRECATE_PROJECT_ALIAS"]==36 and counts["REDIRECT_NEEDS_VERIFICATION"]==3 and counts["SEARCH_ONLY_LEGACY"]==2
    assert family_alias_count()==44 and ambiguous_alias_count()==40 and len(office_only_project_ids())==2
def test_required_direct_chinese_names():
    assert resolve_project("桥牌").project_entity_id=="project:the-bridge";assert resolve_project("世桥").project_entity_id=="project:the-bridge";assert resolve_project("香格里拉").project_entity_id=="project:the-peak";assert resolve_project("雅居乐").project_entity_id=="project:agile-sky-residence";assert resolve_project("财富大厦").project_entity_id=="project:wealth-mansion";assert resolve_project("摩根大厦").project_entity_id=="project:morgan-tower";assert resolve_project("摩根天御").project_entity_id=="project:morgan-enmaison";assert resolve_project("国金一期").project_entity_id=="project:urban-village-1";assert resolve_project("国金二期").project_entity_id=="project:urban-village-2";assert resolve_project("时代9").project_entity_id=="project:times-square-9";assert resolve_project("Palm Creek").project_entity_id=="project:chankiri-palm-creek"
def test_locked_resolution_actions_are_respected():
    bkk1=resolve_project("BKK1雅居乐");assert not bkk1.project_entity_id and bkk1.ambiguity and bkk1.candidates==("project:agile-sky-residence",) and bkk1.resolution_action=="DEPRECATE_PROJECT_ALIAS"
    wells=resolve_project("威尔斯公馆");assert wells.project_entity_id=="project:wealth-mansion" and wells.resolution_action=="REDIRECT_NEEDS_VERIFICATION"
    assert canonical_location_projection("agile-sky-residence")["canonical_geo"]!="BKK1";assert "钻石岛" not in canonical_location_projection("the-peak")["canonical_geo"]
def test_family_and_market_ambiguity():
    for q in ("摩根","首都国金","Chankiri","Orkide","一号路炳发","铁桥头炳发","Norea附近炳发"):assert resolve_project(q).ambiguity
    sixty=resolve_project("60米炳发");assert sixty.ambiguity and set(sixty.candidates)=={"project:the-star-diamond","project:the-star-diamond-ii"}
    fifty=resolve_project("50米炳发");assert fifty.ambiguity and fifty.candidates==("project:the-star-mera-garden",)
    assert project_search_terms("50米炳发") and project_search_terms("BKK1雅居乐")
def test_office_projects_are_not_default_residential_search_terms():
    assert not project_search_terms("摩根大厦");assert project_search_terms("摩根大厦",residential_only=False);assert not project_search_terms("TK Royal One");assert project_search_terms("TK Royal One",residential_only=False)
def test_userbot_chinese_queries_are_strict_project_terms():
    for q in ("桥牌 两房 800","香格里拉 一房","雅居乐 1000以内","60米炳发 别墅","50米炳发 别墅","集茂271 别墅","奥凯德皇家 别墅"):assert parse_search_criteria(q).project_terms
def test_market_alias_never_overwrites_canonical_geo():
    before=canonical_location_projection("the-peak");resolve_project("钻石岛香格里拉");assert canonical_location_projection("the-peak")==before
    before=canonical_location_projection("the-star-platinum-rosato");resolve_project("Norea附近炳发");assert canonical_location_projection("the-star-platinum-rosato")==before
def test_new_verified_naming_projects_are_active_without_duplicate_identity():
    for eid in ("project:new:casa-service-apartment","project:new:the-elysee","project:new:picasso-sky-gemme","project:new:times-square-6","project:new:orkide-the-royal-condominium"):assert project_by_key(eid) is not None
