from v3_core.inventory.project_registry import RESEARCH_PROJECTS,FAMILIES,active_alias_count,ambiguous_family_alias_count,resolve_project,canonical_location_projection,project_search_terms
from v3_core.inventory import listing_taxonomy
from v3_core.inventory.project_relations import RELATIONS
from v3_core.inventory.project_living_facts import CONFLICT_ROWS,evidence_for,safe_project_value
def test_counts():
    assert len(RESEARCH_PROJECTS)==107 and len(FAMILIES)==13 and active_alias_count()==367
    assert len(RELATIONS)==39 and len(CONFLICT_ROWS)==12
def test_search():
    r=resolve_project("60米炳发");assert r.ambiguity and r.candidate_scores[:2]==(("project:the-star-diamond",90),("project:the-star-diamond-ii",90))
    assert resolve_project("50米炳发").project_entity_id=="project:the-star-mera-garden"
    for q in ("一号路炳发","铁桥头炳发","Norea附近炳发","Morgan","Urban Village","Time Square"):assert resolve_project(q).ambiguity
    assert resolve_project("BKK1雅居乐").project_entity_id=="project:agile-sky-residence"
    assert resolve_project("Chip Mong 271").project_entity_id=="project:chip-mong-landmark-271"
    assert resolve_project("Orkide 2004").project_entity_id=="project:orkide-the-royal"
def test_geo_separation():
    assert "BKK1" not in canonical_location_projection("agile-sky-residence")["canonical_geo"]
    assert "钻石岛" not in canonical_location_projection("the-peak")["canonical_geo"]
    for q in ("永旺1","永旺2","永旺3","钻石岛附近","Norea附近"):assert not resolve_project(q).project_entity_id
def test_owner_specific_gate():
    assert [e for e in evidence_for("project:the-palms","electricity_rate") if e.source_scope=="OWNER_SPECIFIC"]
    assert safe_project_value("project:the-palms","electricity_rate") is None

def test_runtime_identity_parity_and_search_terms():
    taxonomy_projects={item.key.replace("_","-") for item in listing_taxonomy.PROJECT_IDENTITIES if item.kind=="project"}
    assert taxonomy_projects=={item.key for item in RESEARCH_PROJECTS}
    assert len(taxonomy_projects)==107
    assert ambiguous_family_alias_count()==37
    assert "雅居乐" in project_search_terms("BKK1 雅居乐 1000以内")

def test_required_semantics():
    assert resolve_project("50米炳发").project_entity_id=="project:the-star-mera-garden"
    assert resolve_project("60米炳发").project_entity_id==""
    assert resolve_project("60米炳发").ambiguity
    assert resolve_project("Morgan").ambiguity
    assert resolve_project("Urban Village").ambiguity
    assert resolve_project("Time Square").ambiguity
    assert resolve_project("Peng Huoth").ambiguity
    assert resolve_project("Chip Mong").ambiguity
    assert resolve_project("永旺1").project_entity_id==""
    assert resolve_project("永旺2").project_entity_id==""
    assert resolve_project("永旺3").project_entity_id==""
