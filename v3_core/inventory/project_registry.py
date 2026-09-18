"""Project Registry V3 with Chinese Project Naming Layer V1."""
from __future__ import annotations
from dataclasses import dataclass,replace
from pathlib import Path
import csv,re
from .chinese_project_naming import PROJECT_NAMES as CHINESE_PROJECT_NAMES,PROJECT_NAME_BY_ID,ALIAS_ROWS as CHINESE_ALIAS_ROWS,aliases_for_project as chinese_aliases_for_project,resolve_chinese_project,residential_eligible
DATA_DIR=Path(__file__).with_name("data");AUTHORITATIVE_STATUSES={"VERIFIED"};KNOWN_STATUSES={"VERIFIED","PARTIAL","NEEDS_VERIFICATION","UNKNOWN"}
def _clean(v):return re.sub(r"\s+"," ",str(v or "").replace("\u00a0"," ")).strip()
def _norm(v):return re.sub(r"[\s·._\-/]+","",_clean(v).casefold())
def _split(v):return tuple(x.strip() for x in str(v or "").split(";") if x.strip())
def _rows(name):
    with (DATA_DIR/name).open(encoding="utf-8-sig",newline="") as f:return tuple(dict(r) for r in csv.DictReader(f))
@dataclass(frozen=True)
class Project:
    entity_id:str;key:str;name:str;name_en:str;project_type:str;developer:str;brand:str;family:str;parent_project:str;khan:str;sangkat:str;locality:str;primary_road:str;secondary_roads:tuple[str,...];canonical_geo_display:str;public_location_display_cn:str;market_positioning:tuple[str,...];market_aliases:tuple[str,...];search_aliases:tuple[str,...];verification_status:str;source_1:str;source_2:str;source_3:str;notes:str;legacy_only:bool=False;occupancy_type:str=""
    @property
    def location_authoritative(self):return self.verification_status in AUTHORITATIVE_STATUSES
    @property
    def aliases(self):return tuple(dict.fromkeys(x for x in (self.name,self.name_en,*self.market_aliases,*self.search_aliases) if x))
    @property
    def residential_eligible(self):return residential_eligible(self.entity_id)
@dataclass(frozen=True)
class Family:
    family_id:str;name_cn:str;name_en:str;developer:str;child_projects:tuple[str,...];ambiguous_aliases:tuple[str,...];resolution_rule:str
@dataclass(frozen=True)
class Resolution:
    project_key:str="";project_entity_id:str="";project_name:str="";project_family:str="";project_type:str="";match_alias:str="";match_type:str="";confidence:int=0;verification_status:str="UNKNOWN";market_evidence:tuple[str,...]=();location_evidence:tuple[str,...]=();ambiguity:bool=False;family_id:str="";candidates:tuple[str,...]=();candidate_scores:tuple[tuple[str,int],...]=();resolution_action:str="";conflict_flag:bool=False;occupancy_type:str=""
def _project(r):
    st=_clean(r.get("verification_status")) or "UNKNOWN";st=st if st in KNOWN_STATUSES else "UNKNOWN"
    return Project(r["entity_id"],r["canonical_key"],r["canonical_name_cn"],r["canonical_name_en"],r["project_type"],r["developer"],r["brand"],r["project_family"],r["parent_project"],r["khan"],r["sangkat"],r["locality"],r["primary_road"],_split(r["secondary_roads"]),r["canonical_geo_display"],r["public_location_display_cn"],_split(r["market_positioning_cn"]),_split(r["market_aliases_cn"]),_split(r["search_aliases_cn"]),st,r["source_1"],r["source_2"],r["source_3"],r["notes"])
RESEARCH_PROJECTS=tuple(_project(r) for r in _rows("pp_project_registry_v3.csv"))
if len(RESEARCH_PROJECTS)!=107:raise RuntimeError(f"research project count drift:{len(RESEARCH_PROJECTS)}")
if len({p.entity_id for p in RESEARCH_PROJECTS})!=107 or len({p.key for p in RESEARCH_PROJECTS})!=107:raise RuntimeError("duplicate project identity")
FAMILIES=tuple(Family(r["family_id"],r["family_name_cn"],r["family_name_en"],r["developer"],_split(r["child_projects"]),_split(r["ambiguous_aliases"]),r["resolution_rule"]) for r in _rows("pp_project_families_v1.csv"))
ALIAS_ROWS=_rows("pp_project_alias_index_v3.csv");PROPERTY_TYPE_ROWS=_rows("pp_project_property_types_v1.csv")
def _overlay(p):
    meta=PROJECT_NAME_BY_ID.get(p.entity_id)
    if not meta:return replace(p,legacy_only=True)
    return replace(p,name=meta.preferred_cn_name or p.name,name_en=meta.canonical_name_en or p.name_en,project_type=meta.project_type or p.project_type,family=meta.family_name or p.family,occupancy_type=meta.occupancy_type,legacy_only=False)
_OVERLAID=tuple(_overlay(p) for p in RESEARCH_PROJECTS);_existing_ids={p.entity_id for p in _OVERLAID};_NAMING_ONLY=[]
for meta in CHINESE_PROJECT_NAMES:
    if meta.canonical_identity in _existing_ids or not meta.canonical_identity.startswith("project:new:"):continue
    key=meta.canonical_identity.split("project:new:",1)[1];st=meta.verification_status if meta.verification_status in KNOWN_STATUSES else "UNKNOWN"
    _NAMING_ONLY.append(Project(meta.canonical_identity,key,meta.preferred_cn_name,meta.canonical_name_en,meta.project_type,"","",meta.family_name,"","","","",meta.canonical_road,(),meta.canonical_geo,meta.public_location_display_cn,(meta.market_cluster,) if meta.market_cluster else (),meta.cn_aliases,meta.search_aliases,st,"Chinese Project Naming Layer V1","","",meta.notes,False,meta.occupancy_type))
ACTIVE_PROJECTS=tuple((*_OVERLAID,*_NAMING_ONLY))
if len({p.entity_id for p in ACTIVE_PROJECTS})!=len(ACTIVE_PROJECTS):raise RuntimeError("duplicate active project entity_id")
if len({p.key for p in ACTIVE_PROJECTS})!=len(ACTIVE_PROJECTS):raise RuntimeError("duplicate active project key")
BY_ENTITY={p.entity_id:p for p in ACTIVE_PROJECTS}
def _key_variants(v):
    k=_clean(v);k=k.split(":",1)[1] if k.startswith("project:") else k;k=k.split(":",1)[1] if k.startswith("new:") else k
    return tuple(dict.fromkeys((k,k.replace("-","_"),k.replace("_","-"))))
BY_KEY={}
for p in ACTIVE_PROJECTS:
    for k in _key_variants(p.key):BY_KEY.setdefault(k,p)
FAMILY_BY_ID={f.family_id:f for f in FAMILIES};_ALIAS_INDEX={}
for row in ALIAS_ROWS:_ALIAS_INDEX.setdefault(_norm(row["raw_alias"]),[]).append(row)
def project_by_key(key):
    k=_clean(key);direct=BY_ENTITY.get(k if k.startswith("project:") else "project:"+k) or BY_ENTITY.get("project:new:"+k)
    if direct:return direct
    for v in _key_variants(k):
        if v in BY_KEY:return BY_KEY[v]
    return None
_GENERIC_PROJECT_CONTEXT_ALIASES={_norm(x) for x in ("BKK","BKK1","BKK2","BKK3","百色河","钻石岛","堆谷","TK","森速","水净华","60米","60米大道","50米","50米路","一号路","1号路","NR1","271","598","永旺1","永旺2","永旺3","AEON1","AEON2","AEON3","Norea","金界","金街")}
def _is_generic_project_context(alias):return _norm(alias) in _GENERIC_PROJECT_CONTEXT_ALIASES
def _contains(text,alias):
    t=_clean(text).casefold();a=_clean(alias).casefold()
    if not t or not a:return False
    return bool(re.search(rf"(?<![a-z0-9]){re.escape(a)}(?![a-z0-9])",t)) if re.search(r"[a-z0-9]",a) else a in t
def _resolution(p,row,raw,action="",conflict=False):
    return Resolution(p.key,p.entity_id,p.name,p.family,p.project_type,row.get("raw_alias",""),row.get("alias_kind",""),int(float(row.get("weight") or 0)),p.verification_status,tuple(x for x in p.market_positioning if _contains(raw,x)),tuple(x for x in (p.canonical_geo_display,p.primary_road) if x and _contains(raw,x)),False,resolution_action=action or row.get("resolution_action",""),conflict_flag=conflict,occupancy_type=p.occupancy_type)
def _from_chinese(raw):
    c=resolve_chinese_project(raw)
    if c.canonical_identity:
        p=BY_ENTITY.get(c.canonical_identity)
        if p:return Resolution(p.key,p.entity_id,p.name,p.family,p.project_type,c.matched_alias,c.alias_kind,c.match_weight,p.verification_status,tuple(x for x in p.market_positioning if _contains(raw,x)),tuple(x for x in (p.canonical_geo_display,p.primary_road) if x and _contains(raw,x)),False,c.family_id,(),(),c.resolution_action,c.conflict_flag,p.occupancy_type)
    if c.ambiguity:
        candidates=tuple(x for x in c.candidate_projects if x in BY_ENTITY)
        if not candidates and c.family_id in FAMILY_BY_ID:candidates=tuple(x for x in FAMILY_BY_ID[c.family_id].child_projects if x in BY_ENTITY)
        scores=tuple((eid,max((a.weight for a in CHINESE_ALIAS_ROWS if a.canonical_identity==eid and _contains(raw,a.raw_alias)),default=20)) for eid in candidates)
        return Resolution(match_alias=c.matched_alias,match_type=c.alias_kind,confidence=c.match_weight,verification_status="UNKNOWN",ambiguity=True,family_id=c.family_id,candidates=candidates,candidate_scores=tuple(sorted(scores,key=lambda x:x[1],reverse=True)),resolution_action=c.resolution_action,conflict_flag=c.conflict_flag)
    return None
def resolve_project(text):
    raw=_clean(text)
    if not raw:return Resolution()
    if _contains(raw,"Chip Mong") or _contains(raw,"集茂"):
        for token,eid in (("271","project:chip-mong-landmark-271"),("598","project:chip-mong-park-land-598"),("50","project:chip-mong-park-land-50m")):
            if token in raw and eid in BY_ENTITY:
                p=BY_ENTITY[eid];return Resolution(p.key,p.entity_id,p.name,p.family,p.project_type,"Chip Mong+"+token,"PROJECT_ROAD",90,p.verification_status,(),(token,),False,occupancy_type=p.occupancy_type)
    chinese=_from_chinese(raw)
    if chinese and (chinese.project_entity_id or chinese.ambiguity):return chinese
    exact=_ALIAS_INDEX.get(_norm(raw),[])
    if exact:
        families=[r for r in exact if r["project_entity_id"].startswith("family:")];projects=[r for r in exact if r["project_entity_id"].startswith("project:") and r["project_entity_id"] in BY_ENTITY];owners=tuple(dict.fromkeys(r["project_entity_id"] for r in projects))
        if families or len(owners)>1 or any(str(r.get("conflict_flag","")).lower()=="true" for r in exact):
            fid=families[0]["project_entity_id"] if families else "";candidates=FAMILY_BY_ID[fid].child_projects if fid in FAMILY_BY_ID else owners;candidates=tuple(eid for eid in candidates if eid in BY_ENTITY)
            if len(owners)==1 and projects:return _resolution(BY_ENTITY[owners[0]],projects[0],raw)
            scores=[(eid,max((int(float(r.get("weight") or 0)) for r in projects if r["project_entity_id"]==eid),default=20)) for eid in candidates];scores.sort(key=lambda x:x[1],reverse=True)
            return Resolution(match_alias=raw,match_type="AMBIGUOUS_FAMILY",confidence=max((x[1] for x in scores),default=20),ambiguity=True,family_id=fid,candidates=tuple(x[0] for x in scores),candidate_scores=tuple(scores))
        if len(projects)==1:return _resolution(BY_ENTITY[projects[0]["project_entity_id"]],projects[0],raw)
    scored=[]
    for row in ALIAS_ROWS:
        if _is_generic_project_context(row["raw_alias"]) or not _contains(raw,row["raw_alias"]):continue
        p=BY_ENTITY.get(row["project_entity_id"]);scored.append((int(float(row.get("weight") or 0)),len(row["raw_alias"]),row,p))
    if not scored:return Resolution()
    scored.sort(key=lambda x:(x[0],x[1]),reverse=True);concrete=[x for x in scored if x[3]]
    if concrete:
        top=concrete[0][0];owners=tuple(dict.fromkeys(x[3].entity_id for x in concrete if x[0]==top))
        if len(owners)==1:return _resolution(concrete[0][3],concrete[0][2],raw)
        return Resolution(match_alias=concrete[0][2]["raw_alias"],match_type=concrete[0][2]["alias_kind"],confidence=top,ambiguity=True,candidates=owners)
    row=scored[0][2];fid=row["project_entity_id"];f=FAMILY_BY_ID.get(fid)
    return Resolution(match_alias=row["raw_alias"],match_type=row["alias_kind"],confidence=int(float(row.get("weight") or 20)),ambiguity=True,family_id=fid,candidates=f.child_projects if f else ())
def project_identity_aliases(project):
    p=project if isinstance(project,Project) else project_by_key(project)
    if not p:return ()
    chinese=chinese_aliases_for_project(p.entity_id);legacy=[r["raw_alias"] for r in ALIAS_ROWS if r["project_entity_id"]==p.entity_id and r["alias_kind"] in {"EXACT_PROJECT_NAME","STRONG_PROJECT_ALIAS"} and not _is_generic_project_context(r["raw_alias"])]
    return tuple(dict.fromkeys((*chinese,*legacy)))
def project_search_terms(text,*,residential_only=True):
    r=resolve_project(text);targets=(r.project_entity_id,) if r.project_entity_id else (r.candidates if r.ambiguity else ());terms=[]
    for target in targets:
        p=BY_ENTITY.get(target)
        if not p or (residential_only and not p.residential_eligible):continue
        terms.extend((*project_identity_aliases(p),*p.aliases))
    return tuple(dict.fromkeys(x for x in terms if x))
def property_types_for_project(project):
    p=project if isinstance(project,Project) else project_by_key(project)
    if not p:return ()
    return tuple(dict(r) for r in PROPERTY_TYPE_ROWS if r["project_entity_id"]==p.entity_id)
def canonical_location_projection(project):
    p=project if isinstance(project,Project) else project_by_key(project)
    if not p or not p.location_authoritative:return {"canonical_geo":"","road":"","public_location_display":""}
    return {"canonical_geo":_clean(p.canonical_geo_display),"road":_clean(p.primary_road),"public_location_display":_clean(p.public_location_display_cn or p.canonical_geo_display or p.primary_road)}
def public_location_for_project(project):return canonical_location_projection(project)["public_location_display"]
def research_project_count():return len(RESEARCH_PROJECTS)
def active_project_count():return len(ACTIVE_PROJECTS)
def legacy_only_project_count():return sum(1 for p in ACTIVE_PROJECTS if p.legacy_only)
def active_alias_count():
    from .chinese_project_naming import unique_normalized_alias_count
    return unique_normalized_alias_count()
def ambiguous_family_alias_count():
    from .chinese_project_naming import ambiguous_alias_count
    return ambiguous_alias_count()
