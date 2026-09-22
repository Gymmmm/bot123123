"""Chinese Project Naming Layer V1.

This is a naming/search overlay on Project Registry V3. It never rewrites
canonical identity or canonical GEO from a market/search alias.
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import csv, io, re, zlib

DATA_DIR = Path(__file__).with_name("data")
SOURCE_XLSX_SHA256="1bd29ace03913e9b476a993bddfcbbd485959ff710edd3e002a6ccc2702f1900"
SOURCE_ALIAS_CSV_SHA256="8933f17a24dde2ac3ee210091b3c3e13158fd784270a703150985c330930aa66"
ACTIONS={"DIRECT_RESOLVE","CANDIDATE_ONLY","RESOLVE_WITH_PROJECT_OR_GEO_CONTEXT","DEPRECATE_PROJECT_ALIAS","REDIRECT_NEEDS_VERIFICATION","SEARCH_ONLY_LEGACY"}

def _clean(v): return re.sub(r"\s+"," ",str(v or "").replace("\u00a0"," ")).strip()
def normalize_alias(v): return re.sub(r"[\s·._\-/]+","",_clean(v).casefold())
def _split(v): return tuple(x.strip() for x in re.split(r"[;；]",str(v or "")) if x.strip())
def _rows(name):
    if name=="pp_chinese_project_naming_v1.csv":
        raw=zlib.decompress((DATA_DIR/"pp_chinese_project_naming_v1.csv.zlib").read_bytes()).decode("utf-8-sig")
        return tuple(dict(r) for r in csv.DictReader(io.StringIO(raw)))
    with (DATA_DIR/name).open(encoding="utf-8-sig",newline="") as f:return tuple(dict(r) for r in csv.DictReader(f))
@dataclass(frozen=True)
class ChineseProjectName:
    preferred_cn_name:str;cn_aliases:tuple[str,...];canonical_name_en:str;project_type:str;occupancy_type:str;family_name:str
    canonical_identity:str;legacy_project_id:str;entity_level:str;market_cluster:str;canonical_geo:str;canonical_road:str
    public_location_display_cn:str;nearby_landmarks:tuple[str,...];search_aliases:tuple[str,...];verification_status:str;notes:str
@dataclass(frozen=True)
class ChineseAlias:
    raw_alias:str;normalized_alias:str;entity_level:str;target_display_name:str;canonical_identity:str;alias_kind:str
    weight:int;search_only:bool;conflict_flag:bool;resolution_action:str;verification_status:str;notes:str
@dataclass(frozen=True)
class ChineseResolution:
    canonical_identity:str="";project_key:str="";preferred_cn_name:str="";canonical_name_en:str="";family_id:str=""
    entity_level:str="";occupancy_type:str="";canonical_geo:str="";canonical_road:str="";chinese_market_cluster:str=""
    public_location_display_cn:str="";matched_alias:str="";normalized_alias:str="";alias_kind:str="";resolution_action:str=""
    match_weight:int=0;conflict_flag:bool=False;ambiguity:bool=False;candidate_projects:tuple[str,...]=();authoritative_direct:bool=False
def _bool(v): return _clean(v).casefold() in {"1","true","yes","y"}
def _name(r):
    return ChineseProjectName(_clean(r.get("中文常用名")),_split(r.get("中文别名")),_clean(r.get("英文/官方名")),_clean(r.get("项目类型")),
      _clean(r.get("住宅/写字楼/综合体属性")),_clean(r.get("所属项目family")),_clean(r.get("canonical project identity")),_clean(r.get("原V3 project_id")),
      _clean(r.get("identity_level")),_clean(r.get("华人地理板块")),_clean(r.get("canonical GEO")),_clean(r.get("道路")),_clean(r.get("中文位置展示")),
      _split(r.get("附近地标")),_split(r.get("搜索别名")),_clean(r.get("verification_status")) or "UNKNOWN",_clean(r.get("备注")))
def _alias(r):
    action=_clean(r.get("resolution_action"))
    if action not in ACTIONS:raise RuntimeError(f"unknown Chinese alias resolution_action: {action!r}")
    return ChineseAlias(_clean(r.get("raw_alias")),_clean(r.get("normalized_alias")) or normalize_alias(r.get("raw_alias")),_clean(r.get("entity_level")),
      _clean(r.get("target_display_name")),_clean(r.get("canonical_identity")),_clean(r.get("alias_kind")),int(float(r.get("weight") or 0)),_bool(r.get("search_only")),
      _bool(r.get("conflict_flag")),action,_clean(r.get("verification_status")) or "UNKNOWN",_clean(r.get("notes")))
NAMING_ROWS=tuple(_name(r) for r in _rows("pp_chinese_project_naming_v1.csv"))
ALIAS_ROWS=tuple(_alias(r) for r in _rows("pp_chinese_project_alias_final_v2.csv"))
if len(NAMING_ROWS)!=114:raise RuntimeError(f"Chinese naming row count drift: {len(NAMING_ROWS)}")
if len(ALIAS_ROWS)!=522:raise RuntimeError(f"Chinese alias row count drift: {len(ALIAS_ROWS)}")
PROJECT_NAMES=tuple(x for x in NAMING_ROWS if x.entity_level=="PROJECT")
FAMILY_NAMES=tuple(x for x in NAMING_ROWS if x.entity_level=="FAMILY")
NAME_BY_ID={x.canonical_identity:x for x in NAMING_ROWS if x.canonical_identity}
PROJECT_NAME_BY_ID={x.canonical_identity:x for x in PROJECT_NAMES}
ALIAS_INDEX={}
for row in ALIAS_ROWS:ALIAS_INDEX.setdefault(row.normalized_alias,[]).append(row)
def preferred_project_name(canonical_identity,fallback_cn="",fallback_en=""):
    row=PROJECT_NAME_BY_ID.get(_clean(canonical_identity));return (row.preferred_cn_name if row else "") or _clean(fallback_cn) or _clean(fallback_en)
def occupancy_type_for(canonical_identity):
    row=PROJECT_NAME_BY_ID.get(_clean(canonical_identity));return row.occupancy_type if row else ""
def residential_eligible(canonical_identity):
    occupancy=occupancy_type_for(canonical_identity).casefold()
    if not occupancy:return True
    return occupancy.strip() not in {"写字楼","office","office-only","office_only"}
def _project_key(identity):
    if not identity.startswith("project:"):return ""
    key=identity.split(":",1)[1]
    return key.split(":",1)[1] if key.startswith("new:") else key
def _contains(text,alias):
    t=_clean(text).casefold();a=_clean(alias).casefold()
    if not t or not a:return False
    return bool(re.search(rf"(?<![a-z0-9]){re.escape(a)}(?![a-z0-9])",t)) if re.search(r"[a-z0-9]",a) else a in t
def _candidate_ids(rows):
    ordered=sorted((r for r in rows if r.canonical_identity.startswith("project:") and r.resolution_action!="DEPRECATE_PROJECT_ALIAS"),key=lambda r:(r.weight,len(r.raw_alias)),reverse=True)
    return tuple(dict.fromkeys(r.canonical_identity for r in ordered))
def _result(row,direct,ambiguity=False,candidates=()):
    meta=PROJECT_NAME_BY_ID.get(row.canonical_identity);identity=row.canonical_identity if direct and row.canonical_identity.startswith("project:") else ""
    return ChineseResolution(identity,_project_key(identity),meta.preferred_cn_name if meta else row.target_display_name,meta.canonical_name_en if meta else "",
      row.canonical_identity if row.canonical_identity.startswith("family:") else "",row.entity_level,meta.occupancy_type if meta else "",meta.canonical_geo if meta else "",
      meta.canonical_road if meta else "",meta.market_cluster if meta else "",meta.public_location_display_cn if meta else "",row.raw_alias,row.normalized_alias,row.alias_kind,
      row.resolution_action,row.weight,row.conflict_flag,ambiguity,candidates,direct and row.resolution_action=="DIRECT_RESOLVE" and not row.conflict_flag)
def resolve_chinese_project(text):
    raw=_clean(text)
    if not raw:return ChineseResolution()
    exact=list(ALIAS_INDEX.get(normalize_alias(raw),()))
    if exact:
        direct=[r for r in exact if r.resolution_action=="DIRECT_RESOLVE" and not r.conflict_flag and r.canonical_identity.startswith("project:")]
        owners=tuple(dict.fromkeys(r.canonical_identity for r in direct))
        if len(owners)==1:return _result(max((r for r in direct if r.canonical_identity==owners[0]),key=lambda r:r.weight),True)
        redirects=[r for r in exact if r.resolution_action=="REDIRECT_NEEDS_VERIFICATION" and r.canonical_identity.startswith("project:")]
        if len({r.canonical_identity for r in redirects})==1:return _result(max(redirects,key=lambda r:r.weight),True)
        family=next((r for r in exact if r.canonical_identity.startswith("family:")),None);candidates=_candidate_ids(exact)
        if family or candidates:return _result(family or max(exact,key=lambda r:r.weight),False,True,candidates)
        deprecated=max(exact,key=lambda r:r.weight)
        if deprecated.resolution_action in {"DEPRECATE_PROJECT_ALIAS","SEARCH_ONLY_LEGACY"}:
            candidates=tuple(dict.fromkeys(r.canonical_identity for r in exact if r.canonical_identity.startswith("project:")))
            return _result(deprecated,False,True,candidates)
    hits=[r for r in ALIAS_ROWS if r.raw_alias and _contains(raw,r.raw_alias)]
    if not hits:return ChineseResolution()
    hits.sort(key=lambda r:(r.weight,len(r.raw_alias)),reverse=True);top_weight=hits[0].weight;top=[r for r in hits if r.weight==top_weight]
    direct=[r for r in top if r.resolution_action=="DIRECT_RESOLVE" and not r.conflict_flag and r.canonical_identity.startswith("project:")]
    owners=tuple(dict.fromkeys(r.canonical_identity for r in direct))
    if len(owners)==1:return _result(max((r for r in direct if r.canonical_identity==owners[0]),key=lambda r:len(r.raw_alias)),True)
    family=next((r for r in hits if r.canonical_identity.startswith("family:")),None);candidates=_candidate_ids(hits)
    if family or candidates:return _result(family or hits[0],False,True,candidates)
    return ChineseResolution()
def aliases_for_project(canonical_identity,include_search_only=True):
    identity=_clean(canonical_identity);rows=[r for r in ALIAS_ROWS if r.canonical_identity==identity and r.resolution_action!="DEPRECATE_PROJECT_ALIAS" and (include_search_only or not r.search_only)]
    return tuple(dict.fromkeys(r.raw_alias for r in sorted(rows,key=lambda r:(r.weight,len(r.raw_alias)),reverse=True)))
def office_only_project_ids():return tuple(x.canonical_identity for x in PROJECT_NAMES if not residential_eligible(x.canonical_identity))
def resolution_action_counts():
    out={k:0 for k in ACTIONS}
    for row in ALIAS_ROWS:out[row.resolution_action]+=1
    return out
def unique_normalized_alias_count():return len({r.normalized_alias for r in ALIAS_ROWS if r.normalized_alias})
def ambiguous_alias_count():return len({r.normalized_alias for r in ALIAS_ROWS if r.conflict_flag})
def family_alias_count():return sum(1 for r in ALIAS_ROWS if r.entity_level.startswith("FAMILY"))
__all__=["ACTIONS","ALIAS_ROWS","FAMILY_NAMES","NAMING_ROWS","PROJECT_NAMES","PROJECT_NAME_BY_ID","ChineseAlias","ChineseProjectName","ChineseResolution","aliases_for_project","ambiguous_alias_count","family_alias_count","normalize_alias","occupancy_type_for","office_only_project_ids","preferred_project_name","residential_eligible","resolution_action_counts","resolve_chinese_project","unique_normalized_alias_count"]
