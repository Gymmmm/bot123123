from __future__ import annotations
import json, sqlite3, uuid
from .canonical_fact_projection import facts_hash, validate_facts
from .canonical_facts import draft_projection
from .listing_taxonomy import resolve_location_alias

_REPAIRABLE_GEOGRAPHY_ERRORS = {
    'canonical_area_not_in_physical_catalog',
    'canonical_area_display_missing',
    'canonical_area_display_mismatch',
    'canonical_area_key_missing_for_display',
    'canonical_area_status_mismatch',
    'canonical_area_level_without_area',
    'city_used_as_canonical_area',
    'city_used_as_public_location',
    'public_location_key_display_incomplete',
    'market_location_candidates_invalid',
    'market_location_candidate_count_mismatch',
    'market_location_candidate_not_in_catalog',
    'market_location_candidate_display_mismatch',
    'publication_location_level_invalid',
    'level_2_physical_area_missing',
    'level_2_public_location_mismatch',
    'level_1_market_not_in_catalog',
    'level_1_market_display_mismatch',
    'level_1_market_not_in_fact_candidates',
    'market_location_promoted_to_canonical_area',
    'level_1_project_location_mismatch',
    'level_1_project_display_mismatch',
    'unknown_level_has_public_location',
}

def ensure_area_audit_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS area_change_audit (
        audit_id TEXT PRIMARY KEY, listing_id TEXT NOT NULL, draft_id TEXT NOT NULL,
        old_area TEXT, new_area TEXT NOT NULL, operator_user_id TEXT NOT NULL,
        changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, reason TEXT NOT NULL,
        old_snapshot_json TEXT NOT NULL DEFAULT '{}', new_snapshot_json TEXT NOT NULL DEFAULT '{}'
    )""")

def set_canonical_area(db_path: str, listing_id: str, canonical_area: str, operator_user_id: str, reason: str) -> dict:
    resolution = resolve_location_alias(canonical_area)
    if not resolution:
        raise ValueError("area_not_in_canonical_catalog")
    area = resolution.display
    if not str(reason or '').strip():
        raise ValueError("area_change_reason_required")
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        ensure_area_audit_schema(conn)
        row = conn.execute("SELECT * FROM drafts WHERE listing_id=? ORDER BY id DESC LIMIT 1", (listing_id,)).fetchone()
        if not row:
            raise ValueError("draft_for_listing_not_found")
        draft=dict(row); draft_id=str(draft['draft_id'])
        frozen = conn.execute(
            "SELECT package_id FROM publication_packages WHERE draft_id=? AND status IN ('approved','published') LIMIT 1",
            (draft_id,),
        ).fetchone()
        if frozen:
            raise ValueError("approved_package_frozen_requires_new_draft")
        published=conn.execute("SELECT 1 FROM posts WHERE draft_id=? AND platform='telegram' AND publish_status IN ('published','success','ok') LIMIT 1", (draft_id,)).fetchone()
        if published:
            raise ValueError("published_listing_area_change_requires_republish_review")
        old_area=str(draft.get('area') or '')
        try: old_norm=json.loads(draft.get('normalized_data') or '{}')
        except Exception: old_norm={}
        old_errors = validate_facts(old_norm)
        # The retired area tool appended only these two legacy keys without
        # updating the hash. Accept that exact, provable legacy shape so the
        # repair tool can recover it; do not waive arbitrary hash corruption.
        if 'canonical_facts_hash_mismatch' in old_errors:
            legacy_probe=dict(old_norm)
            legacy_probe.pop('area', None)
            legacy_probe.pop('normalized_area', None)
            if facts_hash(legacy_probe) == old_norm.get('canonical_facts_hash'):
                old_errors=[error for error in old_errors if error != 'canonical_facts_hash_mismatch']
        non_repairable = [error for error in old_errors if error not in _REPAIRABLE_GEOGRAPHY_ERRORS]
        if non_repairable:
            raise ValueError("canonical_facts_invalid_before_area_override:" + ",".join(non_repairable))
        new_norm=dict(old_norm)
        new_norm.pop('area', None)
        new_norm.pop('normalized_area', None)
        evidence=dict(new_norm.get('evidence') or {})
        manual_evidence={'value':resolution.key,'source':'manual_admin_override','confidence':'operator_confirmed','raw_excerpt':str(reason).strip()[:240]}
        if resolution.kind == 'physical_area':
            new_norm['canonical_area_key']=resolution.key
            new_norm['canonical_area_display']=resolution.display
            new_norm['canonical_area_level']=resolution.canonical_area_level
            new_norm['area_status']='confirmed'
            evidence['canonical_area_key']=[manual_evidence]
        else:
            # A market/search location is safe for public Level 1 display but
            # must never be promoted into a Level 2 physical area.
            new_norm['canonical_area_key']=None
            new_norm['canonical_area_display']=None
            new_norm['canonical_area_level']=None
            new_norm['area_status']='unconfirmed'
            new_norm['market_location_keys']=[resolution.key]
            new_norm['market_location_displays']=[resolution.display]
            evidence['canonical_area_key']=[]
            evidence['market_location_keys']=[manual_evidence]
        new_norm['public_location_key']=resolution.key
        new_norm['public_location_display']=resolution.display
        new_norm['publication_location_level']=resolution.publication_level
        new_norm['evidence']=evidence
        quality=dict(new_norm.get('quality') or {})
        resolved_area_flags={'missing_public_location','ambiguous_area','ambiguous_market_location'}
        for key in ('hard_flags','review_flags','blocking_flags'):
            quality[key]=[flag for flag in (quality.get(key) or []) if flag not in resolved_area_flags]
        info=[flag for flag in (quality.get('info_flags') or []) if flag not in {'manual_area_override','geo_precision_unconfirmed'}]
        if resolution.kind != 'physical_area':
            info.append('geo_precision_unconfirmed')
        if 'manual_area_override' not in info:
            info.append('manual_area_override')
        quality['info_flags']=info
        quality['blocking_flags']=list(dict.fromkeys([*(quality.get('hard_flags') or []), *(quality.get('review_flags') or [])]))
        quality['all_flags']=list(dict.fromkeys([*quality['blocking_flags'], *(quality.get('warning_flags') or []), *info]))
        quality['score']=max(0, 100 - 30 * len(quality.get('hard_flags') or []) - 12 * len(quality.get('review_flags') or []) - 4 * len(quality.get('warning_flags') or []))
        new_norm['quality']=quality
        new_norm['candidate_flags']=[
            flag for flag in (new_norm.get('candidate_flags') or [])
            if flag not in {'ambiguous_area','ambiguous_market_location'}
        ]
        new_norm['canonical_facts_hash']=facts_hash(new_norm)
        new_errors=validate_facts(new_norm)
        if new_errors:
            raise ValueError("canonical_facts_invalid_after_area_override:" + ",".join(new_errors))
        projection=draft_projection(new_norm)
        audit_id='AREA_'+str(uuid.uuid4())
        conn.execute("""UPDATE drafts SET title=?,area=?,normalized_data=?,extracted_data=?,
                     canonical_facts_hash=?,canonical_facts_schema=?,public_location_key=?,
                     public_location_display=?,publication_location_level=?,canonical_area_key=?,
                     review_status='pending',review_note=?,queue_score=?,updated_at=CURRENT_TIMESTAMP WHERE draft_id=?""",
                     (projection['title'],projection['area'],json.dumps(new_norm,ensure_ascii=False,sort_keys=True),json.dumps(new_norm,ensure_ascii=False,sort_keys=True),
                      new_norm['canonical_facts_hash'],new_norm.get('schema_version'),resolution.key,resolution.display,resolution.publication_level,new_norm.get('canonical_area_key'),
                      f'area_manual_override:{old_area or "(empty)"}->{area}; pending_reapproval',quality['score'],draft_id))
        conn.execute("""UPDATE listings SET title=?,area=?,public_location_key=?,public_location_display=?,
                     publication_location_level=?,canonical_area_key=?,canonical_facts_hash=?,canonical_facts_schema=?,
                     status=CASE WHEN status='active' THEN 'pending' ELSE status END,updated_at=CURRENT_TIMESTAMP WHERE listing_id=?""",
                     (projection['title'],projection['area'],resolution.key,resolution.display,resolution.publication_level,new_norm.get('canonical_area_key'),new_norm['canonical_facts_hash'],new_norm.get('schema_version'),listing_id))
        conn.execute("UPDATE publication_packages SET status='superseded',updated_at=CURRENT_TIMESTAMP WHERE draft_id=? AND status='package_ready'", (draft_id,))
        ensure_area_audit_schema(conn)
        conn.execute("INSERT INTO area_change_audit(audit_id,listing_id,draft_id,old_area,new_area,operator_user_id,reason,old_snapshot_json,new_snapshot_json) VALUES(?,?,?,?,?,?,?,?,?)", (audit_id,listing_id,draft_id,old_area,area,str(operator_user_id),str(reason).strip(),json.dumps({'area':old_area,'normalized_data':old_norm},ensure_ascii=False),json.dumps({'area':area,'normalized_data':new_norm},ensure_ascii=False)))
        conn.commit()
    return {
        'audit_id':audit_id,'listing_id':listing_id,'draft_id':draft_id,
        'old_area':old_area,'new_area':area,'location_kind':resolution.kind,
        'publication_location_level':resolution.publication_level,
    }
