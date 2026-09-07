"""Single publishability contract shared by package build, approval and publisher preflight."""
from __future__ import annotations
from typing import Any

PUBLISHABLE_LOCATION_LEVELS = {
    'level_2_physical_confirmed',
    'level_1_market_confirmed',
    'level_1_project_confirmed',
}

def evaluate_publishability(facts: dict[str, Any], *, media_count: int = 0, cover_exists: bool = False) -> dict[str, Any]:
    facts = facts if isinstance(facts, dict) else {}
    quality = facts.get('quality') if isinstance(facts.get('quality'), dict) else {}
    blocking: list[str] = []
    warnings: list[str] = []
    if facts.get('schema_version') != 'canonical_facts.v1':
        blocking.append('canonical_schema_invalid')
    if facts.get('deal_type') != 'rent':
        blocking.append('deal_type_not_rent')
    location_level = str(facts.get('publication_location_level') or '').strip()
    public_location = str(facts.get('public_location_display') or '').strip()
    if location_level not in PUBLISHABLE_LOCATION_LEVELS or not public_location or not facts.get('public_location_key'):
        # A missing location is commercially weaker but not a false fact.  Keep
        # it blank in public copy and let the administrator publish as
        # "location on request" instead of forcing invented geography.
        warnings.append('missing_public_location')
    if not str(facts.get('property_type') or '').strip():
        blocking.append('missing_property_type')
    if not str(facts.get('layout') or '').strip():
        blocking.append('missing_layout')
    try:
        rent = float(facts.get('monthly_rent_usd') or 0)
    except (TypeError, ValueError):
        rent = 0
    if rent <= 0:
        blocking.append('missing_rent')
    if not str(facts.get('canonical_facts_hash') or '').strip():
        blocking.append('missing_canonical_hash')
    if int(media_count or 0) < 4:
        blocking.append('insufficient_media')
    if not cover_exists:
        blocking.append('missing_cover')
    blocking.extend(
        str(x) for x in (quality.get('blocking_flags') or [])
        if str(x).strip() and str(x).strip() != 'missing_public_location'
    )
    if not facts.get('deposit_payment_terms'):
        warnings.append('missing_deposit_details')
    if not facts.get('contract_term_display'):
        warnings.append('missing_contract_details')
    if not any(facts.get(k) for k in ('management_fee','internet_fee','water_rate','electric_rate','parking_fee')):
        warnings.append('missing_recurring_cost_details')
    if not facts.get('size_sqm') and not facts.get('land_dimension') and not facts.get('building_dimension'):
        warnings.append('missing_size')
    if not facts.get('highlights'):
        warnings.append('missing_highlights')
    blocking = list(dict.fromkeys(blocking))
    warnings = list(dict.fromkeys(warnings))
    return {'ok': not blocking, 'blocking': blocking, 'warnings': warnings, 'score': 100 if not blocking else 0}
