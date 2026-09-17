"""V3 inventory-domain extraction."""

from . import listing_taxonomy as _listing_taxonomy
from .phnom_penh_aliases import apply_phnom_penh_aliases as _apply_phnom_penh_aliases

_apply_phnom_penh_aliases(_listing_taxonomy)

# Registry extends existing canonical project identities; it does not create
# a second project identity store.
from .phnom_penh_location_registry import PROJECT_IDENTITIES as _registry_projects
_registry_by_key = {item.key: item for item in _registry_projects}
_listing_taxonomy.PROJECT_IDENTITIES = tuple(
    _listing_taxonomy.ProjectIdentity(
        item.key,
        _registry_by_key[item.key].canonical_project_name if item.key in _registry_by_key else item.display,
        item.kind,
        tuple(dict.fromkeys((*item.aliases, *(_registry_by_key[item.key].project_aliases if item.key in _registry_by_key else ())))),
        item.property_family,
    )
    for item in _listing_taxonomy.PROJECT_IDENTITIES
)

# Defensive gate for market-maintained alias extensions.  A malformed
# single-item Python tuple can otherwise be iterated as characters, and a
# whitespace character would normalize to an empty alias that matches every
# listing.  Never allow blank aliases into the active taxonomy.
_listing_taxonomy.PROJECT_IDENTITIES = tuple(
    _listing_taxonomy.ProjectIdentity(
        item.key,
        item.display,
        item.kind,
        tuple(alias for alias in item.aliases if _listing_taxonomy.clean_text(alias)),
        item.property_family,
    )
    for item in _listing_taxonomy.PROJECT_IDENTITIES
)
_listing_taxonomy.MARKET_LOCATIONS = tuple(
    _listing_taxonomy.MarketLocation(
        item.key,
        item.display,
        item.relation,
        tuple(alias for alias in item.aliases if _listing_taxonomy.clean_text(alias)),
    )
    for item in _listing_taxonomy.MARKET_LOCATIONS
)

__all__ = []
