"""3858 User Bot adapters. Six modules only. No V3 Telegram/IA."""

from .appointment import AppointmentAdapter
from .deeplink import DeeplinkAdapter
from .gallery import GalleryAdapter
from .listing_session import ListingSessionAdapter
from .public_inventory import PublicInventoryAdapter
from .repair import RepairAdapter

ADAPTERS = (
    "PublicInventoryAdapter",
    "ListingSessionAdapter",
    "GalleryAdapter",
    "DeeplinkAdapter",
    "AppointmentAdapter",
    "RepairAdapter",
)

__all__ = [
    "ADAPTERS",
    "AppointmentAdapter",
    "DeeplinkAdapter",
    "GalleryAdapter",
    "ListingSessionAdapter",
    "PublicInventoryAdapter",
    "RepairAdapter",
]
