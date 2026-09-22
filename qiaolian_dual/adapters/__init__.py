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


def _install_start_arg_hook() -> None:
    from qiaolian_dual import session_deeplink as session

    if getattr(session.parse_start_arg_payload, "_qiaolian_deeplink_hook", False):
        return

    original = session.parse_start_arg_payload

    def parse_start_arg_payload(arg: str):
        modern = DeeplinkAdapter().parse(arg)
        if modern is not None:
            return {
                "action": modern["action"],
                "target": modern.get("target") or "",
                "post_token": "",
                "channel_message_id": None,
                "source": modern.get("source") or "channel",
                "channel_return": bool(modern.get("channel_return")),
            }
        return original(arg)

    parse_start_arg_payload._qiaolian_deeplink_hook = True  # type: ignore[attr-defined]
    session.parse_start_arg_payload = parse_start_arg_payload


_install_start_arg_hook()
