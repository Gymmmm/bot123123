from qiaolian_dual.adapters import ADAPTERS
from qiaolian_dual.adapters.appointment import AppointmentAdapter
from qiaolian_dual.adapters.deeplink import DeeplinkAdapter
from qiaolian_dual.adapters.gallery import GalleryAdapter
from qiaolian_dual.adapters.listing_session import ListingSessionAdapter
from qiaolian_dual.adapters.repair import RepairAdapter


def test_six_adapters_locked():
    assert ADAPTERS == (
        "PublicInventoryAdapter",
        "ListingSessionAdapter",
        "GalleryAdapter",
        "DeeplinkAdapter",
        "AppointmentAdapter",
        "RepairAdapter",
    )


def test_listing_session_prev_next_and_drop():
    session = ListingSessionAdapter()
    session.set_results(["QL-PP-AAAA", "QL-PP-BBBB", "QL-PP-CCCC"], index=1)
    assert session.current() == "QL-PP-BBBB"
    assert session.next() == "QL-PP-CCCC"
    assert session.prev() == "QL-PP-BBBB"
    assert session.drop_unavailable({"QL-PP-BBBB"}) == "QL-PP-CCCC"


def test_gallery_media_flag_and_text_return():
    gallery = GalleryAdapter()
    gallery.load(("file_a", "file_b"))
    assert gallery.current() == "file_a"
    assert gallery.should_edit_media() is False
    assert gallery.next() == "file_b"
    assert gallery.should_edit_media() is True
    gallery.back_to_text_panel()
    assert gallery.should_edit_media() is False


def test_deeplink_property_and_book_video():
    parser = DeeplinkAdapter()
    assert parser.parse("property_QL-PP-AAAA_details")["action"] == "detail"
    assert parser.parse("property_QL-PP-AAAA_book")["action"] == "book"
    assert parser.parse("book_video_QL-PP-AAAA")["action"] == "video"
    assert parser.parse("want_home") is None


def test_appointment_duplicate_and_unbookable():
    class Inventory:
        def __init__(self, bookable):
            self.bookable = bookable

        def get_listing(self, public_id):
            return {"listing_id": public_id, "bookable": self.bookable}

    created = {}

    def create(payload):
        created.update(payload)
        return 9

    def list_appts(_user_id, limit=20):
        return [{"listing_id": "QL-PP-AAAA", "status": "pending"}]

    blocked = AppointmentAdapter(
        inventory=Inventory(True), create_appointment=create, list_appointments=list_appts
    )
    try:
        blocked.submit({"user_id": 1, "listing_id": "QL-PP-AAAA"})
        raise AssertionError("duplicate must fail")
    except ValueError as exc:
        assert str(exc) == "appointment_duplicate"

    unbookable = AppointmentAdapter(inventory=Inventory(False), create_appointment=create)
    try:
        unbookable.submit({"user_id": 1, "listing_id": "QL-PP-AAAA"})
        raise AssertionError("unbookable must fail")
    except ValueError as exc:
        assert str(exc) == "listing_not_bookable"

    ok = AppointmentAdapter(inventory=Inventory(True), create_appointment=create)
    assert ok.submit({"user_id": 1, "listing_id": "QL-PP-BBBB"}) == 9
    assert created["listing_id"] == "QL-PP-BBBB"
    assert created["status"] == "pending"


def test_repair_requires_dual_binding_and_stable_token():
    def no_binding(_user_id):
        return None

    adapter = RepairAdapter(get_active_binding=no_binding, persist_ticket=lambda **k: 1)
    try:
        adapter.create(
            user_id=7,
            issue_key="repair_ac",
            issue_type="空调",
            description="x",
            time_slot="today",
            day="2026-09-22",
        )
        raise AssertionError("missing binding must fail")
    except ValueError as exc:
        assert str(exc) == "tenant_binding_required"

    seen = {}

    def persist(**kwargs):
        seen.update(kwargs)
        return {"id": 44, "created": True}

    ok = RepairAdapter(
        get_active_binding=lambda _uid: {"id": 3, "property_name": "BKK1-12", "status": "active"},
        persist_ticket=persist,
    )
    first = ok.create(
        user_id=7,
        issue_key="repair_ac",
        issue_type="空调",
        description="x",
        time_slot="today",
        day="2026-09-22",
    )
    second_token = RepairAdapter.request_token(7, 3, "repair_ac", "2026-09-22")
    assert first["ticket_id"] == 44
    assert first["request_token"] == second_token
    assert seen["binding_id"] == 3
    assert seen["property_name"] == "BKK1-12"
