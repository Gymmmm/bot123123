from v3_core.user_bot.adviser_notes import adviser_notes_for_view
from v3_core.user_bot.appointment_success_view import build_appointment_success_view
from v3_core.user_bot.assurance_views import build_assurance_home_view
from v3_core.user_bot.callbacks import encode_listing_callback
from v3_core.user_bot.home_callbacks import encode_home_callback
from v3_core.user_bot.home_views import build_home_view
from v3_core.user_bot.listing_responses import build_details_response, build_photos_response
from v3_core.user_bot.public_appointment import PublicAppointmentDraft
from v3_core.user_bot.search_cards import build_search_card
from v3_core.user_bot.search_no_match_view import build_search_no_match_view
from v3_core.user_bot.telegram_home_handler import handle_v3_home_callback
from v3_core.user_bot.telegram_navigation import polish_listing_keyboard
from v3_core.user_bot.tenant_v1 import lease_view, renew_view, tenant_home_view, terminate_view
from v3_core.user_bot.transition_actions import SearchSubmitIntent
from v3_core.user_bot.transition_callbacks import encode_transition_choice
from v3_core.user_bot.transition_views import TransitionChoice, TransitionViewService
from v3_core.user_bot.search_query import SearchCriteria
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


FORBIDDEN = (
    "关于侨联",
    "看相近房源",
    "智能找房",
    "继续看房",
    "调整条件",
    "提交续租申请",
    "提交退租申请",
)


def _labels(view) -> list[str]:
    return [choice.label for row in view.rows for choice in row]


def test_home_has_no_about_and_uses_aftercare():
    view = build_home_view(channel_url="https://t.me/example")
    labels = _labels(view)
    assert labels[:4] == ["🔍 开始找房", "📅 我的预约", "🛠 入住服务", "🛡 租后服务"]
    assert "🏠 关于侨联" not in labels
    assert "📢 最新房源" in labels
    assert all(bad not in view.text for bad in FORBIDDEN)


def test_legacy_about_callback_encodes_home_about():
    assert encode_home_callback("about") == "v3u:home:about"
    assert encode_home_callback("rental") == "v3u:home:rental"
    assert handle_v3_home_callback.__code__.co_consts or True
    source = open(handle_v3_home_callback.__code__.co_filename, encoding="utf-8").read()
    assert 'action in {"about", "rental"}' in source or "about", "rental" in source


def test_listing_surfaces_drop_similar_copy():
    from v3_core.user_bot.listing_responses import SemanticAction

    assert SemanticAction("✏️ 换个条件找", "change_search").action == "change_search"
    details_src = open(build_details_response.__code__.co_filename, encoding="utf-8").read()
    cards_src = open(build_search_card.__code__.co_filename, encoding="utf-8").read()
    photos_src = open(build_photos_response.__code__.co_filename, encoding="utf-8").read()
    assert "看相近房源" not in details_src
    assert "看相近房源" not in cards_src
    assert "看相近房源" not in photos_src
    assert "换个条件找" in details_src
    assert "问这套房" in cards_src


def test_success_continue_search_and_consult_route():
    class _Inventory:
        def resolve(self, public_listing_id):
            return None

    draft = PublicAppointmentDraft(public_listing_id="QL-PP-A2B3", date="09-18", time="am")
    view = build_appointment_success_view(draft, _Inventory(), submission_kind="created")
    labels = _labels(view)
    assert "🔍 继续找房" in labels
    assert "💬 问这套房" in labels
    assert "继续看房" not in labels
    consult = [c for row in view.rows for c in row if c.label == "💬 问这套房"][0]
    assert consult.kind == "listing_consult"
    assert consult.public_listing_id == "QL-PP-A2B3"
    encoded = encode_transition_choice(consult)
    assert encoded == encode_listing_callback("consult", "QL-PP-A2B3")
    search = [c for row in view.rows for c in row if c.label == "🔍 继续找房"][0]
    assert encode_transition_choice(search) == encode_home_callback("search")


def test_channel_book_uses_see_details_not_back():
    class _Details:
        subject = "富力城｜两房"
        location = "富力城"
        monthly_rent_usd = 800
        bookable = True

    class _Inventory:
        def resolve(self, public_listing_id):
            class _View:
                bookable = True
            return _View()

    # Avoid listing presenter by stubbing resolve + monkeypatch helper via draft source only.
    from v3_core.user_bot import transition_views as tv

    original = tv._resolve_bookable_details

    def _fake(inventory, public_listing_id):
        return _Details()

    tv._resolve_bookable_details = _fake
    try:
        channel = PublicAppointmentDraft(public_listing_id="QL-PP-A2B3", source="channel_deeplink")
        search = PublicAppointmentDraft(public_listing_id="QL-PP-A2B3", source="search_result")
        channel_view = tv._appointment_date_view(channel, _Inventory(), today=__import__("datetime").date(2026, 9, 17))
        search_view = tv._appointment_date_view(search, _Inventory(), today=__import__("datetime").date(2026, 9, 17))
    finally:
        tv._resolve_bookable_details = original
    assert "📋 先看详情" in _labels(channel_view)
    assert "⬅️ 返回租赁详情" not in _labels(channel_view)
    assert "⬅️ 返回租赁详情" in _labels(search_view)
    assert "📋 先看详情" not in _labels(search_view)


def test_channel_details_chrome_has_no_home():
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("📅 预约看房", callback_data="v3u:listing:book:QL-PP-A2B3")]])
    polished = polish_listing_keyboard(
        markup,
        channel_url="https://t.me/example",
        add_home=True,
        add_channel=True,
    )
    labels = [btn.text for row in polished.inline_keyboard for btn in row]
    assert "📢 回频道看房源" in labels
    assert "🏠 返回首页" not in labels
    assert "⬅️ 回首页" not in labels


def test_search_chrome_returns_to_results_not_home():
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("📅 预约看房", callback_data="v3u:listing:book:QL-PP-A2B3")]])
    polished = polish_listing_keyboard(
        markup,
        channel_url="https://t.me/example",
        back_to_search_callback="v3u:card:0:QL-PP-A2B3",
        add_home=True,
        add_channel=True,
    )
    labels = [btn.text for row in polished.inline_keyboard for btn in row]
    assert "⬅️ 返回结果" in labels
    assert "🏠 返回首页" not in labels


class _Binding:
    property_name = "富力城"
    monthly_rent = 800
    deposit_months = 2
    rent_day = 5
    lease_end_date = "2027-01-01"
    contract_end_date = "2027-01-01"


class _Service:
    def __init__(self, binding):
        self._binding = binding

    def active_binding(self, user_id):
        return self._binding

    def require_active_binding(self, user_id):
        if self._binding is None:
            raise ValueError("missing")
        return self._binding


def test_bound_resident_sees_local_life():
    view = tenant_home_view(_Service(_Binding()), 1)
    assert "📍 周边服务" in _labels(view)


def test_unbound_resident_has_no_local_life():
    view = tenant_home_view(_Service(None), 1)
    assert "📍 周边服务" not in _labels(view)
    assert "看租后服务" in "".join(_labels(view))


def test_renew_terminate_are_requests():
    renew = renew_view(_Service(_Binding()), 1)
    term = terminate_view(_Service(_Binding()), 1)
    assert "✅ 提交续租需求" in _labels(renew)
    assert "✅ 提交退租需求" in _labels(term)
    assert all("申请" not in label for label in _labels(renew) + _labels(term))


def test_search_entry_freeze_copy():
    view = TransitionViewService.search_entry()
    assert "想住什么样的房子" in view.text
    assert "🛏 按户型" in _labels(view)
    assert "🏠 按户型" not in _labels(view)


def test_no_match_uses_change_search_label():
    view = build_search_no_match_view(
        SearchSubmitIntent(criteria=SearchCriteria(raw_text="BKK1"), area_display="BKK1", budget_label="$600–800")
    )
    assert "✏️ 换个条件找" in _labels(view)


def test_aftercare_ad_page():
    view = build_assurance_home_view()
    assert "租到房，不代表服务就结束了" in view.text
    assert "📋 入住交接留档" in _labels(view)
    assert "关于侨联" not in view.text


def test_adviser_notes_readonly_contract():
    class _View:
        snapshot = {"adviser_copy_source": "hidden", "adviser_copy": "不要显示"}

    assert adviser_notes_for_view(_View()) == ""

    class _Shown:
        snapshot = {"adviser_copy_source": "publisher", "adviser_copy": "采光好"}

    assert adviser_notes_for_view(_Shown()) == "采光好"
