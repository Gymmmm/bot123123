"""The sale catalog is a real Telegram URL button, not a callback."""
from v3_core.user_bot.service_views import SALE_SITE_URL, service_home_view
from v3_core.user_bot.telegram_service_handler import build_service_keyboard


def test_sale_site_is_direct_service_button():
    assert SALE_SITE_URL == "https://qiaolian-jinbian-sale.vercel.app/"
    kb = build_service_keyboard(service_home_view())
    links = [(b.text, b.url, b.callback_data) for row in kb.inline_keyboard for b in row]
    assert ("🏠 查看出售房源", SALE_SITE_URL, None) in links
    assert sum(text == "🏠 查看出售房源" for text, _, _ in links) == 1
