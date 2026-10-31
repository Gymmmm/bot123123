"""Home page with the first page of listings pre-rendered into the HTML.

The static app.js still takes over in the browser (filters, paging, detail),
but crawlers and slow phones now get real listing cards plus ItemList data
without waiting for a client-side fetch.
"""
import json
import os
from html import escape
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import quote, urlsplit
from urllib.request import urlopen

PUBLIC_BASE = "https://qiaolian-jinbian-sale.vercel.app"
API = os.environ.get("QL_SALE_LISTINGS_API", PUBLIC_BASE + "/api/v3/sale/listings")
ADVISOR = "https://t.me/pengqingw"
LIMIT = 24
TEMPLATE = Path(__file__).resolve().parents[1] / "home.html"
BAD = {"", "null", "undefined", "未知", "none", "[]"}


def _t(v):
    return "" if v is None else str(v).strip()


def _has(v):
    return _t(v).lower() not in BAD


def _num(v):
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _money(v):
    n = _num(v)
    return "$" + f"{int(n):,}" if n else ""


def _e(v):
    return escape(_t(v), quote=True)


def _dedupe(parts):
    out = []
    for p in parts:
        if p and p not in out:
            out.append(p)
    return out


def _card(item, index):
    pid = _t(item.get("public_id"))
    title = _t(item.get("title")) or _t(item.get("project")) or "金边出售房源"
    cover = _t(item.get("cover_url"))
    price = _money(item.get("sale_price_usd"))
    location = _t(item.get("location"))
    bedrooms = _num(item.get("bedrooms"))
    bathrooms = _num(item.get("bathrooms"))
    size = _num(item.get("size_sqm"))
    summary = " · ".join(x for x in (location, f"{int(bedrooms)}室" if bedrooms else "",
                                    f"{int(bathrooms)}卫" if bathrooms else "",
                                    _t(item.get("layout")), f"{size:g}㎡" if size else "") if x)
    status = _t(item.get("status"))
    url = "/property/" + quote(pid, safe="")
    tg = ADVISOR + "?text=" + quote("你好，想咨询金边出售房源 " + pid, safe="")
    eager = index < 2
    img = (f'<img src="{_e(cover)}" alt="{_e(title)}" width="800" height="560" '
           + ('fetchpriority="high"' if eager else 'loading="lazy"')
           + ' decoding="async" onerror="retryImage(this)"><div class="image-fallback" hidden>暂无房源照片</div>'
           if cover else '<div class="image-fallback">暂无房源照片</div>')
    return ('<article class="card">'
            f'<a class="card-open property-link" href="{_e(url)}" data-id="{_e(pid)}" aria-label="查看{_e(title)}">'
            f'<div class="card-image">{img}<div class="card-overlay"></div><div class="card-meta">'
            + (f'<span class="tag">{_e(status)}</span>' if status else '')
            + ('<span class="tag">有照片</span>' if item.get('gallery_urls') else '')
            + '</div></div><div class="card-body"><p class="card-price">'
            + (f'<strong class="price">{_e(price)}</strong>' if price else '<span class="price-unset">价格面议</span>')
            + f'</p><h2 class="card-title">{_e(title)}</h2>'
            + (f'<p class="card-summary">{_e(summary)}</p>' if summary else '')
            + '</div></a><div class="card-actions">'
            + f'<a class="detail-action property-link" href="{_e(url)}" data-id="{_e(pid)}">查看详情</a>'
            + f'<a href="{_e(tg)}" target="_blank" rel="noopener">咨询这套</a></div></article>')


def _fetch_items():
    with urlopen(f"{API}?limit={LIMIT}&offset=0&sort=newest", timeout=6) as r:
        payload = json.loads(r.read().decode("utf-8"))
    raw = payload.get("items") or []
    items = [i for i in raw if not _t(i.get("public_id")).upper().startswith("QL-VERIFY-")]
    try:
        total = int(payload.get("total"))
    except (TypeError, ValueError):
        total = len(raw)
    return items, max(0, total - (len(raw) - len(items)))


def render(source, items, total):
    cards = "".join(_card(i, n) for n, i in enumerate(items))
    source = source.replace('<section id="grid" class="listing-grid" aria-live="polite"></section>',
                            '<section id="grid" class="listing-grid" aria-live="polite" data-prerendered="1">' + cards + '</section>', 1)
    source = source.replace('<span id="count" class="count">读取中</span>',
                            f'<span id="count" class="count">{total} 套在售</span>', 1)
    first = next((i for i in items if _has(i.get("cover_url"))), None)
    if first:
        source = source.replace('</head>', '<link rel="preload" as="image" href="' + _e(first['cover_url']) + '">\n</head>', 1)
    data = {"@context": "https://schema.org", "@type": "ItemList", "name": "金边在售房源", "numberOfItems": total,
            "itemListElement": [{"@type": "ListItem", "position": n+1,
                                "url": PUBLIC_BASE + "/property/" + _t(i.get("public_id")),
                                "name": _t(i.get("title")) or "金边出售房源"} for n, i in enumerate(items)]}
    ld = json.dumps(data, ensure_ascii=False, separators=(',', ':')).replace('</', '<\\/')
    return source.replace('</head>', '<script type="application/ld+json">' + ld + '</script>\n</head>', 1)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        source = TEMPLATE.read_text(encoding="utf-8")
        cache = "public, s-maxage=60, stale-while-revalidate=600"
        # Filtered / detail URLs stay purely client-rendered.
        if not urlsplit(self.path).query:
            try:
                items, total = _fetch_items()
                if items:
                    source = render(source, items, total)
            except Exception:
                cache = "public, s-maxage=10"
        body = source.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", cache)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
