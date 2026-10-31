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
    title = _t(item.get("title")) or "金边出售房源"
    project = _t(item.get("project"))
    location = _t(item.get("location"))
    kind = _t(item.get("property_subtype")) or _t(item.get("property_type"))
    layout = _t(item.get("layout"))
    if not layout:
        beds, baths = _num(item.get("bedrooms")), _num(item.get("bathrooms"))
        layout = (f"{int(beds)}房" if beds else "") + (f"{int(baths)}卫" if baths else "")
    size = _num(item.get("size_sqm"))
    descriptor = " · ".join(_dedupe([location, kind])) if project else ""
    heading = project or " · ".join(_dedupe([location, kind])) or title
    specs = " · ".join([x for x in (layout, f"{size:g}㎡" if size else "") if x])
    price = _money(item.get("sale_price_usd"))
    status = _t(item.get("status"))
    status_class = {"在售": "on-sale", "已预留": "pending", "待确认": "pending", "已售": "sold", "已下架": "sold"}.get(status, "")
    gallery = [g for g in (item.get("gallery_urls") or []) if _has(g)]
    cover = _t(item.get("cover_url"))
    updated = _t(item.get("updated_at"))[:10].split("-")
    updated_txt = f"{int(updated[1])}月{int(updated[2])}日更新" if len(updated) == 3 and all(x.isdigit() for x in updated) else ""
    ppsm = ""
    kind_all = _t(item.get("property_type")) + _t(item.get("property_subtype"))
    if size and _num(item.get("sale_price_usd")) and ("公寓" in kind_all or "condo" in kind_all.lower()):
        ppsm = "约 $" + f"{round(float(item['sale_price_usd']) / size):,}" + "/㎡"
    url = "/property/" + escape(pid, quote=True)
    msg = "\n".join([x for x in ("你好，想咨询这套金边出售房源", "动作：询问实际价格", "房源：" + title,
                                 ("总价：" + price) if price else "", "系统编号：" + pid, "来源：侨联出售网站",
                                 "链接：" + PUBLIC_BASE + "/property/" + pid) if x])
    tg = ADVISOR + "?text=" + quote(msg, safe="")
    eager = index < 2
    img = (
        f'<img src="{_e(cover)}" alt="{_e(title)}" width="800" height="600" '
        + ('fetchpriority="high" decoding="async"' if eager else 'loading="lazy" decoding="async"')
        + " onerror=\"this.style.display='none';this.nextElementSibling.style.display='grid'\">"
        '<div class="image-fallback" style="display:none">暂无照片</div>'
        if cover else '<div class="image-fallback">暂无照片</div>'
    )
    return (
        '<article class="card">'
        f'<a class="card-link property-link" href="{url}" data-id="{_e(pid)}" aria-label="查看 {_e(title)}">'
        f'<div class="card-image">{img}<div class="card-overlay"></div>'
        + (f'<div class="card-status"><span class="status-tag {status_class}">{_e(status)}</span></div>' if status else "")
        + (f'<div class="card-photo-count">{len(gallery)} 张</div>' if len(gallery) > 1 else "")
        + (f'<div class="card-price-overlay"><span class="card-price-block"><small>总价</small><span class="card-price-text">{_e(price)}</span></span></div>' if price else "")
        + '</div><div class="card-body">'
        f'<h2 class="card-title">{_e(heading)}</h2>'
        + (f'<p class="card-type">{_e(descriptor)}</p>' if descriptor else "")
        + (f'<p class="card-specs"><span>{_e(specs)}</span></p>' if specs else "")
        + (f'<p class="card-specs"><span class="card-ppsm">{_e(ppsm)}</span></p>' if ppsm else "")
        + (f'<p class="card-updated">{_e(updated_txt)}</p>' if updated_txt else "")
        + "</div></a>"
        f'<div class="card-actions"><a class="detail-btn property-link" href="{url}" data-id="{_e(pid)}">看详情</a>'
        f'<a href="{_e(tg)}" target="_blank" rel="noopener">咨询这套</a></div></article>'
    )


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
    source = source.replace(
        '<div id="grid" class="listing-grid" aria-live="polite" aria-label="房源列表"></div>',
        '<div id="grid" class="listing-grid" aria-live="polite" aria-label="房源列表" data-prerendered="1">' + cards + "</div>", 1)
    source = source.replace('<span id="count" class="count-num" aria-live="polite">— 套</span>',
                            f'<span id="count" class="count-num" aria-live="polite">{total:,} 套在售</span>', 1)
    source = source.replace('<span class="stat-num" id="heroCountNum">—</span>',
                            f'<span class="stat-num" id="heroCountNum">{total:,}</span>', 1)
    first = next((i for i in items if _has(i.get("cover_url"))), None)
    if first:
        source = source.replace("</head>", '<link rel="preload" as="image" fetchpriority="high" href="' + _e(first["cover_url"]) + '">\n</head>', 1)
    data = {
        "@context": "https://schema.org", "@type": "ItemList", "name": "金边在售房源",
        "numberOfItems": total,
        "itemListElement": [
            {"@type": "ListItem", "position": n + 1, "url": PUBLIC_BASE + "/property/" + _t(i.get("public_id")),
             "name": _t(i.get("title")) or "金边出售房源"}
            for n, i in enumerate(items)
        ],
    }
    ld = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    return source.replace("</head>", '<script type="application/ld+json">' + ld + "</script>\n</head>", 1)


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
