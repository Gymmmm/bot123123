from http.server import BaseHTTPRequestHandler
from html import escape
import json
from pathlib import Path
from urllib.parse import parse_qs, quote, urlsplit
from urllib.request import urlopen

PUBLIC_BASE = "https://qiaolian-jinbian-sale.vercel.app"
API_BASE = PUBLIC_BASE + "/api/v3/sale/listings"


def _money(value):
    try:
        amount = int(value)
    except (TypeError, ValueError):
        return ""
    return f"$" + f"{amount:,}" if amount > 0 else ""


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        public_id = str((parse_qs(urlsplit(self.path).query).get("id") or [""])[0]).strip()
        if not public_id:
            self.send_error(404)
            return
        try:
            with urlopen(API_BASE + "/" + quote(public_id, safe=""), timeout=8) as response:
                payload = json.loads(response.read().decode("utf-8"))
            item = payload.get("item") or {}
        except Exception:
            self.send_error(404)
            return
        if not item:
            self.send_error(404)
            return

        source = (Path(__file__).resolve().parents[1] / "index.html").read_text(encoding="utf-8")
        project = str(item.get("project") or "").strip()
        location = str(item.get("location") or "").strip()
        layout = str(item.get("layout") or "").strip()
        title_base = project or str(item.get("title") or "").strip() or location or "金边出售房源"
        title = title_base + ((" · " + layout) if layout else "") + "出售｜金边买房｜侨联地产"
        facts = [x for x in (location, project, layout) if x]
        try:
            size = float(item.get("size_sqm") or 0)
        except (TypeError, ValueError):
            size = 0
        if size > 0:
            facts.append(f"{size:g}㎡")
        price = _money(item.get("sale_price_usd"))
        if price:
            facts.append("总价" + price)
        description = "金边出售房源：" + " · ".join(facts) + "。真实在售信息在线更新，中文顾问协助核验房态、产权与具体资料。"
        canonical = PUBLIC_BASE + "/property/" + quote(public_id, safe="")

        source = source.replace("<title>金边买房｜金边房产出售与投资置业｜侨联地产</title>", "<title>" + escape(title) + "</title>")
        source = source.replace('<meta name="description" content="侨联地产金边买房与金边房产出售平台：真实在售公寓、别墅及住宅，提供总价、区域、户型、市场参考与中文置业资料。">', '<meta name="description" content="' + escape(description, quote=True) + '">')
        source = source.replace('<link rel="canonical" href="' + PUBLIC_BASE + '/">', '<link rel="canonical" href="' + escape(canonical, quote=True) + '">')
        source = source.replace('<meta property="og:type" content="website">', '<meta property="og:type" content="article">')
        source = source.replace('<meta property="og:title" content="金边买房｜金边房产出售与投资置业｜侨联地产">', '<meta property="og:title" content="' + escape(title, quote=True) + '">')
        source = source.replace('<meta property="og:description" content="真实在售金边房源、价格与市场参考，附中国买家产权、税费和退出风险资料。">', '<meta property="og:description" content="' + escape(description, quote=True) + '">')
        source = source.replace('<meta property="og:url" content="' + PUBLIC_BASE + '/">', '<meta property="og:url" content="' + escape(canonical, quote=True) + '">')

        structured = {
            "@context": "https://schema.org",
            "@type": "RealEstateListing",
            "name": title_base,
            "description": description,
            "url": canonical,
            "identifier": public_id,
            "dateModified": str(item.get("updated_at") or ""),
            "inLanguage": "zh-CN",
        }
        images = [str(v) for v in (item.get("gallery_urls") or []) if str(v or "").strip()]
        if images:
            structured["image"] = [PUBLIC_BASE + v if v.startswith("/") else v for v in images[:10]]
        if item.get("sale_price_usd"):
            structured["offers"] = {
                "@type": "Offer",
                "priceCurrency": "USD",
                "price": int(item["sale_price_usd"]),
                "url": canonical,
                "availability": "https://schema.org/InStock",
            }
        data = json.dumps(structured, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
        source = source.replace("</head>", '<script type="application/ld+json">' + data + "</script>\n</head>", 1)
        body = source.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "public, s-maxage=300, stale-while-revalidate=3600")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
