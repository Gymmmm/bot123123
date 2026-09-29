from http.server import BaseHTTPRequestHandler
from html import escape
import json
from urllib.request import urlopen

PUBLIC_BASE = "https://qiaolian-jinbian-sale.vercel.app"
API = PUBLIC_BASE + "/api/v3/sale/listings"


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        rows = []
        offset = 0
        try:
            while offset < 1000:
                with urlopen(API + f"?limit=60&offset={offset}", timeout=8) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                items = payload.get("items") or []
                rows.extend(items)
                if not payload.get("has_more") or not items:
                    break
                offset += len(items)
        except Exception:
            rows = []

        urls = ["<url><loc>" + escape(PUBLIC_BASE + "/") + "</loc></url>"]
        for item in rows:
            public_id = str(item.get("public_id") or "").strip()
            if not public_id or public_id.upper().startswith("QL-VERIFY-"):
                continue
            updated = str(item.get("updated_at") or "").strip().replace(" ", "T")
            lastmod = ("<lastmod>" + escape(updated) + "</lastmod>") if updated else ""
            urls.append("<url><loc>" + escape(PUBLIC_BASE + "/property/" + public_id) + "</loc>" + lastmod + "</url>")
        xml = '<?xml version="1.0" encoding="UTF-8"?>' + '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + "".join(urls) + "</urlset>"
        body = xml.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/xml; charset=utf-8")
        self.send_header("Cache-Control", "public, s-maxage=900, stale-while-revalidate=3600")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
