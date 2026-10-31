"""Read-only HTTP surface for the V3 sale catalog."""
from __future__ import annotations

from http import HTTPStatus
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlsplit

from .catalog import SaleCatalogRepository


def _int_arg(values: dict[str, list[str]], key: str) -> int | None:
    raw = str((values.get(key) or [""])[0]).strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def make_handler(repository: SaleCatalogRepository, index_path: str | Path):
    index_file = Path(index_path).expanduser().resolve()

    class SaleRequestHandler(BaseHTTPRequestHandler):
        server_version = "QiaolianV3Sale/1.2"
        default_public_base = "https://qiaolian-jinbian-sale.vercel.app"

        def _public_base(self) -> str:
            return str(os.getenv("SALE_PUBLIC_BASE_URL") or self.default_public_base).strip().rstrip("/")

        @staticmethod
        def _money(value: Any) -> str:
            try:
                amount = int(value)
            except (TypeError, ValueError):
                return ""
            return f"${amount:,}" if amount > 0 else ""

        def _seo_html(self, item: dict[str, Any] | None = None) -> bytes:
            source = index_file.read_text(encoding="utf-8")
            if item is None:
                return source.encode("utf-8")
            public_id = str(item.get("public_id") or "").strip()
            project = str(item.get("project") or "").strip()
            location = str(item.get("location") or "").strip()
            layout = str(item.get("layout") or "").strip()
            title_base = project or str(item.get("title") or "").strip() or location or "金边出售房源"
            title = f"{title_base}{(' · ' + layout) if layout else ''}出售｜金边买房｜侨联地产"
            facts = [part for part in (location, project, layout) if part]
            try:
                size = float(item.get("size_sqm") or 0)
            except (TypeError, ValueError):
                size = 0
            if size > 0:
                facts.append(f"{size:g}㎡")
            price = self._money(item.get("sale_price_usd"))
            if price:
                facts.append(f"总价{price}")
            description = "金边出售房源：" + " · ".join(facts) + "。真实在售信息在线更新，中文顾问协助核验房态与具体资料。"
            canonical = f"{self._public_base()}/property/{quote(public_id, safe='')}"
            source = source.replace(
                '<title>金边买房｜金边房产出售｜侨联地产</title>',
                f"<title>{escape(title)}</title>",
            )
            source = source.replace(
                '<meta name="description" content="侨联地产金边买房与金边房产出售平台，展示真实在售公寓、别墅及住宅房源，按区域、总价、户型筛选，中文顾问协助核验。">',
                f'<meta name="description" content="{escape(description, quote=True)}">',
            )
            source = source.replace(
                '<link rel="canonical" href="https://qiaolian-jinbian-sale.vercel.app/">',
                f'<link rel="canonical" href="{escape(canonical, quote=True)}">',
            )
            source = source.replace('<meta property="og:type" content="website">','<meta property="og:type" content="article">')
            source = source.replace(
                '<meta property="og:title" content="金边买房｜金边房产出售｜侨联地产">',
                f'<meta property="og:title" content="{escape(title, quote=True)}">',
            )
            source = source.replace(
                '<meta property="og:description" content="真实在售金边房源，按区域、总价、户型筛选，中文顾问协助核验。">',
                f'<meta property="og:description" content="{escape(description, quote=True)}">',
            )
            source = source.replace(
                '<meta property="og:url" content="https://qiaolian-jinbian-sale.vercel.app/">',
                f'<meta property="og:url" content="{escape(canonical, quote=True)}">',
            )
            structured: dict[str, Any] = {
                "@context": "https://schema.org", "@type": "RealEstateListing",
                "name": title_base, "description": description, "url": canonical,
                "identifier": public_id, "dateModified": str(item.get("updated_at") or ""),
                "inLanguage": "zh-CN",
            }
            images = [str(value) for value in (item.get("gallery_urls") or []) if str(value or "").strip()]
            if images:
                structured["image"] = [self._public_base() + value if value.startswith("/") else value for value in images[:10]]
            if item.get("sale_price_usd"):
                structured["offers"] = {
                    "@type": "Offer", "priceCurrency": "USD",
                    "price": int(item["sale_price_usd"]), "url": canonical,
                    "availability": "https://schema.org/InStock",
                }
            payload = json.dumps(structured, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
            source = source.replace("</head>", f'<script type="application/ld+json">{payload}</script>\n</head>', 1)
            return source.encode("utf-8")

        def log_message(self, _format: str, *_args: Any) -> None:
            return

        def _common_headers(self) -> None:
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "same-origin")
            self.send_header("X-Frame-Options", "SAMEORIGIN")

        def _json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self._common_headers()
            self.end_headers()
            self.wfile.write(body)

        def _index(self, item: dict[str, Any] | None = None) -> None:
            if not index_file.is_file():
                self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "sale_frontend_missing"})
                return
            body = self._seo_html(item)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            self._common_headers()
            self.end_headers()
            self.wfile.write(body)

        def _text(self, status: int, body: str, content_type: str) -> None:
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "public, max-age=900")
            self._common_headers()
            self.end_headers()
            self.wfile.write(payload)

        def _robots(self) -> None:
            self._text(
                HTTPStatus.OK,
                "User-agent: *\nAllow: /\nDisallow: /api/\nSitemap: " + self._public_base() + "/sitemap.xml\n",
                "text/plain; charset=utf-8",
            )

        def _sitemap(self) -> None:
            urls = [f"<url><loc>{escape(self._public_base() + '/')}</loc></url>"]
            for row in repository.seo_entries():
                public_id = quote(str(row["public_id"]), safe="")
                lastmod = str(row["updated_at"] or "").strip().replace(" ", "T")
                lastmod_xml = f"<lastmod>{escape(lastmod)}</lastmod>" if lastmod else ""
                urls.append(f"<url><loc>{escape(self._public_base() + '/property/' + public_id)}</loc>{lastmod_xml}</url>")
            xml = '<?xml version="1.0" encoding="UTF-8"?>' + '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + "".join(urls) + "</urlset>"
            self._text(HTTPStatus.OK, xml, "application/xml; charset=utf-8")

        def _media(self, asset_id: str) -> None:
            resolved = repository.media_asset(unquote(asset_id))
            if resolved is None:
                self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "media_not_found"})
                return
            path, mime_type = resolved
            body = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", mime_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "public, max-age=3600")
            self._common_headers()
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
            parsed = urlsplit(self.path)
            path = parsed.path
            if path in {"/", "/index.html", "/sale", "/sale/"}:
                self._index()
                return
            if path == "/robots.txt":
                self._robots()
                return
            if path == "/sitemap.xml":
                self._sitemap()
                return
            property_prefix = "/property/"
            if path.startswith(property_prefix):
                public_id = unquote(path[len(property_prefix):]).strip()
                item = repository.get_sale_listing(public_id)
                if item is None:
                    self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "sale_listing_not_found"})
                else:
                    self._index(item)
                return
            if path == "/healthz":
                self._json(
                    HTTPStatus.OK,
                    {"ok": True, "component": "v3-sale-web", "mode": "read_only"},
                )
                return
            if path == "/api/v3/sale/meta":
                self._json(HTTPStatus.OK, {"ok": True, **repository.catalog_meta()})
                return
            if path == "/api/v3/sale/listings":
                query = parse_qs(parsed.query)
                payload = repository.list_sale_listings(
                    q=str((query.get("q") or [""])[0]),
                    area=str((query.get("area") or [""])[0]),
                    property_type=str((query.get("property_type") or [""])[0]),
                    status=str((query.get("status") or [""])[0]),
                    min_price=_int_arg(query, "min_price"),
                    max_price=_int_arg(query, "max_price"),
                    sort=str((query.get("sort") or ["newest"])[0]),
                    limit=_int_arg(query, "limit") or 24,
                    offset=_int_arg(query, "offset") or 0,
                )
                self._json(HTTPStatus.OK, {"ok": True, **payload})
                return
            prefix = "/api/v3/sale/listings/"
            if path.startswith(prefix):
                public_id = unquote(path[len(prefix):]).strip()
                item = repository.get_sale_listing(public_id)
                if item is None:
                    self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "sale_listing_not_found"})
                else:
                    self._json(HTTPStatus.OK, {"ok": True, "item": item})
                return
            media_prefix = "/api/v3/sale/media/"
            if path.startswith(media_prefix):
                self._media(path[len(media_prefix):])
                return
            self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})

    return SaleRequestHandler


def serve(
    *,
    db_path: str | Path,
    index_path: str | Path,
    host: str = "127.0.0.1",
    port: int = 8091,
) -> None:
    repository = SaleCatalogRepository(db_path)
    handler = make_handler(repository, index_path)
    server = ThreadingHTTPServer((str(host), int(port)), handler)
    server.serve_forever()


__all__ = ["make_handler", "serve"]
