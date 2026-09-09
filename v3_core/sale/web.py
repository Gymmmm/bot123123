"""Minimal read-only HTTP surface for the future V3 sale catalog."""
from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

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
        server_version = "QiaolianV3Sale/1.0"

        def log_message(self, _format: str, *_args: Any) -> None:
            return

        def _json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _index(self) -> None:
            if not index_file.is_file():
                self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "sale_frontend_missing"})
                return
            body = index_file.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(body)

        def _media(self, asset_id: str) -> None:
            resolved = repository.media_asset(unquote(asset_id))
            if resolved is None:
                self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "media_not_found"})
                return
            path, mime_type = resolved
            body = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", mime_type or "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
            parsed = urlsplit(self.path)
            path = parsed.path
            if path in {"/", "/index.html", "/sale", "/sale/"}:
                self._index()
                return
            if path == "/healthz":
                self._json(HTTPStatus.OK, {"ok": True, "component": "v3-sale-web", "mode": "read_only"})
                return
            if path == "/api/v3/sale/listings":
                query = parse_qs(parsed.query)
                payload = repository.list_sale_listings(
                    area=str((query.get("area") or [""])[0]),
                    property_type=str((query.get("property_type") or [""])[0]),
                    min_price=_int_arg(query, "min_price"),
                    max_price=_int_arg(query, "max_price"),
                    limit=_int_arg(query, "limit") or 60,
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


def serve(*, db_path: str | Path, index_path: str | Path, host: str = "127.0.0.1", port: int = 8091) -> None:
    repository = SaleCatalogRepository(db_path)
    handler = make_handler(repository, index_path)
    server = ThreadingHTTPServer((str(host), int(port)), handler)
    server.serve_forever()


__all__ = ["make_handler", "serve"]
