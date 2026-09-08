"""DB-free V3 HTML cover renderer.

Uses the same final HTML templates and Playwright rendering model as production,
but public identity and business facts must already be resolved by V3 services.
No draft/listing lookup and no qiaolian_dual import occurs here.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import mimetypes
import os
from pathlib import Path
from typing import Any

from .cover_styles import cover_template_path, normalize_cover_style


REPO_ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault(
    "PLAYWRIGHT_BROWSERS_PATH",
    str(REPO_ROOT / ".playwright-browsers"),
)


@dataclass(frozen=True)
class CoverRenderData:
    public_listing_id: str
    project: str = ""
    project_alias: str = ""
    property_type: str = ""
    deal_type: str = "rent"
    layout: str = ""
    area: str = ""
    size: str = ""
    floor: str = ""
    price: str = ""
    highlight_1: str = ""
    highlight_2: str = ""
    highlight_3: str = ""

    def tokens(self, source_image: str) -> dict[str, str]:
        deal_type = str(self.deal_type or "rent").lower()
        raw_price = str(self.price or "").strip()
        negotiable = raw_price in {"售价面议", "租金面议", "价格面议", "面议"}
        price = raw_price
        if price and not price.startswith("$") and not negotiable:
            price = f"${price}"
        suffix = "/月" if deal_type == "rent" and price and not negotiable else ""
        return {
            "BG_SRC": _file_to_data_url(source_image),
            "REF": str(self.public_listing_id or ""),
            "PROJECT": str(self.project or self.property_type or "优质房源"),
            "PROJECT_ALIAS": str(self.project_alias or ""),
            "PROPERTY_TYPE": str(self.property_type or ""),
            "DEAL_TYPE": deal_type,
            "LAYOUT": str(self.layout or ""),
            "AREA": str(self.area or ""),
            "SIZE": _display_size(self.size),
            "FLOOR": str(self.floor or ""),
            "PRICE": price,
            "PRICE_LINE": f"{price}{suffix}" if price else "",
            "PRICE_SUFFIX": suffix,
            "H1": str(self.highlight_1 or ""),
            "H2": str(self.highlight_2 or ""),
            "H3": str(self.highlight_3 or ""),
        }


def _display_size(value: Any) -> str:
    text = str(value or "").strip().replace("平方米", "㎡").replace("平米", "㎡")
    if text and text.replace(".", "", 1).isdigit():
        text += "㎡"
    return text


def _file_to_data_url(path: str) -> str:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"cover_source_not_found:{source}")
    mime = mimetypes.guess_type(source.name)[0] or "image/jpeg"
    encoded = base64.b64encode(source.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def render_cover(
    *,
    style: str,
    source_image: str,
    output_path: str,
    data: CoverRenderData,
) -> str:
    """Render one final cover from already-resolved V3 facts."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("cover_renderer_requires_playwright") from exc

    template = cover_template_path(normalize_cover_style(style, allow_video=True)).resolve()
    source = Path(source_image).resolve()
    output = Path(output_path).resolve()
    if not template.is_file():
        raise FileNotFoundError(f"cover_template_not_found:{template}")
    if not source.is_file():
        raise FileNotFoundError(f"cover_source_not_found:{source}")
    output.parent.mkdir(parents=True, exist_ok=True)
    tokens = data.tokens(str(source))

    with sync_playwright() as playwright:
        launch_options: dict[str, Any] = {
            "headless": True,
            "args": [
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-crashpad",
                "--disable-breakpad",
                "--disable-features=Crashpad",
            ],
        }
        explicit_browser = str(os.getenv("PLAYWRIGHT_CHROMIUM_EXECUTABLE", "")).strip()
        if explicit_browser and Path(explicit_browser).is_file():
            launch_options["executable_path"] = explicit_browser
        browser = playwright.chromium.launch(**launch_options)
        try:
            page = browser.new_page(
                viewport={"width": 1900, "height": 1500},
                device_scale_factor=1,
            )
            page.goto(template.as_uri(), wait_until="domcontentloaded")
            page.evaluate(
                r"""(values) => {
                    const replace = value => String(value || '').replace(
                        /\{\{([A-Z0-9_]+)\}\}|\$\{([A-Z0-9_]+)\}/g,
                        (match, a, b) => Object.prototype.hasOwnProperty.call(values, a || b)
                            ? (values[a || b] ?? '') : match
                    );
                    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
                    const nodes = [];
                    while (walker.nextNode()) nodes.push(walker.currentNode);
                    for (const node of nodes) node.textContent = replace(node.textContent);
                    for (const el of document.querySelectorAll('*')) {
                        for (const attr of [...el.attributes]) {
                            const next = replace(attr.value);
                            if (next !== attr.value) el.setAttribute(attr.name, next);
                        }
                    }
                }""",
                tokens,
            )

            bg = page.locator("#bg, .bg").first
            if bg.count():
                bg.evaluate("(el, src) => el.src = src", tokens["BG_SRC"])
                bg.evaluate(
                    """async el => {
                        if (!el.complete) await new Promise(resolve => {
                            el.onload = resolve; el.onerror = resolve;
                        });
                        if (el.decode) { try { await el.decode(); } catch (_) {} }
                    }"""
                )

            field_ids = {
                "ref": "REF",
                "project": "PROJECT",
                "project_alias": "PROJECT_ALIAS",
                "property_type": "PROPERTY_TYPE",
                "deal_type": "DEAL_TYPE",
                "layout": "LAYOUT",
                "area": "AREA",
                "size": "SIZE",
                "floor": "FLOOR",
                "price": "PRICE",
                "price_line": "PRICE_LINE",
                "price_suffix": "PRICE_SUFFIX",
                "h1": "H1",
                "h2": "H2",
                "h3": "H3",
            }
            for element_id, token in field_ids.items():
                locator = page.locator(f"#{element_id}")
                if locator.count():
                    locator.first.evaluate(
                        """(el, value) => {
                            const tag = String(el.tagName || '').toLowerCase();
                            if (['input','textarea','select'].includes(tag)) {
                                el.value = value;
                                el.dispatchEvent(new Event('input', {bubbles:true}));
                                el.dispatchEvent(new Event('change', {bubbles:true}));
                            } else {
                                el.textContent = value;
                            }
                        }""",
                        tokens[token],
                    )

            button = page.locator("#btn")
            if button.count():
                button.click()
            else:
                update = page.locator("#updateBtn")
                if update.count():
                    update.click()

            if str(data.deal_type or "rent").lower() != "rent":
                label = page.locator(".pricebox .label")
                if label.count():
                    label.first.evaluate("el => el.textContent = '售价'")

            poster = page.locator(".poster").first
            if not poster.count():
                raise RuntimeError("cover_template_missing_poster")
            page.evaluate(
                """() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))"""
            )
            poster.screenshot(path=str(output), type="jpeg" if output.suffix.lower() in {".jpg", ".jpeg"} else "png")
        finally:
            browser.close()
    return str(output)


__all__ = ["CoverRenderData", "render_cover"]
