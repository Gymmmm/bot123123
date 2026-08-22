"""Render property cover HTML as the single visual source of truth."""
from __future__ import annotations

import base64
import json
import mimetypes
import os
from pathlib import Path
import re
from typing import Any


# Keep the browser in the application directory so the systemd service user can
# use the same Chromium installation that was prepared during deployment.
os.environ.setdefault(
    "PLAYWRIGHT_BROWSERS_PATH",
    str(Path(__file__).resolve().parent / ".playwright-browsers"),
)


def _file_to_data_url(path: str) -> str:
    source = Path(path)
    mime = mimetypes.guess_type(source.name)[0] or "image/jpeg"
    encoded = base64.b64encode(source.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _canonical_cover_fields(data: dict[str, Any]) -> dict[str, Any]:
    """Resolve cover tokens with canonical facts taking strict precedence."""
    facts = _as_dict(data.get("canonical_facts")) or _as_dict(data.get("normalized_data"))

    def first(*values: Any) -> Any:
        for value in values:
            if value is not None and str(value).strip():
                return value
        return ""

    project = first(facts.get("project_name"), data.get("project"))
    project_alias = first(facts.get("project_alias"), data.get("project_alias"))
    property_type = first(
        facts.get("property_type_display"), facts.get("property_type"), data.get("property_type")
    )
    if not project:
        # 项目未确认时只展示真实物业类型；不伪造项目名，也不添加
        # “房源”这类弱信息后缀，避免封面标题显得廉价和重复。
        project = property_type or first(data.get("project"), "优质房源")
    deal_type = str(first(facts.get("deal_type"), data.get("deal_type"), "rent")).lower()
    price_value = first(
        facts.get("monthly_rent_usd") if deal_type == "rent" else facts.get("sale_price_usd"),
        facts.get("price"),
        data.get("price"),
    )
    price = str(price_value).strip()
    price = re.sub(r"\s*/\s*月$", "", price)
    if price and not price.startswith("$"):
        price = f"${price}"
    price_suffix = "/月" if deal_type == "rent" else ""
    if price_suffix and data.get("price_suffix") == "" and facts:
        pass
    elif not facts:
        price_suffix = str(data.get("price_suffix") or price_suffix)

    size_value = first(facts.get("size_sqm"), facts.get("size"), data.get("size"))
    size = str(size_value).strip()
    if size and re.fullmatch(r"\d+(?:\.\d+)?", size):
        size += "㎡"
    highlights = facts.get("highlights") if isinstance(facts.get("highlights"), list) else []
    ref = str(first(data.get("qc_code"), data.get("ref"), data.get("listing_id"))).strip()
    listing_match = re.fullmatch(r"(?i)l[_-]?(\d+)", ref)
    if listing_match:
        ref = f"QC{int(listing_match.group(1)):04d}"
    # Internal draft UUIDs are never human-facing. If no canonical listing key
    # was supplied, leave the field blank instead of leaking an implementation ID.
    if ref.upper().startswith("DRF_"):
        ref = ""
    return {
        "ref": ref,
        "project": project,
        "project_alias": project_alias,
        "property_type": property_type,
        "deal_type": deal_type,
        "layout": first(facts.get("layout"), data.get("layout")),
        "area": first(
            facts.get("public_location_display"),
            facts.get("canonical_area_display"),
            data.get("public_location_display"),
            data.get("area"),
        ),
        "size": size,
        "floor": first(facts.get("floor"), facts.get("floor_display"), data.get("floor")),
        "price": price,
        "price_line": f"{price}{price_suffix}" if price else "",
        "price_suffix": price_suffix,
        "h1": first(highlights[0] if len(highlights) > 0 else "", data.get("h1")),
        "h2": first(highlights[1] if len(highlights) > 1 else "", data.get("h2")),
        "h3": first(highlights[2] if len(highlights) > 2 else "", data.get("h3")),
        "is_real_photo": bool(first(facts.get("is_real_photo"), data.get("is_real_photo"))),
    }


def render_html_cover(
    *,
    template_path: str,
    source_image: str,
    output_path: str,
    data: dict[str, Any],
) -> None:
    """Inject listing data into a template and capture the unscaled `.poster`."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # fail closed: never silently fall back to Pillow
        raise RuntimeError("html_cover_renderer_requires_playwright") from exc

    template = Path(template_path).resolve()
    output = Path(output_path).resolve()
    if not template.is_file():
        raise FileNotFoundError(f"cover_template_not_found:{template}")
    if not Path(source_image).is_file():
        raise FileNotFoundError(f"cover_source_not_found:{source_image}")
    output.parent.mkdir(parents=True, exist_ok=True)

    fields = _canonical_cover_fields(data)

    with sync_playwright() as playwright:
        launch_options: dict[str, Any] = {
            "headless": True,
            "args": ["--disable-dev-shm-usage", "--no-sandbox", "--disable-crashpad", "--disable-breakpad", "--disable-features=Crashpad"],
        }
        explicit_browser = str(os.getenv("PLAYWRIGHT_CHROMIUM_EXECUTABLE", "")).strip()
        if explicit_browser and Path(explicit_browser).is_file():
            launch_options["executable_path"] = explicit_browser
        else:
            # Some controlled deployments install the full Chromium bundle
            # without the optional headless-shell archive. Playwright otherwise
            # looks only for headless_shell and fails despite a usable browser.
            browser_root = Path(os.environ["PLAYWRIGHT_BROWSERS_PATH"])
            installed = sorted(browser_root.glob("chromium-*/chrome-linux/chrome"), reverse=True)
            if installed:
                launch_options["executable_path"] = str(installed[0])
        browser = playwright.chromium.launch(**launch_options)
        try:
            page = browser.new_page(
                viewport={"width": 1900, "height": 1500},
                device_scale_factor=1,
            )
            page.goto(template.as_uri(), wait_until="domcontentloaded")
            token_values = {
                "BG_SRC": _file_to_data_url(source_image),
                "REF": fields["ref"],
                "PROJECT": fields["project"],
                "PROJECT_ALIAS": fields["project_alias"],
                "PROPERTY_TYPE": fields["property_type"],
                "DEAL_TYPE": fields["deal_type"],
                "LAYOUT": fields["layout"],
                "AREA": fields["area"],
                "SIZE": fields["size"],
                "FLOOR": fields["floor"],
                "PRICE": fields["price"],
                "PRICE_LINE": fields["price_line"],
                "PRICE_SUFFIX": fields["price_suffix"],
                "H1": fields["h1"],
                "H2": fields["h2"],
                "H3": fields["h3"],
            }
            page.evaluate(
                r"""(values) => {
                    const replace = value => String(value || '').replace(/\{\{([A-Z0-9_]+)\}\}|\$\{([A-Z0-9_]+)\}/g, (match, a, b) => Object.prototype.hasOwnProperty.call(values, a || b) ? (values[a || b] ?? '') : match);
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
                token_values,
            )
            bg_locator = page.locator("#bg, .bg").first
            bg_locator.evaluate("(el, src) => el.src = src", _file_to_data_url(source_image))
            bg_locator.evaluate(
                """async el => {
                    if (!el.complete) {
                        await new Promise(resolve => {
                            el.onload = resolve;
                            el.onerror = resolve;
                        });
                    }
                    if (el.decode) {
                        try { await el.decode(); } catch (_) {}
                    }
                }"""
            )
            for element_id, value in fields.items():
                locator = page.locator(f"#{element_id}")
                if locator.count():
                    locator.fill(str(value or ""))
            button = page.locator("#btn")
            if button.count():
                button.click()
            else:
                update_button = page.locator("#updateBtn")
                if update_button.count():
                    update_button.click()
            # Interactive legacy templates may append "/月" in their button
            # script. Correct the rendered nodes from the canonical deal type
            # after that script runs, so a sale preview can never say rent/month.
            if str(fields["deal_type"]).lower() != "rent":
                for selector in (
                    "#price_text",
                    ".pricebox .price",
                    ".row1 > .price",
                ):
                    locator = page.locator(selector)
                    if locator.count():
                        locator.first.evaluate("(el, value) => el.textContent = value", str(fields["price"] or ""))
                price_label = page.locator(".pricebox .label")
                if price_label.count():
                    price_label.first.evaluate("el => el.textContent = '售价'")
            # 品牌视觉不依赖 emoji；只清理海报节点，不改编辑面板。
            poster_node = page.locator(".poster")
            poster_node.evaluate(
                r"""root => {
                    const empty = value => {
                        const text = String(value ?? '').replace(/\s+/g, ' ').trim();
                        return !text || /^(待确认|未知|未提供|暂无|—|-|N.?A|None|null)$/i.test(text);
                    };
                    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
                    const nodes = [];
                    while (walker.nextNode()) nodes.push(walker.currentNode);
                    for (const node of nodes) {
                        node.textContent = node.textContent
                            .replace(/[🏠🏡📍📐🏢✨🛏💰🛋🏊🏋️🏋]/gu, '')
                            .replace(/\s*[·|｜/]\s*(?=($|\s))/g, '');
                    }
                    // New production templates mark each optional row explicitly.
                    for (const row of root.querySelectorAll('[data-field], [data-fields]')) {
                        const fields = (row.getAttribute('data-fields') || row.getAttribute('data-field') || '')
                            .split(',').map(x => x.trim()).filter(Boolean);
                        const hasValue = fields.some(id => {
                            const el = root.querySelector('#' + CSS.escape(id));
                            return el && !empty(el.textContent);
                        });
                        if (!hasValue) row.style.display = 'none';
                    }
                    // Backward-compatible cleanup for the previous templates.
                    const hideIfEmpty = id => {
                        const el = root.querySelector('#' + id);
                        if (!el) return;
                        const row = el.closest('[data-field], [data-fields], .field-row, .meta-row, .info > div, .tags');
                        if (row && empty(el.textContent)) row.style.display = 'none';
                    };
                    ['t3','t4','t5','t6','t7','t8','t9'].forEach(hideIfEmpty);
                    const infoRows = root.querySelectorAll('.info > div');
                    for (const row of infoRows) {
                        const visible = [...row.querySelectorAll('span, [id]')].some(el => !empty(el.textContent));
                        if (!visible && empty(row.textContent)) row.style.display = 'none';
                    }
                }"""
            )
            video_label = page.locator(".play-label")
            if video_label.count() and not bool(fields["is_real_photo"]):
                video_label.evaluate("el => el.textContent = '视频看房'")
            page.add_style_tag(
                content="""
                * {
                    animation: none !important;
                    transition: none !important;
                }
                html, body, input, button, .poster {
                    font-family: "PingFang SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif !important;
                }
                .poster {
                    transform: none !important;
                    transform-origin: top left !important;
                    box-shadow: none !important;
                    border-radius: 0 !important;
                }
                .poster .info > div,
                .poster .tags,
                .poster [class*="highlight"] {
                    white-space: nowrap !important;
                    overflow: hidden !important;
                    text-overflow: ellipsis !important;
                }
                """
            )
            page.evaluate("document.fonts.ready")
            poster = page.locator(".poster")
            poster.wait_for(state="visible")
            rendered_text = poster.evaluate("el => el.innerText || el.textContent || \"\"")
            unresolved = re.search(r"(?:\{\{[^{}]+\}\}|\$\{[^{}]+\})", str(rendered_text or ""))
            forbidden_placeholder = re.search(r"(?i)(?:待确认|未知|未提供|暂无|\bN/A\b|\bNone\b)", str(rendered_text or ""))
            if unresolved:
                try:
                    output.unlink(missing_ok=True)
                except Exception:
                    pass
                raise RuntimeError(f"cover_unresolved_token:{unresolved.group(0)}")
            if forbidden_placeholder:
                try:
                    output.unlink(missing_ok=True)
                except Exception:
                    pass
                raise RuntimeError(f"cover_forbidden_placeholder:{forbidden_placeholder.group(0)}")
            poster.screenshot(path=str(output), type="png", animations="disabled")
        finally:
            browser.close()
