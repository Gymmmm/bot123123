from __future__ import annotations
import json
import re
from html import escape
from urllib.parse import quote

# 补齐 keyboards.py 依赖的常量
AREA_OPTIONS = {
    "桑园区": "Sangkat Boeung Keng Kang I",
    "堆谷区": "Tuol Kork",
    "隆边区": "Daun Penh",
    "玛卡拉区": "Prampi Makara",
    "铁桥头区": "Chbar Ampov",
    "水净华区": "Chroy Changvar",
    "森速区": "Sen Sok",
    "棉芷区": "Meanchey",
    "波森芷区": "Porsenchey",
    "金边": "Phnom Penh", # 增加金边作为通用区域
}
TYPE_LABELS = {
    "apartment": "公寓",
    "villa": "别墅",
    "shop": "商铺",
    "office": "办公室",
}

def deep_link(username: str, payload: str) -> str:
    if not username:
        return "#"
    username = username.lstrip("@")
    return f"https://t.me/{username}?start={quote(payload)}"

def _format_list_items(data: str | list) -> list[str]:
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, str) and data.strip():
        try:
            parsed = json.loads(data)
            if isinstance(parsed, list):
                items = parsed
        except Exception:
            items = [i.strip() for i in data.split(",") if i.strip()]
    return [f"• {escape(item)}" for item in items if item.strip()]


def _coerce_text_list(val, *, limit: int, pad: str = "—") -> list[str]:
    """highlights / drawbacks 兼容 list、JSON 字符串、逗号分隔。"""
    items: list[str] = []
    if isinstance(val, list):
        items = [str(x).strip() for x in val if str(x).strip()]
    elif isinstance(val, str) and val.strip():
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                items = [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            items = [i.strip() for i in val.replace("，", ",").split(",") if i.strip()]
    while len(items) < limit:
        items.append(pad)
    return items[:limit]


def _strip_price_for_caption(price_raw: str) -> str:
    s = (price_raw or "").strip().lstrip("$").strip()
    if "/" in s:
        s = s.split("/", 1)[0].strip()
    return s or "面议"


def _format_price_display(price_raw: str) -> str:
    """统一价格展示：有数字时显示 $xxx/月；否则显示 面议。"""
    token = _strip_price_for_caption(price_raw)
    if token == "面议":
        return "面议"
    return f"${token}/月"


def _format_listing_code(data: dict) -> str:
    raw = str(data.get("listing_id") or data.get("draft_id") or "").strip()
    digits = re.sub(r"\D", "", raw)
    if digits:
        return f"QC{digits.zfill(4)}"
    return "QC0000"


def _format_size_for_caption(size_raw: str) -> str:
    s = (size_raw or "").strip()
    if not s:
        return "—"
    if "㎡" in s or "平" in s.lower() or "sqm" in s.lower():
        return s
    return f"{s}㎡"


# 封面图/视频上的一句话（Telegram 相册不能挂按钮，故按钮挂在单张封面上）
CHANNEL_BUTTON_PROMPT = "👇 <b>喜欢这套就直接点下面按钮</b>"


def _generate_tags(area: str, layout: str, highlights: str) -> str:
    tags = ["#金边租房", "#实拍房源"]
    
    # 根据区域添加标签
    if area and area != "未知区域":
        if area == "富力城": # 特殊处理项目名
            tags.append("#富力城")
        elif area == "BKK1":
            tags.append("#BKK1")
        else:
            tags.append(f"#{area}")

    # 根据户型添加标签
    if "公寓" in layout or "房" in layout:
        tags.append("#金边公寓")
    
    # 根据亮点添加标签
    if "拎包入住" in highlights:
        tags.append("#拎包入住")
    if "视频看房" in highlights or "视频" in highlights:
        tags.append("#视频看房")

    # 确保标签数量在 4-6 个之间，并去重
    unique_tags = []
    for tag in tags:
        if tag not in unique_tags:
            unique_tags.append(tag)
    
    # 补充通用标签直到达到4个
    if len(unique_tags) < 4:
        if "#金边公寓" not in unique_tags: unique_tags.append("#金边公寓")
        if "#拎包入住" not in unique_tags and len(unique_tags) < 4: unique_tags.append("#拎包入住")

    return " ".join(unique_tags[:6]) # 最多取前6个

def build_post_text(data: dict, contact_handle: str) -> str:
    """
    频道长文（HTML、无按钮）。与「单图 + caption 挂按钮」的发帖顺序配套。
    contact_handle 保留参数以兼容旧调用。
    """
    _ = contact_handle

    raw_area = (data.get("area") or "").strip()
    raw_project = (
        (data.get("project") or data.get("community") or data.get("title") or "").strip()
    )
    if not raw_project:
        raw_project = raw_area or "优质房源"

    layout_line = (data.get("layout") or "").strip()
    layout_title = layout_line or (data.get("title") or "").strip() or "户型待定"

    project_e = escape(raw_project)
    layout_title_e = escape(layout_title)
    layout_e = escape(layout_line or layout_title)
    size_e = escape(_format_size_for_caption(str(data.get("size") or data.get("size_sqm") or "")))

    price_show = _format_price_display(str(data.get("price") or ""))
    price_e = escape(price_show)
    listing_code_e = escape(_format_listing_code(data))

    floor_raw = (data.get("floor") or "").strip()
    floor_e = escape(floor_raw) if floor_raw else ""

    deposit_e = escape((data.get("deposit") or "押一付一").strip())
    avail_e = escape((data.get("available_date") or "随时入住").strip())

    hl = _coerce_text_list(data.get("highlights"), limit=3, pad="—")
    h1, h2, h3 = (escape(x) for x in hl)

    dw = _coerce_text_list(data.get("drawbacks"), limit=2, pad="暂无补充")
    d1, d2 = (escape(x) for x in dw)

    adv_raw = (data.get("advisor_comment") or "").strip()
    adv_e = escape(adv_raw) if adv_raw else "欢迎私信，顾问按实拍帮您把关。"

    cost_notes = (data.get("cost_notes") or "").strip()
    cost_line = f"\n📌 费用说明：{escape(cost_notes)}" if cost_notes else ""

    # 话题标签：与设计稿一致的四段式；hashtag 内不做 HTML 转义（中文楼盘名等）
    pt = raw_project.replace(" ", "").replace("#", "")[:40] or "金边"
    if raw_area and raw_area not in ("金边", "未知区域"):
        at = raw_area.replace(" ", "").replace("#", "")[:40]
    else:
        at = "金边公寓"
    tags_line = f"#金边租房 #{pt} #实拍房源 #{at}"

    lines: list[str] = [
        f"🏠<b>【侨联实拍】{project_e}｜{layout_title_e}</b>",
        f"🆔编号：<code>{listing_code_e}</code>",
        "",
        f"📍位置：{project_e}",
        f"💰租金：<b>{price_e}</b>",
        f"🛏户型：{layout_e}｜约{size_e}",
    ]
    if floor_e:
        lines.append(f"🏢楼层：{floor_e}")
    lines.extend(
        [
            f"🧾付款：{deposit_e}",
            f"📅入住：{avail_e}",
            "🎥看房：实地可约 / 实时视频代看；实拍视频带侨联水印，细节以相册与顾问说明为准",
            "",
            "✅ 优点优势",
            f"• {h1}",
            f"• {h2}",
            f"• {h3}",
            "",
            "⚠️ 提前说清楚",
            f"• {d1}",
            f"• {d2}",
            "",
            "💬 侨联说",
            adv_e,
            "",
            "💎 侨联地产｜在金边，把找房办明白",
            "",
            tags_line,
        ]
    )
    body = "\n".join(lines)
    if cost_line:
        body += cost_line
    return body

def build_preview_text(data: dict, contact_handle: str = "") -> str:
    text = build_post_text(data, contact_handle)
    return "📋 发布预览（频道先发长文，再发封面图+按钮）\n\n" + text


def build_post_variants(data: dict) -> list[tuple[str, str]]:
    """
    生成同一房源的多文案版本（仅正文）。
    返回: [(variant_name, html_text), ...]
    """
    base = build_post_text(data, contact_handle="")
    area = escape((data.get("area") or "金边").strip())
    title = escape((data.get("title") or data.get("project") or "精选房源").strip())
    layout = escape((data.get("layout") or "户型待定").strip())
    size = escape(_format_size_for_caption(str(data.get("size") or data.get("size_sqm") or "")))
    price = escape(_format_price_display(str(data.get("price") or "")))
    h = _coerce_text_list(data.get("highlights"), limit=3, pad="实拍房源")
    h1, h2, _h3 = (escape(x) for x in h)

    variants: list[tuple[str, str]] = [
        ("标准长文", base),
        (
            "精简卡片",
            "\n".join(
                [
                    f"🏠 <b>{area} · {layout}</b>",
                    f"💰 <b>{price}</b> · 约{size}",
                    f"✨ {h1} · {h2}",
                    "👇 点下方按钮咨询/预约看房",
                ]
            ),
        ),
        (
            "亮点优先",
            "\n".join(
                [
                    f"✨ <b>{title}</b>",
                    f"📍 {area}｜{layout}",
                    f"• {h1}",
                    f"• {h2}",
                    f"💰 <b>{price}</b>",
                    "👇 点下方按钮直接咨询",
                ]
            ),
        ),
        (
            "通勤导向",
            "\n".join(
                [
                    f"🚇 <b>{area} 通勤友好房源</b>",
                    f"🏠 {title}｜{layout}",
                    f"📐 约{size}",
                    f"💰 <b>{price}</b>",
                    "适合上班族/情侣，支持预约实地看房",
                    "👇 点下方按钮领取实拍细节",
                ]
            ),
        ),
        (
            "种草短文",
            "\n".join(
                [
                    f"🌟 <b>{area} · 今日上新</b>",
                    f"{layout}｜约{size}｜<b>{price}</b>",
                    f"主打：{h1}、{h2}",
                    "想看同户型对比，点下方按钮咨询顾问",
                ]
            ),
        ),
    ]
    return variants

def normalize_tags(tags_str: str) -> str:
    """标准化标签字符串，确保每个标签都以 # 开头并以空格分隔。"""
    if not tags_str:
        return ""
    # 处理中文逗号和英文逗号
    raw_tags = tags_str.replace('，', ',').split(',')
    normalized = []
    for t in raw_tags:
        t = t.strip()
        if not t:
            continue
        if not t.startswith('#'):
            t = f'#{t}'
        normalized.append(t)
    return ' '.join(normalized)
