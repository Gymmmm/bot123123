"""侨联房源封面的唯一风格注册表。

正式横版封面只保留三套：经典蓝卡、右侧价格牌、黑金高级感。
旧名称只做兼容映射，不再形成新的渲染分支。
"""
from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

FINAL_COVER_STYLES = ("classic_blue", "right_price", "black_gold")

STYLE_LABELS = {
    "classic_blue": "经典蓝卡",
    "right_price": "右侧价格牌",
    "black_gold": "黑金高级感",
}

_TEMPLATE_PATHS = {
    "classic_blue": REPO_ROOT / "templates" / "property" / "01_经典蓝卡模板.html",
    "right_price": REPO_ROOT / "templates" / "property" / "03_右侧价格牌模板.html",
    "black_gold": REPO_ROOT / "templates" / "property" / "04_黑金高级感_右侧价格牌模板.html",
    "video_vertical": REPO_ROOT / "templates" / "property" / "04_竖版视频封面模板.html",
}

_ALIASES = {
    "classic": "classic_blue",
    "left_info": "classic_blue",
    "minimal": "classic_blue",
    "minimal_white": "classic_blue",
    "blue_banner": "classic_blue",
    "premium_4image": "classic_blue",
    "price_tag": "right_price",
    "right_price_fixed": "right_price",
    "villa_premium": "black_gold",
    "dark_glass": "black_gold",
    "vertical": "video_vertical",
}

ACCEPTED_COVER_STYLE_KEYS = frozenset(
    {*FINAL_COVER_STYLES, "video_vertical", *_ALIASES.keys()}
)


def normalize_cover_style(style: str | None, *, allow_video: bool = True) -> str:
    """把旧名称收敛到最终三套；空值固定使用经典蓝卡。"""
    value = str(style or "classic_blue").strip().lower()
    value = _ALIASES.get(value, value)
    if value == "video_vertical" and allow_video:
        return value
    return value if value in FINAL_COVER_STYLES else "classic_blue"


def cover_template_path(style: str | None, *, allow_video: bool = True) -> Path:
    normalized = normalize_cover_style(style, allow_video=allow_video)
    return _TEMPLATE_PATHS[normalized]


__all__ = [
    "ACCEPTED_COVER_STYLE_KEYS",
    "FINAL_COVER_STYLES",
    "STYLE_LABELS",
    "cover_template_path",
    "normalize_cover_style",
]
