"""Locked-copy V3 views for the public Qiaolian assurance surface."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


AssuranceKind = Literal["home", "moving"]


@dataclass(frozen=True)
class AssuranceChoice:
    label: str
    callback_data: str = ""
    url: str = ""


@dataclass(frozen=True)
class AssuranceView:
    kind: AssuranceKind
    text: str
    rows: tuple[tuple[AssuranceChoice, ...], ...]


ASSURANCE_HOME_TEXT = (
    "🛡 <b>侨联保障</b>\n\n"
    "签约前把费用说清楚。\n"
    "入住时把房屋、表计、物品留档。\n"
    "退租时按留档一项一项对。\n\n"
    "有对不上的，侨联协助与房东、物业沟通处理。"
)

MOVING_TEXT = (
    "🚚 <b>搬家协助</b>\n\n"
    "通过侨联成交的客户，可获得基础搬家协助：\n\n"
    "• 协调搬家时间\n"
    "• 对接车辆和人手\n"
    "• 提供必要的现场支持\n\n"
    "需要时联系中文顾问，说清时间和地点就可以。"
)

HANDOVER_TEXT = (
    "📋 <b>入住怎么交</b>\n\n"
    "入住当天按清单对房屋、表计、物品。\n"
    "双方确认后各留一份，退租时按这份对。"
)

DEPOSIT_TEXT = (
    "🔐 <b>押金怎么退</b>\n\n"
    "签约前先核对金额、退还条件和会扣什么。\n"
    "退租时按合同和入住留档对。\n"
    "最终退多少，以合同和当场核对为准。"
)


def build_assurance_home_view() -> AssuranceView:
    return AssuranceView(
        kind="home",
        text=ASSURANCE_HOME_TEXT,
        rows=(
            (
                AssuranceChoice("📋 入住怎么交", callback_data="v3u:assure:handover"),
                AssuranceChoice("🔐 押金怎么退", callback_data="v3u:assure:deposit"),
            ),
            (
                AssuranceChoice("💬 联系中文顾问", callback_data="v3u:home:contact"),
                AssuranceChoice("🏠 返回首页", callback_data="v3u:t:home"),
            ),
        ),
    )


def build_moving_view() -> AssuranceView:
    return AssuranceView(
        kind="moving",
        text=MOVING_TEXT,
        rows=(
            (AssuranceChoice("💬 联系中文顾问", callback_data="v3u:home:contact"),),
            (AssuranceChoice("⬅️ 返回入住服务", callback_data="v3u:home:service"),),
        ),
    )


@dataclass(frozen=True)
class AssuranceAssetBundle:
    kind: Literal["handover", "deposit"]
    image_path: Path
    pdf_path: Path
    title: str
    instruction: str
    filename: str


def assurance_asset_bundle(repo_root: str | Path, kind: str) -> AssuranceAssetBundle:
    clean = str(kind or "").strip().lower()
    if clean not in {"handover", "deposit"}:
        raise ValueError("unsupported_assurance_asset")
    root = Path(repo_root).expanduser().resolve()
    generated = root / "assets" / "v2_2" / "generated"
    if clean == "handover":
        return AssuranceAssetBundle(
            kind="handover",
            image_path=generated / "handover.png",
            pdf_path=generated / "handover.pdf",
            title="入住怎么交",
            instruction=HANDOVER_TEXT,
            filename="入住交接清单.pdf",
        )
    return AssuranceAssetBundle(
        kind="deposit",
        image_path=generated / "deposit.png",
        pdf_path=generated / "deposit.pdf",
        title="押金怎么退",
        instruction=DEPOSIT_TEXT,
        filename="押金与退租说明.pdf",
    )


__all__ = [
    "ASSURANCE_HOME_TEXT",
    "DEPOSIT_TEXT",
    "HANDOVER_TEXT",
    "MOVING_TEXT",
    "AssuranceAssetBundle",
    "AssuranceChoice",
    "AssuranceView",
    "assurance_asset_bundle",
    "build_assurance_home_view",
    "build_moving_view",
]
