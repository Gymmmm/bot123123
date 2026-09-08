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
    "签约前核对费用；入住时把房屋、表计和物品状态留档；退租时按记录逐项核对。\n\n"
    "发生问题时，侨联协助沟通。"
)

MOVING_TEXT = (
    "🚚 <b>搬家协助</b>\n\n"
    "通过侨联成交的客户，可获得基础搬家协助：\n\n"
    "• 协调搬家时间\n"
    "• 对接车辆和人手\n"
    "• 提供必要的现场支持\n\n"
    "搬家不是租约的终点，而是生活真正开始的节点。我们能做的，是让这一小段过渡，稍微从容一些。"
)


def build_assurance_home_view() -> AssuranceView:
    return AssuranceView(
        kind="home",
        text=ASSURANCE_HOME_TEXT,
        rows=(
            (
                AssuranceChoice("📋 入住交接", callback_data="v3u:assure:handover"),
                AssuranceChoice("🔐 押金与退租", callback_data="v3u:assure:deposit"),
            ),
            (
                AssuranceChoice("🚚 搬家协助", callback_data="v3u:assure:moving"),
                AssuranceChoice("💬 联系我们", callback_data="v3u:home:contact"),
            ),
            (AssuranceChoice("🏠 返回首页", callback_data="v3u:t:home"),),
        ),
    )


def build_moving_view() -> AssuranceView:
    return AssuranceView(
        kind="moving",
        text=MOVING_TEXT,
        rows=(
            (AssuranceChoice("💬 联系我们", callback_data="v3u:home:contact"),),
            (AssuranceChoice("⬅️ 返回侨联保障", callback_data="v3u:home:rental"),),
        ),
    )


@dataclass(frozen=True)
class AssuranceAssetBundle:
    kind: Literal["handover", "deposit"]
    image_path: Path
    pdf_path: Path
    title: str
    instruction: str


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
            title="入住交接",
            instruction="请在入住当天逐项核对并填写，双方确认后各自保存，退租时再按留档记录核对。",
        )
    return AssuranceAssetBundle(
        kind="deposit",
        image_path=generated / "deposit.png",
        pdf_path=generated / "deposit.pdf",
        title="押金说明",
        instruction="请在签约前核对押金金额、退还条件和扣费依据；退租时结合合同与入住留档逐项确认。最终押金退还金额仍以合同和实际核对结果为准。",
    )


__all__ = [
    "ASSURANCE_HOME_TEXT",
    "MOVING_TEXT",
    "AssuranceAssetBundle",
    "AssuranceChoice",
    "AssuranceView",
    "assurance_asset_bundle",
    "build_assurance_home_view",
    "build_moving_view",
]
