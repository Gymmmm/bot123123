"""V3 views for the public Qiaolian assurance surface."""
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
    "签约前把费用和约定说清楚；入住时按清单完成房屋、表计和物品留档；退租时再按同一份留档核对。\n\n"
    "下面两份资料覆盖入住交接和退租押金。需要沟通时，可直接联系中文顾问。"
)

MOVING_TEXT = (
    "🚚 <b>搬家协助</b>\n\n"
    "通过侨联成交的客户，可获得基础搬家协助：\n\n"
    "• 协调搬家时间\n"
    "• 对接车辆和人手\n"
    "• 提供必要的现场支持\n\n"
    "需要时直接联系中文顾问，并说明时间和地点。"
)

HANDOVER_TEXT = (
    "📋 <b>入住交接与留档</b>\n\n"
    "入住当天按清单核对房屋、表计和物品，并把现场状态留档。\n"
    "双方确认后各留一份，退租时按同一份记录核对。"
)

DEPOSIT_TEXT = (
    "🔐 <b>押金与退租</b>\n\n"
    "签约前先核对押金金额、退还条件和可能扣除的项目。\n"
    "退租时按合同和入住留档逐项核对；最终金额以合同和现场核对结果为准。"
)


def build_assurance_home_view() -> AssuranceView:
    return AssuranceView(
        kind="home",
        text=ASSURANCE_HOME_TEXT,
        rows=(
            (AssuranceChoice("📋 入住交接与留档", callback_data="v3u:assure:handover"),),
            (AssuranceChoice("🔐 押金与退租", callback_data="v3u:assure:deposit"),),
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
            title="入住交接与留档",
            instruction=HANDOVER_TEXT,
            filename="入住交接清单.pdf",
        )
    return AssuranceAssetBundle(
        kind="deposit",
        image_path=generated / "deposit.png",
        pdf_path=generated / "deposit.pdf",
        title="押金与退租",
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
