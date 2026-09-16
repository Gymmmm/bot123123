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
    "📄 <b>租赁服务指南</b>\n\n"
    "从签约到退租，按同一套留档来核对：\n\n"
    "1. <b>签约前确认</b>｜租金、押金、付款方式、水电、物业及其他费用\n"
    "2. <b>入住交接</b>｜房屋现状、表计、家具家电拍照留档\n"
    "3. <b>退租核对</b>｜按合同和入住留档逐项确认押金与费用\n\n"
    "有对不上的，侨联协助与房东、物业沟通处理。"
)

MOVING_TEXT = (
    "🚚 <b>搬家协助</b>\n\n"
    "通过侨联成交的客户，需要搬家时可以直接联系我们。\n\n"
    "可协助：\n"
    "• 确认搬家时间\n"
    "• 对接车辆和人手\n"
    "• 协调必要的物业进出事项\n\n"
    "发送搬出地点、搬入地点和大概时间即可。"
)

HANDOVER_TEXT = (
    "📋 <b>入住交接清单</b>\n\n"
    "入住当天按清单核对房屋现状、表计、家具家电，并拍照留档。\n"
    "双方确认后保留记录，退租时按同一份留档核对。"
)

DEPOSIT_TEXT = (
    "🔐 <b>押金与退租说明</b>\n\n"
    "签约前确认押金金额、退还条件及可能产生的扣费。\n"
    "退租时按合同、入住留档和实际费用逐项核对。\n"
    "最终结算以合同约定和双方实际核对结果为准。"
)


def build_assurance_home_view() -> AssuranceView:
    return AssuranceView(
        kind="home",
        text=ASSURANCE_HOME_TEXT,
        rows=(
            (
                AssuranceChoice("📋 入住交接清单", callback_data="v3u:assure:handover"),
                AssuranceChoice("🔐 押金与退租说明", callback_data="v3u:assure:deposit"),
            ),
            (
                AssuranceChoice("💬 联系顾问", callback_data="v3u:home:contact"),
                AssuranceChoice("⬅️ 返回入住服务", callback_data="v3u:home:service"),
            ),
        ),
    )


def build_moving_view() -> AssuranceView:
    return AssuranceView(
        kind="moving",
        text=MOVING_TEXT,
        rows=(
            (AssuranceChoice("💬 联系顾问", callback_data="v3u:home:contact"),),
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
            title="入住交接清单",
            instruction=HANDOVER_TEXT,
            filename="入住交接清单.pdf",
        )
    return AssuranceAssetBundle(
        kind="deposit",
        image_path=generated / "deposit.png",
        pdf_path=generated / "deposit.pdf",
        title="押金与退租说明",
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
