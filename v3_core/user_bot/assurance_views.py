"""Public rental-service views for the V3 User Bot."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

AssuranceKind = Literal["home", "signing", "handover", "deposit", "moving"]


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
    "🏠 <b>安心租房</b>\n\n"
    "从看房到入住，重要信息尽量提前确认并留档。\n\n"
    "入住交接、费用确认和住房问题，都可以继续找侨联。"
)

SIGNING_TEXT = ASSURANCE_HOME_TEXT

HANDOVER_TEXT = (
    "📋 <b>入住交接留档</b>\n\n"
    "入住当天把房屋状态记录清楚，退租时更容易核对。\n\n"
    "建议记录：\n\n"
    "• 水电表\n"
    "• 家具家电\n"
    "• 钥匙和门卡\n"
    "• 已有损坏\n"
    "• 房屋现场照片\n"
    "• 费用和押金"
)

DEPOSIT_TEXT = HANDOVER_TEXT

MOVING_TEXT = (
    "🚚 <b>搬家协助</b>\n\n"
    "请说明搬家日期、出发地、目的地，以及大概物品情况。"
)




def build_assurance_home_view() -> AssuranceView:
    return AssuranceView(
        kind="home",
        text=ASSURANCE_HOME_TEXT,
        rows=(
            (AssuranceChoice("📋 入住交接留档", callback_data="v3u:assure:handover"),),
            (AssuranceChoice("💬 中文顾问", callback_data="v3u:home:contact"),),
            (AssuranceChoice("⬅️ 返回侨联服务", callback_data="v3u:home:service"),),
        ),
    )

def build_signing_view() -> AssuranceView:
    return build_assurance_home_view()



def build_handover_view() -> AssuranceView:
    return AssuranceView(
        kind="handover",
        text=HANDOVER_TEXT,
        rows=(
            (AssuranceChoice("查看入住交接清单", callback_data="v3u:assure:handover_download"),),
            (AssuranceChoice("⬅️ 返回安心租房", callback_data="v3u:home:rental"),),
        ),
    )


def build_deposit_view(
    *,
    back_label: str = "⬅️ 返回安心租房",
    back_callback: str = "v3u:home:rental",
) -> AssuranceView:
    return AssuranceView(
        kind="deposit",
        text=HANDOVER_TEXT,
        rows=(
            (AssuranceChoice("查看入住交接清单", callback_data="v3u:assure:deposit_download"),),
            (AssuranceChoice(back_label, callback_data=back_callback),),
        ),
    )


def build_moving_view() -> AssuranceView:
    return AssuranceView(
        kind="moving",
        text=MOVING_TEXT,
        rows=(
            (AssuranceChoice("💬 中文顾问", callback_data="v3u:home:contact"),),
            (AssuranceChoice("⬅️ 返回入住服务", callback_data="v3u:service:concierge"),),
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
    return AssuranceAssetBundle(
        kind=clean,  # type: ignore[arg-type]
        image_path=generated / ("handover.png" if clean == "handover" else "deposit.png"),
        pdf_path=generated / ("handover.pdf" if clean == "handover" else "deposit.pdf"),
        title="入住交接留档",
        instruction=HANDOVER_TEXT,
        filename="入住交接清单.pdf",
    )


__all__ = [
    "ASSURANCE_HOME_TEXT", "SIGNING_TEXT", "DEPOSIT_TEXT", "HANDOVER_TEXT", "MOVING_TEXT",
    "AssuranceAssetBundle", "AssuranceChoice", "AssuranceView", "assurance_asset_bundle",
    "build_assurance_home_view", "build_signing_view", "build_handover_view",
    "build_deposit_view", "build_moving_view",
]