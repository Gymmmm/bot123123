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
    "📄 <b>租赁服务</b>\n\n"
    "租房过程中，几个重要节点建议提前确认并留好记录。\n\n"
    "可以直接查看下面的内容，不需要先绑定租约。"
)

SIGNING_TEXT = (
    "📝 <b>签约前确认</b>\n\n"
    "签约前建议逐项确认：\n"
    "• 月租、押金、付款方式和租期\n"
    "• 水费、电费、物业费、网络费及其他固定费用\n"
    "• 家具家电、维修责任和提前退租约定\n"
    "• 房屋交付时间和双方需要保留的记录\n\n"
    "不确定的项目，可以在签约前交给中文顾问一起核对。"
)

HANDOVER_TEXT = (
    "📸 <b>入住交接留档</b>\n\n"
    "入住当天建议边检查、边拍照留档：\n"
    "• 房屋整体和已有瑕疵\n"
    "• 家具家电状态\n"
    "• 水表、电表读数\n"
    "• 钥匙、门卡数量\n"
    "• 双方确认的费用和其他事项\n\n"
    "需要保存完整清单时，再点击下方下载。"
)

DEPOSIT_TEXT = (
    "🔐 <b>押金与退租</b>\n\n"
    "退租前先核对合同中的通知期、押金退还条件和扣费约定。\n\n"
    "交接时建议结合入住留档逐项确认：\n"
    "• 房屋及家具家电状态\n"
    "• 水电、物业及其他费用\n"
    "• 钥匙 / 门卡交还\n"
    "• 押金扣费项目及依据\n\n"
    "最终结算以合同约定和实际交接核对结果为准。需要保存完整说明时，再点击下方下载。"
)

MOVING_TEXT = (
    "🚚 <b>搬家协助</b>\n\n"
    "需要搬家协助时，请直接联系中文顾问说明搬出地点、搬入地点和大概时间。"
)


def build_assurance_home_view() -> AssuranceView:
    return AssuranceView(
        kind="home",
        text=ASSURANCE_HOME_TEXT,
        rows=(
            (AssuranceChoice("📝 签约前确认", callback_data="v3u:assure:signing"),),
            (
                AssuranceChoice("📸 入住交接留档", callback_data="v3u:assure:handover"),
                AssuranceChoice("🔐 押金与退租", callback_data="v3u:assure:deposit"),
            ),
            (AssuranceChoice("💬 中文顾问", callback_data="v3u:home:contact"),),
            (AssuranceChoice("🏠 返回首页", callback_data="v3u:t:home"),),
        ),
    )


def build_signing_view() -> AssuranceView:
    return AssuranceView(
        kind="signing",
        text=SIGNING_TEXT,
        rows=(
            (AssuranceChoice("💬 中文顾问", callback_data="v3u:home:contact"),),
            (AssuranceChoice("⬅️ 返回租赁服务", callback_data="v3u:home:rental"),),
        ),
    )


def build_handover_view() -> AssuranceView:
    return AssuranceView(
        kind="handover",
        text=HANDOVER_TEXT,
        rows=(
            (AssuranceChoice("📥 下载入住交接清单", callback_data="v3u:assure:handover_download"),),
            (AssuranceChoice("💬 中文顾问", callback_data="v3u:home:contact"),),
            (AssuranceChoice("⬅️ 返回租赁服务", callback_data="v3u:home:rental"),),
        ),
    )


def build_deposit_view() -> AssuranceView:
    return AssuranceView(
        kind="deposit",
        text=DEPOSIT_TEXT,
        rows=(
            (AssuranceChoice("📥 下载押金与退租说明", callback_data="v3u:assure:deposit_download"),),
            (AssuranceChoice("💬 中文顾问", callback_data="v3u:home:contact"),),
            (AssuranceChoice("⬅️ 返回租赁服务", callback_data="v3u:home:rental"),),
        ),
    )


def build_moving_view() -> AssuranceView:
    return AssuranceView(
        kind="moving",
        text=MOVING_TEXT,
        rows=(
            (AssuranceChoice("💬 中文顾问", callback_data="v3u:home:contact"),),
            (AssuranceChoice("🏠 返回首页", callback_data="v3u:t:home"),),
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
            title="入住交接留档",
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
    "ASSURANCE_HOME_TEXT", "SIGNING_TEXT", "DEPOSIT_TEXT", "HANDOVER_TEXT", "MOVING_TEXT",
    "AssuranceAssetBundle", "AssuranceChoice", "AssuranceView", "assurance_asset_bundle",
    "build_assurance_home_view", "build_signing_view", "build_handover_view",
    "build_deposit_view", "build_moving_view",
]
