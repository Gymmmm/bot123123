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
    "🏠 <b>侨联地产｜金边中文租房</b>\n"
    "⭐ 金边本地6年经验\n"
    "📍 富力城｜炳发城｜BKK1｜钻石岛\n"
    "💬 专业中文顾问\n"
    "📸 真实房源｜实拍更新\n"
    "📹 实地看房 / 视频代看\n"
    "找房 · 看房 · 签约 · 入住 · 售后\n"
    "签约不是服务的结束。"
)

SIGNING_TEXT = ASSURANCE_HOME_TEXT

HANDOVER_TEXT = (
    "<b>入住交接留档</b>\n\n"
    "入住当天把房子当时的状态记下来，退租时才有依据可以对。\n\n"
    "覆盖：房源和费用、水电表、房屋和家具家电、钥匙门卡、已有损坏，以及退租时的核对说明。"
)

DEPOSIT_TEXT = HANDOVER_TEXT

MOVING_TEXT = (
    "<b>入住服务</b>\n\n"
    "搬家协助请联系中文顾问说明搬出地点、搬入地点和大概时间。"
)


def build_assurance_home_view() -> AssuranceView:
    return AssuranceView(
        kind="home",
        text=ASSURANCE_HOME_TEXT,
        rows=(
            (AssuranceChoice("🔍 开始找房", callback_data="v3u:home:search"),),
            (AssuranceChoice("💬 中文顾问", callback_data="v3u:home:contact"),),
            (AssuranceChoice("⬅️ 返回首页", callback_data="v3u:t:home"),),
        ),
    )

def build_signing_view() -> AssuranceView:
    return build_assurance_home_view()


def build_handover_view() -> AssuranceView:
    return AssuranceView(
        kind="handover",
        text=HANDOVER_TEXT,
        rows=(
            (AssuranceChoice("查看入住交接留档", callback_data="v3u:assure:handover_download"),),
            (AssuranceChoice("开始找房", callback_data="v3u:home:search"),),
            (AssuranceChoice("返回租后服务", callback_data="v3u:home:rental"),),
        ),
    )


def build_deposit_view(
    *,
    back_label: str = "返回租后服务",
    back_callback: str = "v3u:home:rental",
) -> AssuranceView:
    return AssuranceView(
        kind="deposit",
        text=HANDOVER_TEXT,
        rows=(
            (AssuranceChoice("查看入住交接留档", callback_data="v3u:assure:deposit_download"),),
            (AssuranceChoice(back_label, callback_data=back_callback),),
        ),
    )


def build_moving_view() -> AssuranceView:
    return AssuranceView(
        kind="moving",
        text=MOVING_TEXT,
        rows=(
            (AssuranceChoice("中文顾问", callback_data="v3u:home:contact"),),
            (AssuranceChoice("返回首页", callback_data="v3u:t:home"),),
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
