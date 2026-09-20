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
    "<b>租到房，不代表服务就结束了。</b>\n\n"
    "找房只是开始。住进去以后的事情，侨联也接着管。\n\n"
    "签约前　核对租金、押金和相关费用\n"
    "入住时　房屋、表计、家具家电拍照留档\n"
    "入住后　报修或物业沟通，可以找侨联\n"
    "退租时　按入住记录协助逐项核对\n\n"
    "从找房到住进去以后，有需要都可以找侨联。"
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
            (AssuranceChoice("入住交接留档", callback_data="v3u:assure:handover"),),
            (AssuranceChoice("开始找房", callback_data="v3u:home:search"), AssuranceChoice("中文顾问", callback_data="v3u:home:contact")),
            (AssuranceChoice("返回首页", callback_data="v3u:t:home"),),
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
