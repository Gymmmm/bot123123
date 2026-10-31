"""Optional, session-only focus choices for one-to-one live video viewings."""
from __future__ import annotations

from .transition_views import TransitionChoice, TransitionView

FOCUS_SESSION_KEY = "v3_video_focus"
FOCUS_ITEMS = (
    ("furniture", "🛋 家具家电"),
    ("sunlight", "☀️ 采光朝向"),
    ("noise", "🔊 噪音隔音"),
    ("water", "💧 水压热水"),
    ("common", "🚪 楼道公区"),
    ("space", "📐 房间尺寸"),
    ("network", "📶 网络信号"),
    ("view", "🪟 窗外景观"),
    ("other", "✏️ 其他指定要求"),
)
FOCUS_LABELS = dict(FOCUS_ITEMS)


def valid_focus(values: object) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)):
        return ()
    return tuple(code for code, _ in FOCUS_ITEMS if code in values)


def focus_summary(values: object) -> str:
    return "、".join(FOCUS_LABELS[k].split(" ", 1)[-1] for k in valid_focus(values))


def video_focus_view(selected: object = ()) -> TransitionView:
    chosen = valid_focus(selected)
    options = [TransitionChoice(("✅ " if code in chosen else "") + label, "appointment_focus_toggle", code) for code, label in FOCUS_ITEMS]
    rows = tuple(tuple(options[i:i+2]) for i in range(0, len(options), 2))
    return TransitionView(
        kind="appointment_video_focus",
        text=(
            "🎥 <b>视频带看｜重点查看</b>\n\n"
            "顾问到房屋现场，与你进行1对1实时视频连线。\n"
            "请选择你想重点了解的内容（可多选）。\n"
            "其他具体要求可在预约后补充给中文顾问。\n"
            "现场条件不允许核验的项目，顾问会向你说明。\n\n"
            f"<b>已选：</b>{focus_summary(chosen) or '按常规展示'}"
        ),
        rows=rows + (
            (TransitionChoice("✅ 继续预约时间", "appointment_focus_continue"),),
            (TransitionChoice("➡️ 跳过，按常规展示", "appointment_focus_skip"),),
            (TransitionChoice("⬅️ 返回看房方式", "appointment_focus_back"),),
        ),
    )
