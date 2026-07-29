from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values


TOKEN_RE = re.compile(r"^\d{6,}:[A-Za-z0-9_-]{20,}$")
USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{5,32}$")


@dataclass(frozen=True)
class PreflightResult:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors


def _value(values: dict[str, object], key: str) -> str:
    return str(values.get(key) or "").strip()


def _check_token(values: dict[str, object], key: str, errors: list[str]) -> None:
    token = _value(values, key)
    if not token:
        errors.append(f"{key} 未配置")
    elif not TOKEN_RE.fullmatch(token):
        errors.append(f"{key} 格式不正确（请填写 BotFather Token）")


def _check_username(values: dict[str, object], key: str, errors: list[str]) -> None:
    username = _value(values, key).lstrip("@")
    if not username:
        errors.append(f"{key} 未配置")
    elif not USERNAME_RE.fullmatch(username):
        errors.append(f"{key} 格式不正确")


def validate_environment(
    env_path: Path,
    *,
    with_user: bool = True,
    with_publisher: bool = False,
    with_collector: bool = False,
) -> PreflightResult:
    errors: list[str] = []
    warnings: list[str] = []
    if not env_path.is_file():
        return PreflightResult(
            errors=("缺少 .env：请先执行 cp .env.example .env 并填写配置",),
            warnings=(),
        )

    values = dict(dotenv_values(env_path))

    if with_user:
        _check_token(values, "USER_BOT_TOKEN", errors)
        _check_username(values, "USER_BOT_USERNAME", errors)

    if with_publisher:
        _check_token(values, "PUBLISHER_BOT_TOKEN", errors)
        admin_ids = [
            item.strip()
            for item in _value(values, "ADMIN_IDS").split(",")
            if item.strip()
        ]
        if not admin_ids or any(not item.isdigit() for item in admin_ids):
            errors.append("ADMIN_IDS 未配置或包含非数字 ID")
        if not _value(values, "CHANNEL_ID"):
            errors.append("CHANNEL_ID 未配置")
        if not (
            _value(values, "USER_BOT_USERNAME")
            or _value(values, "DEEPLINK_BOT_USERNAME")
        ):
            errors.append("USER_BOT_USERNAME 或 DEEPLINK_BOT_USERNAME 至少配置一个")

    if with_collector:
        if not _value(values, "TG_API_ID").isdigit():
            errors.append("TG_API_ID 未配置或不是数字")
        if not _value(values, "TG_API_HASH"):
            errors.append("TG_API_HASH 未配置")
        sources_path = Path(
            _value(values, "COLLECTOR_SOURCES_JSON") or env_path.parent / "sources.json"
        )
        if not sources_path.is_absolute():
            sources_path = env_path.parent / sources_path
        if not sources_path.is_file():
            errors.append(f"采集源配置不存在：{sources_path}")

    if not _value(values, "ADVISOR_TG"):
        warnings.append("ADVISOR_TG 未配置，将使用代码默认顾问账号")
    if with_publisher and not _value(values, "DISCUSSION_GROUP_LINK"):
        warnings.append("DISCUSSION_GROUP_LINK 未配置，评论区按钮会使用降级链接")

    return PreflightResult(tuple(errors), tuple(warnings))
