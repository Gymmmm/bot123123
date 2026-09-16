#!/usr/bin/env python3
"""安全导入老客户租约档案，并生成 Telegram 绑定链接。

默认仅执行预览校验；必须显式传入 --apply 才会写入数据库。
该工具不会存储手机号、微信或姓名等身份信息，仅写入租约服务所需字段。
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from qiaolian_dual.config import DB_PATH, USER_BOT_USERNAME

REQUIRED_COLUMNS = {"property_name"}
OPTIONAL_COLUMNS = {
    "binding_code",
    "lease_end_date",
    "rent_day",
    "monthly_rent",
    "contract_start_date",
    "contract_end_date",
    "deposit_months",
    "contract_notes",
}
CODE_PATTERN = re.compile(r"^[A-Z0-9-]{4,40}$")
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def clean(value: object) -> str:
    return str(value or "").strip()


def parse_date(value: str, field: str, row_number: int) -> str:
    value = clean(value)
    if not value:
        return ""
    if not DATE_PATTERN.fullmatch(value):
        raise ValueError(f"第 {row_number} 行 {field} 必须为 YYYY-MM-DD")
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"第 {row_number} 行 {field} 不是有效日期") from exc
    return value


def parse_int(value: str, field: str, row_number: int, *, minimum: int, maximum: int) -> int | None:
    value = clean(value)
    if not value:
        return None
    try:
        number = int(value)
    except ValueError as exc:
        raise ValueError(f"第 {row_number} 行 {field} 必须为整数") from exc
    if not minimum <= number <= maximum:
        raise ValueError(f"第 {row_number} 行 {field} 必须在 {minimum}–{maximum} 之间")
    return number


def parse_decimal(value: str, field: str, row_number: int, *, default: str) -> float:
    value = clean(value) or default
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"第 {row_number} 行 {field} 必须为数字") from exc
    if number < 0:
        raise ValueError(f"第 {row_number} 行 {field} 不能为负数")
    return float(number)


def normalize_code(value: str, row_number: int) -> str:
    code = clean(value).upper()
    if not code:
        code = f"OLD-{datetime.now(timezone.utc):%Y%m%d}-{row_number:04d}"
    if not CODE_PATTERN.fullmatch(code):
        raise ValueError(
            f"第 {row_number} 行 binding_code 仅支持大写字母、数字和连字符，长度 4–40"
        )
    return code


def binding_link(username: str, code: str) -> str:
    handle = clean(username).lstrip("@")
    if not handle:
        return ""
    return f"https://t.me/{handle}?start={quote('t_bind_' + code, safe='')}"


def read_rows(input_path: Path, existing_codes: set[str]) -> tuple[list[dict[str, object]], list[str]]:
    records: list[dict[str, object]] = []
    errors: list[str] = []
    seen_codes: set[str] = set()
    with input_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        header = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - header
        if missing:
            return [], [f"CSV 缺少必填列：{', '.join(sorted(missing))}"]
        for row_number, row in enumerate(reader, start=2):
            if not any(clean(value) for value in row.values()):
                continue
            try:
                property_name = clean(row.get("property_name"))
                if not property_name:
                    raise ValueError(f"第 {row_number} 行 property_name 不能为空")
                if len(property_name) > 200:
                    raise ValueError(f"第 {row_number} 行 property_name 不能超过 200 个字符")
                code = normalize_code(clean(row.get("binding_code")), row_number)
                if code in seen_codes:
                    raise ValueError(f"第 {row_number} 行 binding_code 在 CSV 中重复：{code}")
                if code in existing_codes:
                    raise ValueError(f"第 {row_number} 行 binding_code 已存在于数据库：{code}")
                seen_codes.add(code)
                notes = clean(row.get("contract_notes"))
                if len(notes) > 1000:
                    raise ValueError(f"第 {row_number} 行 contract_notes 不能超过 1000 个字符")
                records.append(
                    {
                        "binding_code": code,
                        "property_name": property_name,
                        "lease_end_date": parse_date(clean(row.get("lease_end_date")), "lease_end_date", row_number),
                        "rent_day": parse_int(clean(row.get("rent_day")), "rent_day", row_number, minimum=1, maximum=31),
                        "monthly_rent": parse_decimal(clean(row.get("monthly_rent")), "monthly_rent", row_number, default="0"),
                        "contract_start_date": parse_date(clean(row.get("contract_start_date")), "contract_start_date", row_number),
                        "contract_end_date": parse_date(clean(row.get("contract_end_date")), "contract_end_date", row_number),
                        "deposit_months": parse_decimal(clean(row.get("deposit_months")), "deposit_months", row_number, default="2"),
                        "contract_notes": notes,
                    }
                )
            except ValueError as exc:
                errors.append(str(exc))
    return records, errors


def write_records(connection: sqlite3.Connection, records: list[dict[str, object]]) -> None:
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    connection.executemany(
        """
        INSERT INTO tenant_bindings (
            user_id, binding_code, property_name, lease_end_date, rent_day,
            monthly_rent, contract_start_date, contract_end_date, deposit_months,
            contract_notes, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        [
            (
                0,
                record["binding_code"],
                record["property_name"],
                record["lease_end_date"],
                record["rent_day"],
                record["monthly_rent"],
                record["contract_start_date"],
                record["contract_end_date"],
                record["deposit_months"],
                record["contract_notes"],
                created_at,
            )
            for record in records
        ],
    )


def write_result(path: Path, records: list[dict[str, object]], username: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["binding_code", "property_name", "lease_end_date", "rent_day", "binding_link"],
        )
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "binding_code": record["binding_code"],
                    "property_name": record["property_name"],
                    "lease_end_date": record["lease_end_date"],
                    "rent_day": record["rent_day"] or "",
                    "binding_link": binding_link(username, str(record["binding_code"])),
                }
            )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="批量导入老客户租约档案；默认只校验，不写入数据库。"
    )
    parser.add_argument("--input", required=True, type=Path, help="UTF-8 CSV 文件路径")
    parser.add_argument("--apply", action="store_true", help="确认写入数据库；未提供时仅预览")
    parser.add_argument("--output", type=Path, help="写入后导出的绑定链接 CSV 路径")
    parser.add_argument(
        "--bot-username",
        default=USER_BOT_USERNAME,
        help="生成绑定链接用的机器人用户名，默认读取服务器配置",
    )
    parser.add_argument("--db", type=Path, default=DB_PATH, help="SQLite 数据库路径")
    args = parser.parse_args()

    if not args.input.is_file():
        print(json.dumps({"ok": False, "errors": [f"找不到输入文件：{args.input}"]}, ensure_ascii=False))
        return 2
    if args.apply and args.output is None:
        print(json.dumps({"ok": False, "errors": ["执行 --apply 时必须提供 --output 保存绑定链接"]}, ensure_ascii=False))
        return 2

    with sqlite3.connect(args.db) as connection:
        connection.row_factory = sqlite3.Row
        existing_codes = {
            str(row["binding_code"] or "").upper()
            for row in connection.execute("SELECT binding_code FROM tenant_bindings")
        }
        records, errors = read_rows(args.input, existing_codes)
        summary = {
            "ok": not errors,
            "mode": "apply" if args.apply else "dry_run",
            "valid_records": len(records),
            "errors": errors,
        }
        if errors:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 1
        if not args.apply:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 0
        try:
            write_records(connection, records)
            connection.commit()
        except sqlite3.DatabaseError as exc:
            connection.rollback()
            print(json.dumps({"ok": False, "errors": [f"数据库写入失败：{exc}"]}, ensure_ascii=False))
            return 1

    write_result(args.output, records, str(args.bot_username or ""))
    print(
        json.dumps(
            {
                "ok": True,
                "mode": "apply",
                "imported": len(records),
                "result_file": str(args.output),
                "note": "档案已以 pending 状态写入；请通过安全渠道将专属绑定链接发送给对应老客户。",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
