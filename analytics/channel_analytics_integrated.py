#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""侨联地产运营分析（集成版）

直接读取业务数据库，输出发布/转化/续租/到期合同等关键指标。
设计目标：
- 兼容新旧字段（例如 lease_end_date 与 contract_end_date）
- 允许部分表缺失时降级返回，不让命令崩溃
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


class QiaolianAnalytics:
    """侨联地产数据分析。"""

    def __init__(self, db_path: str = "data/qiaolian_dual_bot.db") -> None:
        self.db_path = str(db_path)

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None

    def _view_exists(self, conn: sqlite3.Connection, view_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name=? LIMIT 1",
            (view_name,),
        ).fetchone()
        return row is not None

    def _column_exists(self, conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(str(r[1]) == column_name for r in rows)

    def _safe_div_rate(self, numerator: int, denominator: int) -> float:
        if denominator <= 0:
            return 0.0
        return round(float(numerator) / float(denominator) * 100.0, 2)

    def get_publish_overview(self, days: int = 7) -> dict[str, Any]:
        conn = self.get_connection()
        try:
            total_posts = 0
            consult_count = 0
            appointment_count = 0

            if self._table_exists(conn, "publish_analytics"):
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS total
                    FROM publish_analytics
                    WHERE published_at >= datetime('now', ?)
                    """,
                    (f"-{int(days)} days",),
                ).fetchone()
                total_posts = int(row["total"] or 0)

            if self._table_exists(conn, "leads"):
                row = conn.execute(
                    """
                    SELECT COUNT(DISTINCT user_id) AS total
                    FROM leads
                    WHERE action IN ('consult', 'consult_click')
                      AND created_at >= datetime('now', ?)
                    """,
                    (f"-{int(days)} days",),
                ).fetchone()
                consult_count = int(row["total"] or 0)

            if self._table_exists(conn, "appointments"):
                row = conn.execute(
                    """
                    SELECT COUNT(DISTINCT user_id) AS total
                    FROM appointments
                    WHERE created_at >= datetime('now', ?)
                    """,
                    (f"-{int(days)} days",),
                ).fetchone()
                appointment_count = int(row["total"] or 0)

            return {
                "total_posts": total_posts,
                "consult_count": consult_count,
                "appointment_count": appointment_count,
                "consult_rate": self._safe_div_rate(consult_count, total_posts),
                "appointment_rate": self._safe_div_rate(appointment_count, total_posts),
            }
        finally:
            conn.close()

    def get_ab_test_results(self, days: int = 7) -> list[dict[str, Any]]:
        conn = self.get_connection()
        try:
            if not self._table_exists(conn, "publish_analytics"):
                return []

            has_leads = self._table_exists(conn, "leads")
            has_appointments = self._table_exists(conn, "appointments")

            leads_join = ""
            consult_expr = "0 AS consult_count"
            params: list[Any] = []
            if has_leads:
                leads_join = """
                LEFT JOIN leads l
                  ON p.listing_id = l.listing_id
                 AND l.action IN ('consult', 'consult_click')
                 AND l.created_at >= datetime('now', ?)
                """
                consult_expr = "COUNT(DISTINCT l.user_id) AS consult_count"
                params.append(f"-{int(days)} days")

            appointments_join = ""
            appointment_expr = "0 AS appointment_count"
            if has_appointments:
                appointments_join = """
                LEFT JOIN appointments a
                  ON p.listing_id = a.listing_id
                 AND a.created_at >= datetime('now', ?)
                """
                appointment_expr = "COUNT(DISTINCT a.user_id) AS appointment_count"
                params.append(f"-{int(days)} days")

            params.append(f"-{int(days)} days")
            rows = conn.execute(
                f"""
                SELECT
                    p.caption_variant AS caption_variant,
                    COUNT(DISTINCT p.listing_id) AS total_posts,
                    {consult_expr},
                    {appointment_expr}
                FROM publish_analytics p
                {leads_join}
                {appointments_join}
                WHERE p.published_at >= datetime('now', ?)
                GROUP BY p.caption_variant
                ORDER BY p.caption_variant
                """,
                params,
            ).fetchall()
            results: list[dict[str, Any]] = []
            for row in rows:
                total_posts = int(row["total_posts"] or 0)
                consult_count = int(row["consult_count"] or 0)
                appointment_count = int(row["appointment_count"] or 0)
                results.append(
                    {
                        "caption_variant": row["caption_variant"] or "a",
                        "total_posts": total_posts,
                        "consult_count": consult_count,
                        "appointment_count": appointment_count,
                        "consult_rate": self._safe_div_rate(consult_count, total_posts),
                        "appointment_rate": self._safe_div_rate(appointment_count, total_posts),
                    }
                )
            return results
        finally:
            conn.close()

    def get_best_posting_time(self, days: int = 30) -> dict[str, list[dict[str, Any]]]:
        conn = self.get_connection()
        try:
            if not self._table_exists(conn, "publish_analytics"):
                return {"hourly": [], "weekly": []}

            has_leads = self._table_exists(conn, "leads")
            leads_join = ""
            consult_expr = "0 AS consult_count"
            leads_params: list[Any] = []
            if has_leads:
                leads_join = """
                LEFT JOIN leads l
                  ON p.listing_id = l.listing_id
                 AND l.action IN ('consult', 'consult_click')
                 AND l.created_at >= datetime('now', ?)
                """
                consult_expr = "COUNT(DISTINCT l.user_id) AS consult_count"
                leads_params.append(f"-{int(days)} days")

            hourly_rows = conn.execute(
                f"""
                SELECT
                    p.publish_hour,
                    COUNT(*) AS post_count,
                    {consult_expr}
                FROM publish_analytics p
                {leads_join}
                WHERE p.published_at >= datetime('now', ?)
                GROUP BY p.publish_hour
                ORDER BY consult_count DESC, post_count DESC, p.publish_hour ASC
                """,
                [*leads_params, f"-{int(days)} days"],
            ).fetchall()

            weekly_rows = conn.execute(
                f"""
                SELECT
                    p.publish_day_of_week,
                    COUNT(*) AS post_count,
                    {consult_expr}
                FROM publish_analytics p
                {leads_join}
                WHERE p.published_at >= datetime('now', ?)
                GROUP BY p.publish_day_of_week
                ORDER BY consult_count DESC, post_count DESC, p.publish_day_of_week ASC
                """,
                [*leads_params, f"-{int(days)} days"],
            ).fetchall()

            return {
                "hourly": [dict(r) for r in hourly_rows],
                "weekly": [dict(r) for r in weekly_rows],
            }
        finally:
            conn.close()

    def get_popular_areas(self, days: int = 30) -> list[dict[str, Any]]:
        conn = self.get_connection()
        try:
            if not self._table_exists(conn, "publish_analytics"):
                return []

            has_leads = self._table_exists(conn, "leads")
            has_appointments = self._table_exists(conn, "appointments")

            leads_join = ""
            consult_expr = "0 AS consult_count"
            params: list[Any] = []
            if has_leads:
                leads_join = """
                LEFT JOIN leads l
                  ON p.listing_id = l.listing_id
                 AND l.action IN ('consult', 'consult_click')
                 AND l.created_at >= datetime('now', ?)
                """
                consult_expr = "COUNT(DISTINCT l.user_id) AS consult_count"
                params.append(f"-{int(days)} days")

            appointments_join = ""
            appointment_expr = "0 AS appointment_count"
            if has_appointments:
                appointments_join = """
                LEFT JOIN appointments a
                  ON p.listing_id = a.listing_id
                 AND a.created_at >= datetime('now', ?)
                """
                appointment_expr = "COUNT(DISTINCT a.user_id) AS appointment_count"
                params.append(f"-{int(days)} days")

            params.append(f"-{int(days)} days")
            rows = conn.execute(
                f"""
                SELECT
                    COALESCE(NULLIF(TRIM(p.area), ''), '未标注区域') AS area,
                    COUNT(DISTINCT p.listing_id) AS total_posts,
                    {consult_expr},
                    {appointment_expr}
                FROM publish_analytics p
                {leads_join}
                {appointments_join}
                WHERE p.published_at >= datetime('now', ?)
                GROUP BY COALESCE(NULLIF(TRIM(p.area), ''), '未标注区域')
                ORDER BY consult_count DESC, appointment_count DESC, total_posts DESC
                """,
                params,
            ).fetchall()
            results: list[dict[str, Any]] = []
            for row in rows:
                total_posts = int(row["total_posts"] or 0)
                consult_count = int(row["consult_count"] or 0)
                appointment_count = int(row["appointment_count"] or 0)
                results.append(
                    {
                        "area": row["area"],
                        "total_posts": total_posts,
                        "consult_count": consult_count,
                        "appointment_count": appointment_count,
                        "consult_rate": self._safe_div_rate(consult_count, total_posts),
                        "appointment_rate": self._safe_div_rate(appointment_count, total_posts),
                    }
                )
            return results
        finally:
            conn.close()

    def get_renewal_stats(self, months: int = 3) -> list[dict[str, Any]]:
        conn = self.get_connection()
        try:
            if self._view_exists(conn, "renewal_conversion"):
                rows = conn.execute(
                    """
                    SELECT month, total_reminders, completed, conversion_rate
                    FROM renewal_conversion
                    ORDER BY month DESC
                    LIMIT ?
                    """,
                    (int(months),),
                ).fetchall()
                return [dict(r) for r in rows]

            if not self._table_exists(conn, "renewal_tracking"):
                return []

            rows = conn.execute(
                """
                SELECT
                    strftime('%Y-%m', created_at) AS month,
                    COUNT(*) AS total_reminders,
                    SUM(CASE WHEN renewal_status = 'completed' THEN 1 ELSE 0 END) AS completed,
                    ROUND(
                      CAST(SUM(CASE WHEN renewal_status = 'completed' THEN 1 ELSE 0 END) AS FLOAT)
                      / NULLIF(COUNT(*), 0) * 100,
                      2
                    ) AS conversion_rate
                FROM renewal_tracking
                WHERE created_at >= datetime('now', ?)
                GROUP BY strftime('%Y-%m', created_at)
                ORDER BY month DESC
                LIMIT ?
                """,
                (f"-{int(max(1, months)) * 2} months", int(months)),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_expiring_contracts(self, days: int = 30) -> list[dict[str, Any]]:
        conn = self.get_connection()
        try:
            if not self._table_exists(conn, "tenant_bindings"):
                return []

            has_contract_end = self._column_exists(conn, "tenant_bindings", "contract_end_date")
            has_lease_end = self._column_exists(conn, "tenant_bindings", "lease_end_date")
            if has_contract_end and has_lease_end:
                end_expr = "COALESCE(NULLIF(b.contract_end_date, ''), b.lease_end_date)"
            elif has_contract_end:
                end_expr = "NULLIF(b.contract_end_date, '')"
            elif has_lease_end:
                end_expr = "NULLIF(b.lease_end_date, '')"
            else:
                return []

            rent_expr = "b.monthly_rent" if self._column_exists(conn, "tenant_bindings", "monthly_rent") else "0"
            listing_col = "property_name"
            if self._column_exists(conn, "tenant_bindings", "listing_id"):
                listing_col = "listing_id"
            elif self._column_exists(conn, "tenant_bindings", "property_name"):
                listing_col = "property_name"
            elif self._column_exists(conn, "tenant_bindings", "binding_code"):
                listing_col = "binding_code"

            users_join = ""
            users_cols = "'' AS first_name, '' AS username"
            if self._table_exists(conn, "users"):
                users_join = "LEFT JOIN users u ON b.user_id = u.user_id"
                users_cols = "COALESCE(u.first_name, '') AS first_name, COALESCE(u.username, '') AS username"

            query = f"""
                SELECT
                    b.id,
                    b.user_id,
                    b.{listing_col} AS listing_id,
                    {rent_expr} AS monthly_rent,
                    {end_expr} AS contract_end_date,
                    {users_cols},
                    CAST(julianday(date({end_expr})) - julianday(date('now')) AS INTEGER) AS days_left
                FROM tenant_bindings b
                {users_join}
                WHERE b.status='active'
                  AND {end_expr} IS NOT NULL
                  AND date({end_expr}) <= date('now', ?)
                ORDER BY date({end_expr}) ASC
            """
            rows = conn.execute(query, (f"+{int(days)} days",)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def generate_report(self, days: int = 7) -> dict[str, Any]:
        days = max(1, int(days))
        report: dict[str, Any] = {
            "period": f"最近{days}天",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "publish_overview": self.get_publish_overview(days),
            "ab_test": self.get_ab_test_results(days),
            "best_posting_time": self.get_best_posting_time(max(days, 7)),
            "popular_areas": self.get_popular_areas(max(days, 7)),
            "renewal_stats": self.get_renewal_stats(3),
            "expiring_soon": self.get_expiring_contracts(30),
        }
        return report

    def format_telegram_report(self, report: dict[str, Any]) -> str:
        overview = report.get("publish_overview", {})
        ab_list = report.get("ab_test", [])
        areas = report.get("popular_areas", [])
        expiring = report.get("expiring_soon", [])
        best = report.get("best_posting_time", {})

        lines: list[str] = []
        lines.append(f"📊 <b>{report.get('period', '最近7天')}运营报表</b>")
        lines.append("")
        lines.append("<b>【发布概况】</b>")
        lines.append(f"总发布: {int(overview.get('total_posts', 0) or 0)}条")
        lines.append(
            f"咨询: {int(overview.get('consult_count', 0) or 0)}次 ({float(overview.get('consult_rate', 0.0) or 0.0):.1f}%)"
        )
        lines.append(
            f"预约: {int(overview.get('appointment_count', 0) or 0)}次 ({float(overview.get('appointment_rate', 0.0) or 0.0):.1f}%)"
        )

        lines.append("")
        lines.append("<b>【A/B测试】</b>")
        if ab_list:
            for row in ab_list[:3]:
                lines.append(
                    f"{str(row.get('caption_variant', 'a')).upper()}版: {float(row.get('consult_rate', 0.0) or 0.0):.1f}% "
                    f"({int(row.get('consult_count', 0) or 0)}/{int(row.get('total_posts', 0) or 0)})"
                )
        else:
            lines.append("暂无数据")

        best_hour = (best.get("hourly") or [None])[0]
        best_day = (best.get("weekly") or [None])[0]
        day_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

        lines.append("")
        lines.append("<b>【最佳发布时间】</b>")
        if best_hour:
            lines.append(f"小时: {best_hour.get('publish_hour', '-')}点 ({best_hour.get('consult_count', 0)}次咨询)")
        else:
            lines.append("小时: 暂无")
        if best_day:
            idx = int(best_day.get("publish_day_of_week", 0) or 0)
            idx = min(max(idx, 0), 6)
            lines.append(f"星期: {day_names[idx]} ({best_day.get('consult_count', 0)}次咨询)")
        else:
            lines.append("星期: 暂无")

        lines.append("")
        lines.append("<b>【热门区域】</b>")
        if areas:
            for area in areas[:3]:
                lines.append(
                    f"{area.get('area', '-')}: {float(area.get('consult_rate', 0.0) or 0.0):.1f}%"
                )
        else:
            lines.append("暂无数据")

        lines.append("")
        lines.append(f"<b>【30天内到期】</b> {len(expiring)} 个合同")

        text = "\n".join(lines)
        if len(text) > 3800:
            return text[:3796] + "..."
        return text

    def print_report(self, days: int = 7, as_json: bool = False) -> None:
        report = self.generate_report(days)
        if as_json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return

        print("\n" + "=" * 60)
        print(f"📊 侨联地产运营报表 - {report['period']}")
        print("=" * 60 + "\n")

        overview = report["publish_overview"]
        print("【发布概况】")
        print(f"总发布数: {overview['total_posts']}条")
        print(f"咨询转化: {overview['consult_count']}次 ({overview['consult_rate']:.1f}%)")
        print(f"预约转化: {overview['appointment_count']}次 ({overview['appointment_rate']:.1f}%)")

        print("\n【文案A/B测试】")
        if report["ab_test"]:
            for ab in report["ab_test"]:
                print(
                    f"{str(ab['caption_variant']).upper()}版: {float(ab['consult_rate'] or 0):.1f}% "
                    f"({ab['consult_count']}/{ab['total_posts']})"
                )
        else:
            print("暂无数据")

        print("\n【热门区域 Top 5】")
        if report["popular_areas"]:
            for idx, area in enumerate(report["popular_areas"][:5], 1):
                print(
                    f"{idx}. {area['area']}: {float(area['consult_rate'] or 0):.1f}% "
                    f"({area['consult_count']}/{area['total_posts']})"
                )
        else:
            print("暂无数据")

        print("\n【30天内到期合同】")
        if report["expiring_soon"]:
            for contract in report["expiring_soon"][:5]:
                print(
                    f"• user={contract['user_id']} {contract.get('listing_id', '-')}: "
                    f"还剩{contract.get('days_left', '-') }天"
                )
        else:
            print("暂无")

        print("\n" + "=" * 60 + "\n")


def _default_db_path() -> str:
    here = Path(__file__).resolve().parents[1]
    return str(here / "data" / "qiaolian_dual_bot.db")


def main() -> int:
    parser = argparse.ArgumentParser(description="Qiaolian integrated analytics")
    parser.add_argument("days", nargs="?", type=int, default=7, help="report window days")
    parser.add_argument("--db", default=_default_db_path(), help="sqlite db path")
    parser.add_argument("--json", action="store_true", help="print json")
    args = parser.parse_args()

    analytics = QiaolianAnalytics(db_path=args.db)
    analytics.print_report(days=max(1, args.days), as_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
