"""
AI 解析：source_posts(pending) → drafts(pending)。

当前默认走规则解析，先把「采集 -> 频道运营」主链打通。
后续如接 OpenRouter / OpenAI，可在本文件上继续切换为模型抽取。
"""
from __future__ import annotations

import json
import re
import uuid
from collections import Counter

from db import DatabaseManager
from qiaolian_pipeline.parser import RuleBasedListingParser, non_rental_source_reasons


class LLMClient:
    def parse_text_with_llm(self, raw_text: str) -> dict:
        """
        第一阶段先用规则抽取，解决现网大量空 project / price=0 的占位问题。
        方法名保留不变，减少对现有流水线的侵入。
        """
        return RuleBasedListingParser().parse(raw_text)


class AIParserModule:
    def __init__(self, db_path: str):
        self.db_manager = DatabaseManager(db_path)
        self.llm_client = LLMClient()

    @staticmethod
    def _build_review_note(parsed_data: dict) -> str:
        flags = parsed_data.get("quality_flags") or []
        if not flags:
            return "quality:ok"
        return "quality:" + ",".join(str(flag) for flag in flags)

    @staticmethod
    def _price_from_parsed(parsed_data: dict) -> int:
        raw = parsed_data.get("price")
        if raw in (None, ""):
            return 0
        try:
            return int(float(str(raw).replace("$", "").replace(",", "").strip()))
        except Exception:
            pass
        txt = str(raw)
        m = re.search(r"(\d{2,6})", txt)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return 0
        return 0

    @staticmethod
    def _price_from_raw_text(raw_text: str) -> int:
        text = str(raw_text or "")
        # 优先匹配显式货币/租金表达，避免把面积误识别为价格。
        patterns = [
            r"([1-9]\d?(?:\.\d+)?)\s*k\s*(?:/month|per month|/月|每月|usd|USD|美金|刀)",
            r"(?:\$|usd|USD|美金|刀|租金[:：]?)\s*([1-9]\d{1,5})",
            r"([1-9]\d{1,5})\s*(?:\$|usd|USD|美金|刀|/月|每月)",
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if not m:
                continue
            try:
                value = float(m.group(1))
                if "k" in m.group(0).lower():
                    value *= 1000
                return int(value)
            except Exception:
                continue
        return 0

    def _resolve_price_value(self, parsed_data: dict, raw_text: str) -> int:
        p = self._price_from_parsed(parsed_data)
        if p > 0:
            return p
        return self._price_from_raw_text(raw_text)

    @staticmethod
    def _extract_non_rental_reasons(parsed_data: dict, raw_text: str) -> list[str]:
        reasons: list[str] = []
        flags = [str(flag) for flag in (parsed_data.get("quality_flags") or [])]
        for flag in flags:
            if not flag.startswith("non_rental_"):
                continue
            reason = flag.replace("non_rental_", "", 1)
            if reason in {"source", "commercial_waste"}:
                continue
            reasons.append(reason)
        if not reasons:
            reasons = non_rental_source_reasons(raw_text or "")
        seen: list[str] = []
        for item in reasons:
            if item and item not in seen:
                seen.append(item)
        return seen

    @staticmethod
    def _is_manual_intake_source(source_type: str) -> bool:
        st = str(source_type or "").strip().lower()
        return st in {"csv_intake", "wechat_note", "excel_intake"}

    def _mark_source_post(self, post_id: int, status: str, error: str = "") -> None:
        self.db_manager._execute_query(
            "UPDATE source_posts SET parse_status = ?, parse_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, error[:500], post_id),
        )

    def _apply_parsed_data_to_draft(self, draft_id: str, parsed_data: dict) -> None:
        price_value = self._price_from_parsed(parsed_data)
        parsed_data["price"] = price_value if price_value > 0 else None
        self.db_manager.update_draft(
            draft_id,
            title=parsed_data.get("title"),
            project=parsed_data.get("project"),
            community=parsed_data.get("community"),
            area=parsed_data.get("area"),
            property_type=parsed_data.get("property_type"),
            price=parsed_data.get("price"),
            layout=parsed_data.get("layout"),
            size=parsed_data.get("size"),
            floor=parsed_data.get("floor"),
            deposit=parsed_data.get("deposit"),
            available_date=parsed_data.get("available_date"),
            highlights=parsed_data.get("highlights", []),
            drawbacks=parsed_data.get("drawbacks", []),
            advisor_comment=parsed_data.get("advisor_comment"),
            cost_notes=parsed_data.get("cost_notes"),
            water_rate=parsed_data.get("water_rate"),
            electric_rate=parsed_data.get("electric_rate"),
            extracted_data=json.dumps(parsed_data, ensure_ascii=False),
            normalized_data=json.dumps(parsed_data, ensure_ascii=False),
            queue_score=parsed_data.get("quality_score", 0),
            review_note=self._build_review_note(parsed_data),
        )

    def _process_single_source_post_with_status(self, source_post_db_id: int) -> tuple[str, str | None]:
        source_post = self.db_manager._fetch_one(
            "SELECT id, source_id, source_type, raw_text FROM source_posts WHERE id = ? AND parse_status = ?",
            (source_post_db_id, "pending"),
        )
        if not source_post:
            return "not_pending", None

        post_id, _source_id, source_type, raw_text = source_post
        existing_draft = self.db_manager._fetch_one(
            "SELECT draft_id FROM drafts WHERE source_post_id = ? ORDER BY id DESC LIMIT 1",
            (post_id,),
        )
        if existing_draft:
            self._mark_source_post(post_id, "parsed", "")
            return "already_parsed", existing_draft[0]
        try:
            text = (raw_text or "").strip()
            if not text:
                self._mark_source_post(post_id, "skipped_empty_text", "empty_raw_text")
                return "skipped_empty_text", None
            parsed_data = self.llm_client.parse_text_with_llm(raw_text or "")
            non_rental_reasons = self._extract_non_rental_reasons(parsed_data, raw_text or "")
            if non_rental_reasons:
                self._mark_source_post(
                    post_id,
                    "skipped_non_rental",
                    "non_rental:" + ",".join(non_rental_reasons),
                )
                return "skipped_non_rental", None
            price_value = self._resolve_price_value(parsed_data, raw_text or "")
            if price_value <= 0:
                if self._is_manual_intake_source(source_type):
                    # 手工导入允许无价格入草稿，发布端统一展示“面议”。
                    parsed_data["price"] = None
                    flags = [str(f) for f in (parsed_data.get("quality_flags") or [])]
                    if "missing_price_manual_consult" not in flags:
                        flags.append("missing_price_manual_consult")
                    parsed_data["quality_flags"] = flags
                else:
                    # 自动采集保持硬规则：无价格不入 drafts。
                    self._mark_source_post(post_id, "skipped_no_price", "missing_price")
                    return "skipped_no_price", None
            else:
                parsed_data["price"] = price_value
            extracted_data_json = json.dumps(parsed_data, ensure_ascii=False)
            normalized_data_json = extracted_data_json

            draft_id = f"DRF_{uuid.uuid4()}"
            self.db_manager.create_draft(
                draft_id=draft_id,
                source_post_id=post_id,
                title=parsed_data.get("title"),
                project=parsed_data.get("project"),
                community=parsed_data.get("community"),
                area=parsed_data.get("area"),
                property_type=parsed_data.get("property_type"),
                price=parsed_data.get("price"),
                layout=parsed_data.get("layout"),
                size=parsed_data.get("size"),
                floor=parsed_data.get("floor"),
                deposit=parsed_data.get("deposit"),
                available_date=parsed_data.get("available_date"),
                highlights=parsed_data.get("highlights", []),
                drawbacks=parsed_data.get("drawbacks", []),
                advisor_comment=parsed_data.get("advisor_comment"),
                cost_notes=parsed_data.get("cost_notes"),
                extracted_data=extracted_data_json,
                normalized_data=normalized_data_json,
                review_status="pending",
                water_rate=parsed_data.get("water_rate"),
                electric_rate=parsed_data.get("electric_rate"),
                queue_score=parsed_data.get("quality_score", 0),
                review_note=self._build_review_note(parsed_data),
            )

            self._mark_source_post(post_id, "parsed", "")
            return "parsed", draft_id
        except Exception as e:
            self._mark_source_post(post_id, "failed", str(e))
            return "failed", None

    def process_single_source_post(self, source_post_db_id: int):
        _status, draft_id = self._process_single_source_post_with_status(source_post_db_id)
        return draft_id

    def process_pending_source_posts(self) -> dict[str, int]:
        pending_posts = self.db_manager._fetch_all(
            "SELECT id FROM source_posts WHERE parse_status = ?",
            ("pending",),
        )
        stats: Counter[str] = Counter()
        for (post_id,) in pending_posts or []:
            status, _draft_id = self._process_single_source_post_with_status(post_id)
            stats[status] += 1
        stats["total_pending"] = len(pending_posts or [])
        return dict(stats)

    def refresh_low_quality_drafts(self, limit: int = 50) -> int:
        rows = self.db_manager._fetch_all(
            """
            SELECT d.draft_id, sp.raw_text
            FROM drafts d
            JOIN source_posts sp ON sp.id = d.source_post_id
            WHERE d.review_status = 'pending'
              AND (
                    d.project IS NULL OR d.project = ''
                 OR d.price IS NULL OR d.price = 0
                 OR d.queue_score IS NULL OR d.queue_score = 0
              )
            ORDER BY d.id DESC
            LIMIT ?
            """,
            (limit,),
        )
        count = 0
        for draft_id, raw_text in rows or []:
            parsed_data = self.llm_client.parse_text_with_llm(raw_text or "")
            non_rental_reasons = self._extract_non_rental_reasons(parsed_data, raw_text or "")
            if non_rental_reasons:
                flags = [str(f) for f in (parsed_data.get("quality_flags") or [])]
                for reason in non_rental_reasons:
                    marker = f"non_rental_{reason}"
                    if marker not in flags:
                        flags.append(marker)
                parsed_data["quality_flags"] = flags
                parsed_data["quality_score"] = 0
            self._apply_parsed_data_to_draft(draft_id, parsed_data)
            count += 1
        return count

    def refresh_pending_drafts(self, limit: int = 200) -> int:
        """重跑所有 pending 草稿解析，用于规则升级后的批量回填。"""
        rows = self.db_manager._fetch_all(
            """
            SELECT d.draft_id, sp.raw_text
            FROM drafts d
            JOIN source_posts sp ON sp.id = d.source_post_id
            WHERE d.review_status = 'pending'
            ORDER BY d.id DESC
            LIMIT ?
            """,
            (limit,),
        )
        count = 0
        for draft_id, raw_text in rows or []:
            parsed_data = self.llm_client.parse_text_with_llm(raw_text or "")
            non_rental_reasons = self._extract_non_rental_reasons(parsed_data, raw_text or "")
            if non_rental_reasons:
                flags = [str(f) for f in (parsed_data.get("quality_flags") or [])]
                for reason in non_rental_reasons:
                    marker = f"non_rental_{reason}"
                    if marker not in flags:
                        flags.append(marker)
                parsed_data["quality_flags"] = flags
                parsed_data["quality_score"] = 0
            self._apply_parsed_data_to_draft(draft_id, parsed_data)
            count += 1
        return count

    def normalize_pending_area_labels(self, limit: int = 500) -> int:
        """修正 pending 中明显异常的 area 值，回退为“金边”并记录备注。"""
        rows = self.db_manager._fetch_all(
            """
            SELECT draft_id, area, review_note
            FROM drafts
            WHERE review_status='pending'
              AND area IS NOT NULL
              AND TRIM(area) <> ''
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        if not rows:
            return 0

        valid_tokens = (
            "bkk",
            "金边",
            "chamkar",
            "mon",
            "tonle",
            "bassac",
            "daun",
            "penh",
            "sen sok",
            "kork",
            "makara",
            "russian",
            "市场",
            "大道",
            "区",
            "钻石岛",
            "一号公路",
            "永旺",
        )
        noisy_tokens = ("啊雷莎", "阿雷莎")

        fixed = 0
        for draft_id, area, review_note in rows:
            area_text = str(area or "").strip()
            area_lower = area_text.lower()
            if not area_text:
                continue
            is_valid = any(tok in area_lower or tok in area_text for tok in valid_tokens)
            is_noisy = any(tok in area_text for tok in noisy_tokens)
            if is_valid and not is_noisy:
                continue

            old_note = str(review_note or "").strip()
            marker = f"area_normalized:{area_text}->金边"
            merged_note = old_note
            if marker not in old_note:
                merged_note = f"{old_note} | {marker}".strip(" |")[:500]

            self.db_manager.update_draft(
                draft_id,
                area="金边",
                review_note=merged_note,
            )
            fixed += 1
        return fixed
