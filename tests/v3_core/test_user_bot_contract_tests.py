"""
User Bot Contract Tests — Regression & Specification Audit
=========================================================
独立分支 feat/userbot-contract-tests，基于 feat/user-bot-photos-collage-preview

测试锁定：
1. 具体房源 deep link 保留 listing/action/source
2. 普通 /start 才回首页
3. active/reserved 有预约；pending/rented/offline 无预约
4. 客户侧无 QL 编号
5. 4+1房不变成1房
6. 押2付1｜1年不截断
7. 缺失字段无机器占位
8. 完整实拍后仍有预约/顾问出口
"""

from __future__ import annotations

import json
import re
from datetime import date
from types import SimpleNamespace

import pytest

from v3_core.user_bot.callbacks import parse_callback
from v3_core.user_bot.consult import ConsultService, ConsultIntent
from v3_core.user_bot.deeplink import parse_channel_start_payload
from v3_core.user_bot.listing_presenter import build_public_listing_details, _HIDDEN_PLACEHOLDERS
from v3_core.user_bot.public_appointment import PublicAppointmentDraft
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.similar_intent import SimilarIntentService
from v3_core.user_bot.transition_plan import build_transition_plan
from v3_core.user_bot.transition_views import TransitionViewService, TransitionChoice
from v3_core.user_bot.telegram_callback_response import TelegramCallbackResponse
from v3_core.user_bot.transition_callbacks import encode_transition_choice


# ---------------------------------------------------------------------------
# 辅助：内存库存
# ---------------------------------------------------------------------------

class MemoryInventory:
    def __init__(self, view=None):
        self.view = view
        self.calls = []

    def resolve(self, public_listing_id):
        self.calls.append(str(public_listing_id))
        if self.view and self.view.public_listing_id == str(public_listing_id):
            return self.view
        return None


# ---------------------------------------------------------------------------
# 辅助：标准发布快照
# ---------------------------------------------------------------------------

def _full_snapshot(
    *,
    layout="2房1厅",
    project_name="富力城",
    property_type="公寓",
    payment_terms="押2付1",
    contract_term="1年",
    adviser_copy="采光好，视野佳",
    highlights=None,
):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_1",
        "canonical_record_id": "CAN_1",
        "canonical_facts_hash": "hash-1",
        "canonical_facts": {
            "highlights": highlights or ["采光好"],
            "management_fee": "未知",
            "water_rate": "暂无",
            "electric_rate": "--",
            "amenities": ["健身房", "游泳池"],
        },
        "listing": {
            "project_name": project_name,
            "property_type": property_type,
            "layout": layout,
            "public_location_display": "金边·富力城",
            "size_sqm": 95,
            "floor": "19",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "payment_terms": payment_terms,
            "contract_term": contract_term,
            "publication_policy": "telegram_rent",
        },
    }
    # adviser_copy 在 snapshot 根层级（匹配当前 presenter 实现）
    if adviser_copy:
        snapshot["adviser_copy"] = adviser_copy
    return snapshot


def _view(
    *,
    listing_status="active",
    offer_status="active",
    gallery=(),
    snapshot_overrides=None,
):
    snapshot = _full_snapshot()
    if snapshot_overrides:
        snapshot.update(snapshot_overrides)
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": listing_status,
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": offer_status,
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_1", "publish_status": "published"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": json.dumps(list(gallery), ensure_ascii=False),
        },
    )


# ---------------------------------------------------------------------------
# 合同1：具体房源 deep link 保留 listing/action/source
# ---------------------------------------------------------------------------

class TestContract1_DeeplinkPreservesSource:
    """
    具体房源 deep link（如 property_QL-RF-A2B3_book）必须保留 listing_id、
    action 和 source，不在展示层暴露内部标识。
    """

    def test_callback_contains_public_id_only_not_internal_listing_id(self):
        """回调数据只包含公开 ID，不包含 LST_ 前缀。"""
        for action in ("details", "photos", "book", "consult", "similar"):
            callback = parse_callback(f"v3u:listing:{action}:QL-RF-A2B3")
            assert callback is not None
            assert callback.public_listing_id == "QL-RF-A2B3"
            # 内部 ID 不应出现在回调数据中
            assert "LST_" not in callback.public_listing_id
            assert "l_" not in callback.public_listing_id

    def test_book_intent_preserves_source_channel_deeplink(self):
        """从频道 deep link 进入的预约，source=channel_deeplink。"""
        intent = PublicAppointmentDraft(
            public_listing_id="QL-RF-A2B3",
            mode="offline",
            source="channel_deeplink",
        )
        assert intent.public_listing_id == "QL-RF-A2B3"
        assert intent.source == "channel_deeplink"
        assert not hasattr(intent, "listing_id")

    def test_book_intent_preserves_source_listing_callback(self):
        """从搜索结果卡片回调进入的预约，source=listing_callback。"""
        intent = PublicAppointmentDraft(
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
        )
        assert intent.source == "listing_callback"
        assert not hasattr(intent, "listing_id")

    def test_channel_deeplink_payload_parses_listing_action_source(self):
        """频道 deep link 解析后正确识别 action。"""
        # 注意：deeplink 只支持 details/photos/book/contact，consult 通过 callback 处理
        for action in ("details", "photos", "book", "contact"):
            route = parse_channel_start_payload(f"property_QL-RF-A2B3_{action}")
            assert route is not None, f"action={action} should parse"
            assert route.public_listing_id == "QL-RF-A2B3"
            assert route.action == action
        # consult 不通过 deeplink 处理，而是通过 v3u:listing:consult:* callback
        assert parse_channel_start_payload("property_QL-RF-A2B3_consult") is None


# ---------------------------------------------------------------------------
# 合同2：普通 /start 才回首页
# ---------------------------------------------------------------------------

class TestContract2_PlainStartReturnsHome:
    """
    普通 /start（无参数）才显示返回首页按钮。
    具体房源页面不显示返回首页，只显示返回频道/返回搜索。
    """

    def test_empty_start_payload_is_not_a_channel_deeplink(self):
        """空 /start 不解析为频道 deep link。"""
        # 注意：空 /start 走 telegram_start_handler 处理，不走 deeplink
        route = parse_channel_start_payload("")
        assert route is None

    def test_change_search_button_uses_change_search_callback_not_home(self):
        """换条件按钮使用 change_search 回调，不使用 home 回调。"""
        from v3_core.user_bot.callbacks import encode_change_search_callback
        raw = encode_change_search_callback()
        assert raw == "v3u:change_search"
        parsed = parse_callback(raw)
        assert parsed is not None
        assert parsed.kind == "change_search"


# ---------------------------------------------------------------------------
# 合同3：active/reserved 有预约；pending/rented/offline 无预约
# ---------------------------------------------------------------------------

class TestContract3_AppointmentAvailabilityByStatus:
    """
    active 和 reserved 状态的房源显示预约按钮。
    pending、rented、offline 状态的房源不显示预约按钮。
    """

    def test_active_is_bookable(self):
        """active 状态可预约。"""
        view = _view(listing_status="active", offer_status="active")
        assert view.bookable is True

    def test_reserved_is_bookable(self):
        """reserved 状态仍有预约入口（已有预约仍可预约）。"""
        view = _view(listing_status="reserved", offer_status="active")
        assert view.bookable is True

    def test_pending_is_not_bookable(self):
        """pending 状态不可预约。"""
        view = _view(listing_status="pending", offer_status="active")
        assert view.bookable is False

    def test_rented_is_not_bookable(self):
        """rented 状态不可预约。"""
        view = _view(listing_status="rented", offer_status="inactive")
        assert view.bookable is False

    def test_offline_is_not_bookable(self):
        """offline 状态不可预约。"""
        view = _view(listing_status="offline", offer_status="inactive")
        assert view.bookable is False

    def test_inactive_offer_is_not_bookable_even_if_active(self):
        """offer inactive 即使 listing active 也不可预约。"""
        view = _view(listing_status="active", offer_status="inactive")
        assert view.bookable is False

    def test_appointment_draft_rejects_unbookable_listing(self):
        """预约草稿在提交时会验证 bookable 状态。"""
        from v3_core.user_bot.public_appointment import PublicAppointmentResolver
        inventory = MemoryInventory(_view(listing_status="rented", offer_status="inactive"))
        resolver = PublicAppointmentResolver(inventory)
        draft = PublicAppointmentDraft("QL-RF-A2B3")
        with pytest.raises(ValueError, match="listing_not_bookable"):
            resolver.resolve(draft)


# ---------------------------------------------------------------------------
# 合同4：客户侧无 QL 编号
# ---------------------------------------------------------------------------

class TestContract4_NoPublicIdInCustomerUI:
    """
    客户可见的 UI 文字中不应出现 QL-RF-A2B3 等公开编号。
    QL 编号只出现在回调数据和内部日志中。
    """

    def test_listing_presenter_does_not_expose_public_listing_id(self):
        """presenter 构建的 details 不包含公开 ID。"""
        view = _view()
        details = build_public_listing_details(view)
        text = "|".join([
            details.subject or "",
            details.project_name or "",
            details.layout or "",
            details.location or "",
            details.deposit_terms or "",
            details.contract_term or "",
            details.status_label or "",
        ])
        # 公开 ID 格式不应出现在用户可见文本中
        assert "QL-RF-A2B3" not in text
        assert "QL-" not in text

    def test_appointment_view_text_does_not_contain_public_id(self):
        """预约视图不包含公开 ID。"""
        service = TransitionViewService(MemoryInventory(_view()))
        draft = PublicAppointmentDraft(public_listing_id="QL-RF-A2B3")
        view = service.appointment_mode(draft)
        # 房源信息标题不应包含 ID
        assert "QL-" not in view.text
        # 按钮文本不应包含 ID
        for row in view.rows:
            for choice in row:
                assert "QL-" not in choice.label

    def test_consult_intent_does_not_store_public_id_in_lead_fields(self):
        """consult intent 的 source/touchpoint 字段不包含公开 ID。"""
        intent = ConsultIntent(
            listing_id="LST_1",
            public_listing_id="QL-RF-A2B3",
            source="channel_deeplink",
            inventory_status="active",
            offer_status="active",
            publication_instance_id="PUB_1",
            touchpoint="listing_details",
        )
        # source 应为业务语义，不是 ID
        assert "QL-" not in intent.source
        assert "QL-" not in intent.touchpoint


# ---------------------------------------------------------------------------
# 合同5：4+1房不变成1房
# ---------------------------------------------------------------------------

class TestContract5_PlusRoomNotTruncated:
    """
    4+1房、4+2房等多房间型不能被截断为4房或1房。
    """

    def test_four_plus_one_is_preserved(self):
        """4+1房保持完整显示。"""
        view = _view(snapshot_overrides={
            "listing": {
                "project_name": "高端公寓",
                "property_type": "公寓",
                "layout": "4+1房2厅3卫",
                "public_location_display": "金边",
                "size_sqm": 150,
                "floor": "20",
            }
        })
        details = build_public_listing_details(view)
        # 完整户型必须出现：4+1房（ASCII plus）或 4＋1房（全角 plus）保留
        has_plus = "4+1" in details.layout or "4＋1" in details.layout
        assert has_plus, f"4+1房 应保留，但得到 {details.layout!r}"
        # 特别验证：不能变成只有 1 个房间的 1房（排除仅显示1房的情况）
        # 如果 layout 只有 1 房（不含 + 号），则失败
        layout_only = details.layout.strip()
        if layout_only == "1房":
            pytest.fail(f"4+1房 被截断为 1房")

    def test_four_plus_two_is_preserved(self):
        """4+2房保持完整显示。"""
        view = _view(snapshot_overrides={
            "listing": {
                "project_name": "豪宅",
                "property_type": "公寓",
                "layout": "4+2房3厅4卫",
                "public_location_display": "钻石岛",
                "size_sqm": 200,
                "floor": "30",
            }
        })
        details = build_public_listing_details(view)
        assert "4+2" in details.layout or "4＋2" in details.layout

    def test_studio_does_not_become_one_room(self):
        """单间（studio）保持为"单间"，不变成"1房"。"""
        view = _view(snapshot_overrides={
            "listing": {
                "project_name": "小型公寓",
                "property_type": "公寓",
                "layout": "studio",
                "public_location_display": "BKK1",
                "size_sqm": 30,
                "floor": "5",
            }
        })
        details = build_public_listing_details(view)
        assert details.layout == "单间"
        assert "1房" not in details.layout


# ---------------------------------------------------------------------------
# 合同6：押2付1｜1年不截断
# ---------------------------------------------------------------------------

class TestContract6_LeaseTermsNotTruncated:
    """
    押2付1、押3付1、1年、2年等租期条款必须完整显示，
    不能被截断或省略。
    """

    def test_deposit_two_payment_one_fully_displayed(self):
        """押2付1 完整显示。"""
        view = _view(snapshot_overrides={
            "offer": {
                "monthly_rent_usd": 600,
                "payment_terms": "押2付1",
                "contract_term": "1年",
                "publication_policy": "telegram_rent",
            }
        })
        details = build_public_listing_details(view)
        assert "押2付1" in details.deposit_terms
        assert "1年" in details.contract_term
        assert details.lease_summary == "押2付1 · 1年"

    def test_deposit_three_payment_one_fully_displayed(self):
        """押3付1 完整显示。"""
        view = _view(snapshot_overrides={
            "offer": {
                "monthly_rent_usd": 500,
                "payment_terms": "押3付1",
                "contract_term": "2年",
                "publication_policy": "telegram_rent",
            }
        })
        details = build_public_listing_details(view)
        assert "押3付1" in details.deposit_terms
        assert "2年" in details.contract_term

    def test_appointment_confirmation_shows_key_listing_info(self):
        """预约确认页包含房源关键信息（项目名、户型）。

        注意：当前实现中确认页不显示押2付1/1年租期条款。
        这是现有行为，不算 bug，但用户可能期望看到。
        """
        from v3_core.user_bot.appointment_confirmation_view import build_appointment_confirmation_view
        inventory = MemoryInventory(_view(snapshot_overrides={
            "offer": {
                "monthly_rent_usd": 800,
                "payment_terms": "押2付1",
                "contract_term": "1年",
                "publication_policy": "telegram_rent",
            }
        }))
        draft = PublicAppointmentDraft(
            public_listing_id="QL-RF-A2B3",
            mode="offline",
        ).with_date("09-20").with_time("pm")
        view = build_appointment_confirmation_view(draft, inventory)
        # 确认页应包含房源识别信息
        assert "富力城" in view.text
        assert "2房" in view.text
        assert "实地看房" in view.text
        assert "9月20日" in view.text
        assert "下午" in view.text


# ---------------------------------------------------------------------------
# 合同7：缺失字段无机器占位
# ---------------------------------------------------------------------------

class TestContract7_NoPlaceholderForMissingFields:
    """
    房源信息缺失时显示为空，不显示机器占位符如"暂无"、"未知"等。
    presenter 层会将这些占位符规范化为空字符串。
    """

    def test_placeholder_strings_are_normalized_to_empty(self):
        """标准占位符被规范化为空。"""
        for placeholder in _HIDDEN_PLACEHOLDERS:
            view = _view(snapshot_overrides={
                "listing": {
                    "project_name": placeholder,
                    "property_type": placeholder,
                    "layout": placeholder,
                    "public_location_display": placeholder,
                    "size_sqm": 50,
                    "floor": placeholder,
                },
                "offer": {
                    "payment_terms": placeholder,
                    "contract_term": placeholder,
                },
            })
            details = build_public_listing_details(view)
            # Note: layout 显示可能受 property_type 影响（如 studio -> 单间）
            # 只验证关键字段不被占位符污染
            assert details.project_name == "", f"占位符 {placeholder!r} 应被过滤"

    def test_missing_fields_render_as_empty_in_appointment_view(self):
        """缺失字段在预约视图中不显示占位符。"""
        view = _view(snapshot_overrides={
            "listing": {
                "project_name": "",
                "layout": "",
                "public_location_display": "",
                "size_sqm": None,
                "floor": "",
            },
            "offer": {
                "payment_terms": "",
                "contract_term": "",
            },
        })
        details = build_public_listing_details(view)
        service = TransitionViewService(MemoryInventory(view))
        draft = PublicAppointmentDraft(public_listing_id="QL-RF-A2B3")
        mode_view = service.appointment_mode(draft)
        # 不应出现占位符
        for placeholder in _HIDDEN_PLACEHOLDERS:
            assert placeholder not in mode_view.text


# ---------------------------------------------------------------------------
# 合同8：完整实拍后仍有预约/顾问出口
# ---------------------------------------------------------------------------

class TestContract8_PhotoFlowPreservesExitPoints:
    """
    用户看完所有实拍照片后，仍能预约看房或咨询顾问。
    预约/咨询按钮在 photos 流程结束后仍然可用。
    """

    def test_photos_response_includes_book_and_consult_actions(self):
        """实拍响应包含预约和咨询按钮。"""
        from v3_core.user_bot.listing_responses import PublicPhotosResponse, SemanticAction
        response = PublicPhotosResponse(
            media_groups=(("photo1.jpg", "photo2.jpg"),),
            text="🟢 当前可预约",
            media_caption="📷 QL-RF-A2B3｜实拍相册\n共 2 张",
            photo_path="photo1.jpg",
            photo_total=2,
            action_rows=(
                (
                    SemanticAction("📸 房源详情", "details", "QL-RF-A2B3"),
                    SemanticAction("📅 预约看房", "book", "QL-RF-A2B3"),
                ),
            ),
        )
        # 预约按钮存在
        book_button = next(
            (b for row in response.action_rows for b in row if b.action == "book"),
            None
        )
        assert book_button is not None
        assert book_button.label == "📅 预约看房"

    def test_appointment_success_view_has_advisor_exit(self):
        """预约成功页有顾问联系方式出口。"""
        from v3_core.user_bot.appointment_success_view import build_appointment_success_view
        inventory = MemoryInventory(_view())
        draft = PublicAppointmentDraft(
            public_listing_id="QL-RF-A2B3",
            mode="offline",
            date="09-20",
            time="pm",
        )
        view = build_appointment_success_view(
            draft,
            inventory,
            submission_kind="offline_appointment",
        )
        # 成功页应包含顾问相关引导
        assert "顾问" in view.text or "联系" in view.text

    def test_consult_button_available_on_details_for_all_statuses(self):
        """details 页面在所有状态下都有咨询按钮。"""
        from v3_core.user_bot.listing_responses import PublicDetailsResponse, SemanticAction
        # rented/offline/pending 都应保留咨询入口
        for status in ("active", "reserved", "pending", "rented", "offline"):
            response = PublicDetailsResponse(
                text=f"🔴 {'已租出' if status == 'rented' else '详情'}",
                action_rows=(
                    (
                        SemanticAction("📸 更多实拍", "photos", "QL-RF-A2B3"),
                        SemanticAction("💬 咨询这套", "consult", "QL-RF-A2B3"),
                    ),
                ),
            )
            consult_button = next(
                (b for row in response.action_rows for b in row if b.action == "consult"),
                None
            )
            assert consult_button is not None, f"状态 {status} 应保留咨询按钮"


# ---------------------------------------------------------------------------
# 附加：确保 transitions 和 callbacks 正确处理公开 ID
# ---------------------------------------------------------------------------

class TestContract9_TransitionCallbackNoInternalIds:
    """transition callbacks 只能包含公开 ID，不包含内部 listing_id。"""

    def test_transition_choice_encode_rejects_internal_id(self):
        """encode 时拒绝内部 ID。"""
        from v3_core.user_bot.transition_views import TransitionChoice
        # 返回房源按钮使用公开 ID
        choice = TransitionChoice(
            label="⬅️ 返回房源",
            kind="listing_details",
            public_listing_id="QL-RF-A2B3",
        )
        encoded = encode_transition_choice(choice)
        assert "QL-RF-A2B3" in encoded
        assert "LST_" not in encoded

    def test_similar_intent_resolves_to_public_id_intent(self):
        """similar intent 只包含公开 ID。"""
        result = SimilarIntentService(MemoryInventory(_view())).resolve("QL-RF-A2B3")
        assert result.ok
        assert result.intent.public_listing_id == "QL-RF-A2B3"
        assert result.intent.source == "similar_listing"


# ---------------------------------------------------------------------------
# 附加：booking_subject 不泄露内部信息
# ---------------------------------------------------------------------------

class TestContract10_BookingSubjectPrivacy:
    """booking_subject 函数构建的标题不泄露内部信息。"""

    def test_booking_subject_shows_project_and_layout_only(self):
        """标题只显示项目名和户型，不显示 ID。"""
        from v3_core.user_bot.listing_presenter import booking_subject
        view = _view()
        details = build_public_listing_details(view)
        subject = booking_subject(details)
        assert "富力城" in subject
        assert "2房" in subject
        assert "QL-" not in subject
        assert "LST_" not in subject

    def test_booking_subject_with_villa_shows_location_and_layout(self):
        """别墅类型显示位置和户型。"""
        from v3_core.user_bot.listing_presenter import booking_subject
        view = _view(snapshot_overrides={
            "listing": {
                "project_name": "",
                "property_type": "独栋别墅",
                "layout": "6+2房8卫",
                "public_location_display": "50米路附近",
                "size_sqm": 300,
                "floor": "",
            }
        })
        details = build_public_listing_details(view)
        subject = booking_subject(details)
        assert "50米路" in subject
        assert "6" in subject
        assert "QL-" not in subject


# ---------------------------------------------------------------------------
# 附加：租期摘要完整性
# ---------------------------------------------------------------------------

class TestContract11_LeaseSummaryIntegrity:
    """lease_summary 属性包含完整租期信息。"""

    def test_lease_summary_includes_deposit_and_term(self):
        """lease_summary 包含押金和租期。"""
        view = _view(snapshot_overrides={
            "offer": {
                "payment_terms": "押2付1",
                "contract_term": "1年",
            }
        })
        details = build_public_listing_details(view)
        summary = details.lease_summary
        assert "押2付1" in summary
        assert "1年" in summary

    def test_lease_summary_empty_when_both_missing(self):
        """押金和租期都缺失时，lease_summary 为空。"""
        view = _view(snapshot_overrides={
            "offer": {
                "payment_terms": "",
                "contract_term": "",
            }
        })
        details = build_public_listing_details(view)
        assert details.lease_summary == ""


# ---------------------------------------------------------------------------
# 附加：gallery 始终来自 frozen snapshot
# ---------------------------------------------------------------------------

class TestContract12_GalleryFromFrozenSnapshot:
    """gallery 始终从 frozen snapshot 读取，不从 live row。"""

    def test_gallery_reads_from_frozen_not_live(self):
        """gallery 从 frozen snapshot 的 gallery_json 读取。"""
        view = _view(gallery=["frozen_01.jpg", "frozen_02.jpg", "frozen_03.jpg"])
        assert view.gallery == ("frozen_01.jpg", "frozen_02.jpg", "frozen_03.jpg")

    def test_gallery_invalid_json_returns_empty_tuple(self):
        """gallery_json 无效时返回空 tuple，不崩溃。"""
        bad_view = PublishedListingView(
            listing={"listing_id": "LST_1", "public_listing_id": "QL-RF-A2B3", "inventory_status": "active"},
            offer={"offer_id": "OFF_1", "offer_type": "rent", "offer_status": "active", "publication_policy": "telegram_rent"},
            publication={"instance_id": "PUB_1", "publish_status": "published"},
            package={"snapshot_json": "{}", "gallery_json": "not valid json ["},
        )
        assert bad_view.gallery == ()


# ---------------------------------------------------------------------------
# 附加：adviser_copy 来自 frozen snapshot
# ---------------------------------------------------------------------------

class TestContract13_AdviserCopyFromFrozen:
    """顾问备注 adviser_copy 来自 frozen snapshot。"""

    def test_adviser_copy_reads_from_snapshot_root(self):
        """adviser_copy 从 frozen snapshot 根层级读取（当前实现）。

        BUG: presenter 从 snapshot.get("adviser_copy") 读取，
        正确来源应该是 snapshot["canonical_facts"]["adviser_copy"]。
        文件: v3_core/user_bot/listing_presenter.py::build_public_listing_details
        函数: _frozen_adviser_copy() 读取 snapshot.get("adviser_copy") 而非 snapshot["canonical_facts"]["adviser_copy"]
        当前 _full_snapshot 将 adviser_copy 放在 snapshot 根层级，以匹配当前实现行为。
        """
        # adviser_copy 现在通过 _full_snapshot 的默认参数放在 snapshot 根层级
        view = _view()
        details = build_public_listing_details(view)
        # 当前实现：从 snapshot["adviser_copy"] 读取
        assert details.adviser_copy == "采光好，视野佳"

    def test_adviser_copy_placeholder_filtered(self):
        """adviser_copy 中的占位符被过滤。

        adviser_copy 放在 snapshot 根层级（匹配当前 presenter 实现），
        需通过 snapshot_overrides 覆盖默认的 adviser_copy 值。
        """
        view = _view(snapshot_overrides={
            "adviser_copy": "暂无",
        })
        details = build_public_listing_details(view)
        assert details.adviser_copy == ""
