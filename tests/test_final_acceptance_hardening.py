from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from PIL import Image
from types import SimpleNamespace

import collector_bot
from ai_parser import AIParserModule
from collector_db_compat import DatabaseManager
from meihua_publisher import assert_public_output_safe
from publication_package import approve_package, build_package
import publication_package
from qiaolian_dual.canonical_facts import canonicalize_source, draft_projection
from qiaolian_dual.db import SCHEMA as USER_BOT_SCHEMA
from qiaolian_dual.listing_taxonomy import classify_listing_taxonomy
from qiaolian_dual.area_admin import set_canonical_area
from qiaolian_dual.canonical_fact_projection import validate_facts
from qiaolian_dual import common
from qiaolian_dual.flows import start_appointment
from qiaolian_dual.session_deeplink import parse_start_arg_payload
import meihua_publisher
from source_sanitizer import sanitize_source_text
from v2.qiaolian_publisher_v2.bot import PublisherBot


def _bootstrap(db_path: Path) -> None:
    root = Path(__file__).resolve().parent.parent
    with sqlite3.connect(db_path) as conn:
        conn.executescript((root / "schema_core.sql").read_text(encoding="utf-8"))
        conn.executescript(USER_BOT_SCHEMA)


def test_city_is_not_area_and_taxonomy_layers_stay_separate() -> None:
    city_only = canonicalize_source("金边公寓出租\n1房1卫\n租金 $500/月")
    assert city_only["canonical_area_key"] is None
    assert city_only["public_location_display"] is None
    assert "missing_public_location" in city_only["quality"]["blocking_flags"]

    facts = canonicalize_source(
        "金边 永旺一 Aeon1公寓出租\n区域：BKK1\n1房1卫\nRent $550/month"
    )
    assert facts["canonical_area_key"] == "BKK1"
    assert facts["project_name"] == "永旺一"
    assert facts["project_alias"] == "Aeon1"
    assert facts["property_type"] == "公寓"
    assert facts["public_location_display"] == "BKK1"
    projection = draft_projection(facts)
    assert projection["project"] == "永旺一"
    assert projection["area"] == "BKK1"
    assert len(projection["title"].split("｜")) == len(set(projection["title"].split("｜")))


def test_latin_aliases_are_tokens_and_explicit_unknown_project_is_preserved() -> None:
    taxonomy = classify_listing_taxonomy(
        "项目：Urban Village\n区域：洪森大道\n物业类型：公寓\n租金：$680/月"
    )
    assert taxonomy.project_name == "Urban Village"
    assert taxonomy.property_type == "公寓"
    assert taxonomy.property_type_status == "confirmed"
    assert taxonomy.property_subtype is None


def test_chinese_english_khmer_input_keeps_facts_separate() -> None:
    facts = canonicalize_source(
        "项目：Urban Village\n区域：洪森大道\nProperty type: apartment\n"
        "2 bedrooms / 2 bathrooms\nRent: $680/month\n"
        "កក់ 1 ខែ បង់មុន 1 ខែ\n家具家电齐全"
    )
    assert facts["project_name"] == "Urban Village"
    assert facts["canonical_area_key"] is None
    assert facts["market_location_keys"] == ["洪森大道"]
    assert facts["public_location_display"] == "洪森大道"
    assert facts["property_type"] == "公寓"
    assert facts["bedrooms"] == 2
    assert facts["bathrooms"] == 2
    assert facts["monthly_rent_usd"] == 680
    assert facts["deal_type"] == "rent"


def test_source_contacts_and_attribution_cannot_reach_public_output() -> None:
    raw = (
        "区域：BKK1\n公寓 1房1卫\n租金 $600/月\n"
        "来源频道：每日房源 @outside_agent\n联系人：+855 12 345 678"
    )
    cleaned = sanitize_source_text(raw)
    assert "BKK1" in cleaned.text and "$600" in cleaned.text
    assert "@outside_agent" not in cleaned.text
    assert "+855" not in cleaned.text
    inline = sanitize_source_text("租金 $600/月 联系微信：@outside_agent")
    assert inline.text == "租金 $600/月"
    inline_facts = canonicalize_source(raw, sanitized_text=cleaned.text)
    assert "@outside_agent" not in json.dumps(inline_facts, ensure_ascii=False)
    with pytest.raises(ValueError, match="public_output_source_contact"):
        assert_public_output_safe("咨询 @outside_agent", context="test")
    with pytest.raises(ValueError, match="public_output_source_attribution"):
        assert_public_output_safe("来源频道：每日房源", context="test")


def test_empty_database_compat_manager_does_not_attempt_invalid_alter(tmp_path: Path) -> None:
    manager = DatabaseManager(str(tmp_path / "empty.db"))
    with sqlite3.connect(manager.db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='drafts'"
        ).fetchone()[0] == 0


def test_duplicate_source_message_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "dedupe.db"
    _bootstrap(db_path)
    manager = DatabaseManager(str(db_path))
    images = []
    for index in range(4):
        image_path = tmp_path / f"dedupe-{index}.jpg"
        Image.new("RGB", (640, 480), (150 + index, 170, 190)).save(image_path)
        images.append({
            "local_path": str(image_path),
            "file_hash": hashlib.sha256(image_path.read_bytes()).hexdigest(),
            "telegram_file_id": f"f-{index}",
            "telegram_file_unique_id": f"u-{index}",
            "message_id": index + 1,
        })
    kwargs = dict(
        client=object(), source_cfg={"source_name": "admin", "source_type": "telegram_admin_upload"},
        chat_id=-1001, source_post_id="album_777", anchor_message_id=7,
        raw_text="区域：BKK1\n公寓 1房\n租金 $600/月", raw_images=images,
        raw_videos=[], grouped_id=777, ingest_kind="album", message_count=4,
    )
    with patch.object(collector_bot, "db_manager", manager), patch.object(
        collector_bot, "_classify_and_package", new=AsyncMock(return_value={"status": "deferred"})
    ):
        first = asyncio.run(collector_bot.persist_source_post(**kwargs))
        second = asyncio.run(collector_bot.persist_source_post(**kwargs))
    assert first["status"] == "inserted"
    assert second["status"] == "duplicate"
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM source_posts").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM media_assets").fetchone()[0] == 4


def test_manual_area_override_rehashes_canonical_facts_and_never_blanks(tmp_path: Path) -> None:
    db_path = tmp_path / "area.db"
    _bootstrap(db_path)
    facts = canonicalize_source("金边公寓出租\n1房1卫\n租金 $500/月")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO drafts(draft_id,listing_id,title,area,property_type,price,normalized_data,
               extracted_data,review_status,canonical_facts_hash,canonical_facts_schema)
               VALUES ('DRF_AREA','l_81','1房｜公寓','', '公寓',500,?,?, 'pending',?,?)""",
            (json.dumps(facts, ensure_ascii=False), json.dumps(facts, ensure_ascii=False), facts["canonical_facts_hash"], facts["schema_version"]),
        )
        conn.execute(
            """INSERT INTO listings(listing_id,title,property_type,area,community,price,currency,status,created_at,updated_at)
               VALUES ('l_81','1房｜公寓','公寓','','',500,'USD','pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"""
        )
    result = set_canonical_area(str(db_path), "l_81", "BKK1", "7", "管理员核对了原始地址")
    assert result["new_area"] == "BKK1"
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT area,normalized_data FROM drafts WHERE draft_id='DRF_AREA'").fetchone()
        new_facts = json.loads(row[1])
    assert row[0] == "BKK1"
    assert new_facts["canonical_area_key"] == "BKK1"
    assert validate_facts(new_facts) == []
    with pytest.raises(ValueError, match="area_not_in_canonical_catalog"):
        set_canonical_area(str(db_path), "l_81", "", "7", "blank")
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT area FROM drafts WHERE draft_id='DRF_AREA'").fetchone()[0] == "BKK1"


def test_known_appointment_mode_keeps_session_and_skips_reselection(monkeypatch: pytest.MonkeyPatch) -> None:
    render = AsyncMock()
    monkeypatch.setattr("qiaolian_dual.texts.render_panel", render)
    monkeypatch.setattr("qiaolian_dual.listing.listing_is_available", lambda _listing_id: (True, "published"))
    monkeypatch.setattr("qiaolian_dual.listing.listing_context", lambda _listing_id: {"title": "测试房源", "area": "BKK1", "layout": "1房", "price": 600})
    context = SimpleNamespace(user_data={})
    state = asyncio.run(start_appointment(SimpleNamespace(), context, "l_88", initial_mode="offline"))
    assert state == common.APPT_DATE
    assert context.user_data["appt"]["mode"] == "offline"
    assert context.user_data["appt"]["focus_keys"] == list(common.APPOINTMENT_FOCUS_ORDER)
    assert "第二步：选择方便的日期" in render.await_args.kwargs["text"]


def test_publication_token_links_preserve_consult_and_appointment_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(meihua_publisher, "BOT_USERNAME", "QiaoLianUserBot")
    links = meihua_publisher._caption_action_links(
        "l_88", listing={"area": "BKK1", "price": 600}, post_token="qlabc123"
    )
    assert "start=a__qlabc123" in links
    assert "start=q__qlabc123" in links
    appoint = parse_start_arg_payload("a__qlabc123")
    consult = parse_start_arg_payload("q__qlabc123")
    assert appoint and appoint["action"] == "appoint" and appoint["opaque_token"] == "qlabc123"
    assert consult and consult["action"] == "consult" and consult["opaque_token"] == "qlabc123"


def test_v2_legacy_variant_command_cannot_publish_to_channel() -> None:
    publisher = object.__new__(PublisherBot)
    publisher._ensure_admin = AsyncMock(return_value=True)
    reply = AsyncMock()
    bot = SimpleNamespace(send_message=AsyncMock())
    update = SimpleNamespace(effective_message=SimpleNamespace(reply_text=reply))
    context = SimpleNamespace(args=["DRF_unsafe"], bot=bot)
    asyncio.run(publisher.cmd_send_variants(update, context))
    bot.send_message.assert_not_awaited()
    assert "现场直发已停用" in reply.await_args.args[0]


def test_v2_cover_test_stays_in_admin_private_chat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    publisher = object.__new__(PublisherBot)
    publisher._ensure_admin = AsyncMock(return_value=True)
    publisher._cover_test_payload_from_db = lambda _draft_id: (
        {"project": "测试", "area": "BKK1", "property_type": "公寓", "price": "$600/月"},
        "",
        "测试数据",
    )
    publisher._runtime_render_dir = lambda: tmp_path
    monkeypatch.setattr(
        "v2.qiaolian_publisher_v2.bot.generate_house_cover",
        lambda _source, output, **_kwargs: Path(output).write_bytes(b"preview"),
    )
    reply = AsyncMock()
    send_photo = AsyncMock()
    update = SimpleNamespace(
        effective_message=SimpleNamespace(reply_text=reply),
        effective_chat=SimpleNamespace(id=778899),
    )
    context = SimpleNamespace(args=[], bot=SimpleNamespace(send_photo=send_photo))
    asyncio.run(publisher.cmd_cover_test(update, context))
    assert send_photo.await_count == 2
    assert {call.kwargs["chat_id"] for call in send_photo.await_args_list} == {778899}


def test_every_automatic_cover_route_points_to_an_existing_template() -> None:
    assert all(path.is_file() for path in publication_package.COVER_TEMPLATE_MAP.values())


def test_e2e_canonical_package_uses_one_source_and_freezes_after_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "e2e.db"
    _bootstrap(db_path)
    source_images = []
    for index in range(4):
        path = tmp_path / f"source-a-{index}.jpg"
        Image.new("RGB", (900 + index * 10, 650), (170, 180 + index, 195)).save(path)
        source_images.append(path)
    other = tmp_path / "source-b.jpg"
    Image.new("RGB", (900, 650), (20, 30, 40)).save(other)
    text = (
        "项目：Urban Village\n区域：洪森大道\n物业类型：公寓\n"
        "户型：2房1厅2卫\n面积：82㎡\n楼层：12楼\n租金：$680/月\n"
        "押一付一\n合同：一年\n家具家电齐全，采光好"
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO source_posts
               (source_type,source_name,source_post_id,raw_text,raw_images_json,raw_meta_json,dedupe_hash,parse_status)
               VALUES ('telegram','source_a','album_1',?,?,?,?,'pending')""",
            (
                text,
                json.dumps([{"local_path": str(p)} for p in source_images]),
                json.dumps({"grouped_id": "g1", "sanitized_text": text}),
                "hash-a",
            ),
        )
        source_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        for index, path in enumerate(source_images):
            conn.execute(
                """INSERT INTO media_assets
                   (asset_id,owner_type,owner_ref_id,owner_ref_key,asset_type,local_path,file_hash,sort_order)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (f"A-{index}", "source_post", source_id, str(source_id), "photo", str(path), hashlib.sha256(path.read_bytes()).hexdigest(), index),
            )
        conn.execute(
            """INSERT INTO source_posts
               (source_type,source_name,source_post_id,raw_text,raw_images_json,raw_meta_json,dedupe_hash,parse_status)
               VALUES ('telegram','source_b','album_2','other','$[]','{}','hash-b','pending')"""
        )
        other_source_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.execute(
            """INSERT INTO media_assets
               (asset_id,owner_type,owner_ref_id,owner_ref_key,asset_type,local_path,file_hash,sort_order)
               VALUES ('B-0','source_post',?,?,'photo',?,?,0)""",
            (other_source_id, str(other_source_id), str(other), hashlib.sha256(other.read_bytes()).hexdigest()),
        )
    stats = AIParserModule(str(db_path)).process_pending_source_posts()
    assert stats["parsed"] == 1
    with sqlite3.connect(db_path) as conn:
        draft_id = conn.execute(
            "SELECT draft_id FROM drafts WHERE source_post_id=?", (source_id,)
        ).fetchone()[0]
    monkeypatch.setattr("publication_package.PACKAGE_ROOT", tmp_path / "packages")
    # The container security profile forbids Chromium's process socket. Keep
    # this test focused on the data/package/freeze E2E boundary; the real HTML
    # renderer is exercised separately by deployment preflight on the host.
    def _isolated_cover(draft: dict, source: str, output: str, template: str) -> str:
        del draft, template
        with Image.open(source) as image:
            image.convert("RGB").resize((1280, 720)).save(output, "PNG")
        return output
    monkeypatch.setattr("publication_package._render_cover", _isolated_cover)
    package = build_package(str(db_path), str(draft_id), caption_variant_override="b")
    approved = approve_package(str(db_path), str(draft_id), "tester")
    assert approved["package_id"] == package["package_id"]
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM publication_packages WHERE package_id=?", (package["package_id"],)
        ).fetchone()
        identity = json.loads(row["source_identity_json"])
        assert identity["source_post_db_id"] == source_id
        assert identity["media_asset_ids"] == ["A-0", "A-1", "A-2", "A-3"]
        assert "B-0" not in identity["media_asset_ids"]
        frozen_hash = row["content_hash"]
        frozen_snapshot = row["snapshot_json"]
        snapshot = json.loads(frozen_snapshot)
        assert snapshot["caption_variant"] == "b"
        assert "Urban Village" in row["post_text"] or "洪森大道" in row["post_text"]
        assert "$680" in row["post_text"]
    with pytest.raises(ValueError, match="approved_package_frozen"):
        build_package(str(db_path), str(draft_id))
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT status,content_hash,snapshot_json FROM publication_packages WHERE package_id=?",
            (package["package_id"],),
        ).fetchone()
        assert row == ("approved", frozen_hash, frozen_snapshot)
