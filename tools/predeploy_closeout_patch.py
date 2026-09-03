from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, got {count}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


package = ROOT / "publication_package.py"
collector = ROOT / "collector_bot.py"
test_file = ROOT / "tests" / "test_launch_readiness_contract.py"

replace_once(
    package,
    '''    processed = []\n    badge = "微信实拍" if routing["source_type"] == "wechat" else ""\n    detail_sources = [path for path in originals if path != cover_source]\n    for index, path in enumerate(detail_sources, start=2):\n        target = out / f"image_{index:02d}.jpg"\n        # Package 阶段生成最终详情字节；Publisher 之后只读取并发送，不再加 Logo/调色/裁切。\n        _finalize_detail_image(path, str(target)); processed.append(str(target))\n''',
    '''    processed = []\n    badge = "微信实拍" if routing["source_type"] == "wechat" else ""\n    # 封面是从同一套原图生成的衍生资产，不应吞掉那张原始实拍。\n    # 完整实拍始终按去重后的来源顺序保留全部原图；封面单独作为频道主图。\n    detail_sources = list(originals)\n    for index, path in enumerate(detail_sources, start=1):\n        target = out / f"image_{index:02d}.jpg"\n        # Package 阶段生成最终详情字节；Publisher 之后只读取并发送，不再加 Logo/调色/裁切。\n        _finalize_detail_image(path, str(target)); processed.append(str(target))\n''',
)

replace_once(
    collector,
    '''    source_kind = str(source_cfg.get("source_type") or "telegram_channel")\n    if source_kind == "telegram_channel" and len(raw_images) < MIN_LISTING_IMAGES:\n        _inc_stat("skipped_few_images", 1)\n        logger.info(\n            "跳过少图房源 source=%s post=%s images=%s min=%s",\n            source_cfg.get("source_name", ""), source_post_id,\n            len(raw_images), MIN_LISTING_IMAGES,\n        )\n        return {"status": "skipped", "reason": "fewer_than_min_images"}\n    source_type = source_cfg.get("source_type", "telegram_channel")\n''',
    '''    source_kind = str(source_cfg.get("source_type") or "telegram_channel")\n    insufficient_media = (\n        source_kind == "telegram_channel" and len(raw_images) < MIN_LISTING_IMAGES\n    )\n    source_type = source_cfg.get("source_type", "telegram_channel")\n''',
)

replace_once(
    collector,
    '''        "visual_review_required": bool(raw_images),\n        "republish_policy": "facts_only_review_required",\n    }\n''',
    '''        "visual_review_required": bool(raw_images),\n        "republish_policy": "facts_only_review_required",\n        "media_status": "insufficient_media" if insufficient_media else "sufficient_media",\n        "min_listing_images": MIN_LISTING_IMAGES,\n    }\n''',
)

replace_once(
    collector,
    '''            dedupe_hash=dedupe_hash,\n            parse_status="pending",\n        )\n''',
    '''            dedupe_hash=dedupe_hash,\n            parse_status="insufficient_media" if insufficient_media else "pending",\n        )\n''',
)

replace_once(
    collector,
    '''        classification = (\n            await _classify_and_package(post_pk)\n            if classify_after_insert\n            else {"status": "deferred"}\n        )\n''',
    '''        if insufficient_media:\n            _inc_stat("skipped_few_images", 1)\n            _inc_stat(f"source.{source_name}.insufficient_media", 1)\n            logger.info(\n                "少图房源已保留 RAW，暂不成包 source=%s post=%s images=%s min=%s",\n                source_name, source_post_id, len(raw_images), MIN_LISTING_IMAGES,\n            )\n            classification = {\n                "status": "insufficient_media",\n                "image_count": len(raw_images),\n                "min_images": MIN_LISTING_IMAGES,\n            }\n        elif classify_after_insert:\n            classification = await _classify_and_package(post_pk)\n        else:\n            classification = {"status": "deferred"}\n''',
)

text = test_file.read_text(encoding="utf-8")
append = '''\n\ndef test_cover_source_remains_in_complete_gallery():\n    source = Path("publication_package.py").read_text(encoding="utf-8")\n    assert "detail_sources = list(originals)" in source\n    assert "path for path in originals if path != cover_source" not in source\n\n\ndef test_under_four_photos_are_retained_as_insufficient_media():\n    source = Path("collector_bot.py").read_text(encoding="utf-8")\n    assert 'parse_status="insufficient_media" if insufficient_media else "pending"' in source\n    assert '"media_status": "insufficient_media" if insufficient_media else "sufficient_media"' in source\n    assert 'return {"status": "skipped", "reason": "fewer_than_min_images"}' not in source\n\n\ndef test_deploy_gate_covers_current_launch_suite_and_browser_runtime():\n    workflow = Path(".github/workflows/qiaolian-production-deploy.yml").read_text(encoding="utf-8")\n    for required in (\n        "tests/test_launch_readiness_contract.py",\n        "tests/test_parser_v12_regressions.py",\n        "tests/test_parser_v2_safe.py",\n        "tests/test_media_pipeline_v11.py",\n        "PLAYWRIGHT_BROWSERS_PATH",\n        "playwright install chromium",\n    ):\n        assert required in workflow\n    assert "PROD_SSH_USER: '${{ secrets.PROD_SSH_USER }}'" not in workflow\n'''
if "test_cover_source_remains_in_complete_gallery" not in text:
    test_file.write_text(text.rstrip() + append.rstrip() + "\n", encoding="utf-8")

print("predeploy closeout patch applied")
