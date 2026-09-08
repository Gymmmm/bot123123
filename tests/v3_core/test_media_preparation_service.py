from pathlib import Path

from PIL import Image, ImageDraw

from v3_core.ingest.source_reader import SourceReader
from v3_core.ingest.source_repository import SourceRepository
from v3_core.media.service import MediaPreparationService


def _image(path: Path, value: int):
    """Create a deterministic room-like image that satisfies production media gates."""
    image = Image.new("RGB", (960, 720), (value + 45, value + 35, value + 25))
    draw = ImageDraw.Draw(image)
    draw.rectangle((55, 55, 905, 665), outline=(238, 238, 230), width=10)
    draw.rectangle((120, 135, 480, 430), fill=(185, 205, 220), outline=(60, 75, 90), width=8)
    draw.rectangle((545, 150, 835, 510), fill=(215, 190, 150), outline=(80, 65, 45), width=8)
    draw.line((80, 590, 880, 590), fill=(65, 65, 65), width=12)
    draw.line((180, 590, 260, 470), fill=(95, 75, 55), width=9)
    draw.line((760, 590, 680, 470), fill=(95, 75, 55), width=9)
    for x in range(110, 880, 80):
        draw.line((x, 85, x + 35, 115), fill=(110 + value % 60, 90, 80), width=4)
    image.save(path, "JPEG", quality=94)
    return str(path)


def test_media_preparation_reads_source_order_and_selects_one_cover(tmp_path):
    db = tmp_path / "media.sqlite3"
    repo = SourceRepository(str(db))
    paths = [_image(tmp_path / f"{i}.jpg", 30 + i * 20) for i in range(4)]
    source_id = repo.save_source_post(
        source_id=None,
        source_type="telegram_channel",
        source_name="collector",
        source_post_id="100",
        source_url="https://t.me/c/1/100",
        source_author="channel",
        raw_text="富力城 2房1厅 月租 $800/月",
        raw_images=[{"local_path": path} for path in paths],
        raw_videos=[],
        raw_contact="",
        raw_meta={"sanitized_text": "富力城 2房1厅 月租 $800/月"},
        dedupe_hash="hash-100",
    )
    repo.save_source_images(
        source_id,
        [{"local_path": path, "file_hash": f"hash-{i}"} for i, path in enumerate(paths)],
    )

    reader = SourceReader(str(db))
    prepared = MediaPreparationService(reader).prepare(source_post_id=source_id)

    assert prepared.source_post_id == source_id
    assert prepared.cover_source_path in prepared.gallery_paths
    assert len(prepared.gallery_paths) >= 1
    assert prepared.source_identity["source_post_db_id"] == source_id
    assert prepared.source_identity["source_post_id"] == "100"
    assert all(Path(path).is_file() for path in prepared.gallery_paths)


def test_manual_cover_is_honoured_only_when_it_is_usable_source_media(tmp_path):
    db = tmp_path / "media-manual.sqlite3"
    repo = SourceRepository(str(db))
    paths = [_image(tmp_path / f"m{i}.jpg", 50 + i * 25) for i in range(4)]
    source_id = repo.save_source_post(
        source_id=None,
        source_type="manual",
        source_name="admin_import",
        source_post_id="manual-1",
        source_url="",
        source_author="admin",
        raw_text="BKK1 公寓 1房 月租 $600/月",
        raw_images=[{"local_path": path} for path in paths],
        raw_videos=[],
        raw_contact="",
        raw_meta={},
        dedupe_hash="manual-hash",
    )
    repo.save_source_images(
        source_id,
        [{"local_path": path, "file_hash": f"mhash-{i}"} for i, path in enumerate(paths)],
    )

    prepared = MediaPreparationService(SourceReader(str(db))).prepare(
        source_post_id=source_id,
        manual_cover_path=paths[2],
    )
    if paths[2] in prepared.gallery_paths:
        assert prepared.cover_source_path == paths[2]
