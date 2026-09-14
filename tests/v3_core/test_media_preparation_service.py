from pathlib import Path
import hashlib

from PIL import Image, ImageDraw

from v3_core.ingest.source_reader import SourceReader
from v3_core.ingest.source_repository import SourceRepository
from v3_core.media.service import MediaPreparationService
from v3_core.storage.bootstrap import initialize_v3_storage


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


def _sha(path: str | Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _source(tmp_path: Path, name: str = "media"):
    db = tmp_path / f"{name}.sqlite3"
    initialize_v3_storage(db)
    repo = SourceRepository(str(db))
    paths = [_image(tmp_path / f"{name}-{i}.jpg", 30 + i * 20) for i in range(4)]
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
        dedupe_hash=f"hash-{name}",
    )
    repo.save_source_images(
        source_id,
        [{"local_path": path, "file_hash": f"hash-{name}-{i}"} for i, path in enumerate(paths)],
    )
    return db, source_id, paths


def test_media_preparation_scrubs_to_derived_files_before_selection(tmp_path):
    db, source_id, paths = _source(tmp_path)
    before = [_sha(path) for path in paths]
    prepared_dir = tmp_path / "prepared"

    prepared = MediaPreparationService(
        SourceReader(str(db)), prepared_dir=prepared_dir
    ).prepare(source_post_id=source_id)

    assert prepared.source_post_id == source_id
    assert prepared.cover_source_path not in prepared.gallery_paths
    assert len(prepared.gallery_paths) >= 1
    assert prepared.source_identity["source_post_db_id"] == source_id
    assert prepared.source_identity["source_post_id"] == "100"
    assert all(Path(path).is_file() for path in prepared.gallery_paths)
    assert all(prepared_dir in Path(path).parents for path in prepared.gallery_paths)
    assert all(Path(path).parent.name == "gallery" for path in prepared.gallery_paths)
    assert all(Path(path).name.endswith("_gallery.jpg") for path in prepared.gallery_paths)
    assert prepared.source_identity["gallery_brand_revision"] == "qiaolian_gallery_logo_v1"
    assert all(str(Path(path).resolve()) not in prepared.gallery_paths for path in paths)
    assert [_sha(path) for path in paths] == before


def test_manual_raw_cover_maps_to_its_derived_media(tmp_path):
    db, source_id, paths = _source(tmp_path, "manual")
    prepared_dir = tmp_path / "prepared-manual"
    prepared = MediaPreparationService(
        SourceReader(str(db)), prepared_dir=prepared_dir
    ).prepare(source_post_id=source_id, manual_cover_path=paths[2])

    assert prepared.cover_source_path not in prepared.gallery_paths
    assert prepared.cover_source_path != str(Path(paths[2]).resolve())
    expected_service = MediaPreparationService(
        SourceReader(str(db)), prepared_dir=prepared_dir
    )
    expected = expected_service._scrubbed_paths(source_post_id=source_id, paths=paths)[1]
    assert prepared.cover_source_path == expected[str(Path(paths[2]).resolve())]


def test_scrub_over_eight_percent_falls_back_to_untouched_derived_copy(tmp_path, monkeypatch):
    db, source_id, paths = _source(tmp_path, "reject")
    before = [_sha(path) for path in paths]

    def fake_scrub(src, dst, *, prefer_crop=False):
        Path(dst).write_bytes(Path(src).read_bytes())
        return {"coverage": 0.081, "methods": ["inpaint_telea"]}

    monkeypatch.setattr("v3_core.media.service.scrub_file", fake_scrub)
    prepared_dir = tmp_path / "risky"
    prepared = MediaPreparationService(
        SourceReader(str(db)), prepared_dir=prepared_dir
    ).prepare(source_post_id=source_id)

    assert prepared.gallery_paths
    assert all(Path(path).name.endswith("_gallery.jpg") for path in prepared.gallery_paths)
    assert all(prepared_dir in Path(path).parents for path in prepared.gallery_paths)
    assert [_sha(path) for path in paths] == before
