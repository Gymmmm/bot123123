import io
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from PIL import Image, ImageDraw

import meihua_publisher
from cover_generator import _draw_new_cover, _fit_single_line_text


class ChannelMobileListingV2Tests(unittest.TestCase):
    def test_pending_verification_and_numeric_size_are_not_misrepresented(self):
        caption = meihua_publisher.build_chinese_listing_post(
            {
                "listing_id": "l_0315",
                "area": "BKK1",
                "room_type": "2房",
                "price": 750,
                "size_sqm": 75,
                "verification_status": "待核验",
            }
        )
        self.assertIn("75㎡", caption)
        self.assertNotIn("发布前已核实", caption)

    def _listing(self):
        return {
            "draft_id": "DRAFT-1024",
            "listing_id": "l_1024",
            "project": "SKYTREE天空树",
            "area": "钻石岛",
            "layout": "1房",
            "price": 480,
            "size": "84㎡",
            "floor": "14",
            "deposit": "押1付1",
            "contract_term": "1年",
            "available_date": "8月1日",
            "normalized_data": {},
            "highlights": [],
        }

    def test_mobile_caption_is_compact_and_has_tracked_actions(self):
        with patch.object(meihua_publisher, "BOT_USERNAME", "QiaoLianUserBot"):
            caption = meihua_publisher.build_chinese_listing_post(
                self._listing(),
                caption_variant="a",
                post_token="abc123",
            )

        self.assertLessEqual(len(caption), 1024)
        self.assertIn("SKYTREE天空树｜1房", caption)
        self.assertIn("#钻石岛", caption)
        self.assertNotIn("您在金边的自己人", caption)
        self.assertNotIn("每月费用：", caption)

    def test_mobile_caption_only_promises_extra_photos_after_publish(self):
        caption = meihua_publisher.build_chinese_listing_post(
            self._listing(),
            has_extra_photos=True,
        )

        self.assertIn("更多实拍和费用说明见评论区", caption)

    def test_discussion_action_keyboard_only_keeps_two_high_intent_actions(self):
        with patch.object(meihua_publisher, "BOT_USERNAME", "QiaoLianUserBot"):
            keyboard = meihua_publisher._build_discussion_action_keyboard(
                "l_1024", "abc123"
            )

        rows = keyboard.inline_keyboard
        self.assertEqual([len(row) for row in rows], [2])
        self.assertEqual(rows[0][0].text, "💬 问清费用")
        self.assertEqual(rows[0][1].text, "📅 预约这套")
        self.assertIn("start=q__abc123__", rows[0][0].url)
        self.assertIn("start=a__abc123__", rows[0][1].url)

    def test_detail_logo_keeps_original_image_ratio(self):
        source = io.BytesIO()
        Image.new("RGB", (900, 600), (220, 220, 220)).save(source, "JPEG")
        result = meihua_publisher.add_detail_logo_watermark(
            source.getvalue(), self._listing()
        )
        with Image.open(result) as rendered:
            self.assertEqual(rendered.size, (900, 600))

    def test_detail_logo_uses_readable_ascii_when_cjk_font_is_missing(self):
        with patch("meihua_publisher.os.path.isfile", return_value=False):
            self.assertEqual(
                meihua_publisher._watermark_brand_lines(),
                ("QIAO LIAN", "PROPERTY · PHNOM PENH"),
            )

    def test_detail_logo_is_a_light_corner_badge_not_a_center_watermark(self):
        source = io.BytesIO()
        Image.new("RGB", (900, 600), (220, 220, 220)).save(source, "JPEG")

        result = meihua_publisher.add_detail_logo_watermark(
            source.getvalue(), self._listing()
        )

        with Image.open(result).convert("RGB") as rendered:
            # Uniform input makes the deterministic tie-break choose the top-left.
            badge_pixel = rendered.getpixel((20, 20))
            center_pixel = rendered.getpixel((450, 300))
            self.assertGreater(sum(abs(value - 220) for value in badge_pixel), 80)
            self.assertLess(sum(abs(value - 220) for value in center_pixel), 15)

    def test_cover_overlay_keeps_original_four_by_three_canvas(self):
        source = io.BytesIO()
        Image.new("RGB", (1600, 1200), (190, 198, 207)).save(source, "JPEG")

        result = meihua_publisher.add_channel_listing_overlay(
            source.getvalue(),
            meihua_publisher.build_cover_listing_data(self._listing()),
            with_listing_footer=True,
        )

        with Image.open(result) as rendered:
            self.assertEqual(rendered.size, (1600, 1200))
            # 安全区外的底部中央仍应保留房源图，不再画整条信息栏。
            bottom_center = rendered.getpixel((800, 1160))
            self.assertTrue(all(160 <= value <= 220 for value in bottom_center))

    def test_cover_data_only_exposes_verified_fields(self):
        listing = self._listing()
        listing.pop("size")
        listing.pop("floor")

        cover = meihua_publisher.build_cover_listing_data(listing)

        self.assertEqual(cover["size"], "")
        self.assertEqual(cover["floor"], "")

    def test_generated_cover_is_not_decorated_twice_after_album_crop(self):
        """回归真实顺序：封面生成器 → 四图首槽 16:9 → 发布准备。

        旧逻辑先生成 4:3 再裁成 16:9，并在已包含 $850 的封面上再叠
        当前 $680。新逻辑应直接生成 16:9，并在发布阶段原样透传。
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_path = root / "source.jpg"
            generated_path = root / "cover.jpg"
            Image.new("RGB", (1600, 1200), (185, 194, 205)).save(source_path, "JPEG")
            legacy_4_3 = io.BytesIO()
            Image.new("RGB", (1280, 960), (185, 194, 205)).save(legacy_4_3, "JPEG")
            legacy_slot = meihua_publisher._normalize_for_album_slot(
                legacy_4_3.getvalue(), index=0, total=4
            )
            with Image.open(io.BytesIO(legacy_slot)) as cropped_legacy:
                self.assertEqual(cropped_legacy.size, (1280, 720))

            _draw_new_cover(
                output_path=str(generated_path),
                project="SKYTREE天空树",
                layout="1房",
                area="钻石岛",
                price=850,
                size="84㎡",
                floor="14楼",
                highlights=[],
                base_image_path=str(source_path),
            )

            with Image.open(generated_path) as upstream:
                self.assertEqual(upstream.size, (1280, 720))

            album_bytes = meihua_publisher._normalize_for_album_slot(
                generated_path.read_bytes(), index=0, total=4
            )
            with Image.open(io.BytesIO(album_bytes)) as production_slot:
                self.assertEqual(production_slot.size, (1280, 720))

            current_listing = self._listing()
            current_listing["price"] = 680
            legacy_double_render = meihua_publisher.add_brand_watermark(
                album_bytes,
                meihua_publisher.build_cover_listing_data(current_listing),
                with_listing_footer=True,
            ).getvalue()
            self.assertNotEqual(legacy_double_render, album_bytes)

            fixed = meihua_publisher.prepare_channel_photo_for_publish(
                album_bytes,
                current_listing,
                is_generated_cover=True,
            )
            self.assertEqual(fixed.getvalue(), album_bytes)

    def test_long_chinese_cover_title_fits_before_price_area(self):
        canvas = Image.new("RGB", (1280, 720), "white")
        draw = ImageDraw.Draw(canvas)
        fitted, font = _fit_single_line_text(
            draw,
            "洪森大道国际高端花园社区独栋況别墅",
            max_width=760,
            start_size=48,
            min_size=38,
        )

        bbox = draw.textbbox((0, 0), fitted, font=font)
        self.assertLessEqual(bbox[2] - bbox[0], 760)
        self.assertTrue(fitted)

    def test_cover_stress_scenarios_share_final_sixteen_by_nine_canvas(self):
        scenarios = [
            ("钻石岛河景公寓", "2房2卫", 980, (36, 52, 70)),
            ("百适河精选房源", "", 650, (220, 224, 228)),
            ("森林公寓", "单间公寓", 480, (246, 244, 238)),
            ("百适河商务中心", "办公室", 1800, (24, 29, 38)),
            ("洪森大道国际高端花园社区独栋況别墅", "5房6卫独栋别墅", 3500, (76, 89, 96)),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for index, (project, layout, price, color) in enumerate(scenarios):
                source = root / f"source-{index}.jpg"
                output = root / f"cover-{index}.jpg"
                Image.new("RGB", (1600, 900), color).save(source, "JPEG")
                _draw_new_cover(
                    output_path=str(output),
                    project=project,
                    layout=layout,
                    area="",
                    price=price,
                    size="",
                    floor="",
                    highlights=[],
                    base_image_path=str(source),
                )
                with Image.open(output) as rendered:
                    self.assertEqual(rendered.size, (1280, 720))

    def test_cover_and_mobile_caption_share_the_same_price_source(self):
        listing = self._listing()
        listing["price"] = 850

        cover = meihua_publisher.build_cover_listing_data(listing)
        caption = meihua_publisher.build_chinese_listing_post(listing)

        self.assertEqual(cover["price"], 850)
        self.assertEqual(cover["project"], "SKYTREE天空树")
        self.assertEqual(cover["area"], "钻石岛")
        self.assertEqual(cover["layout"], "1房")
        self.assertIn("$850/月", caption)
        self.assertNotIn("$680/月", caption)

    def test_plus_room_layout_keeps_meaning_in_mobile_hashtag(self):
        listing = self._listing()
        listing["layout"] = "2+1房"

        caption = meihua_publisher.build_chinese_listing_post(listing)

        self.assertIn("2+1房", caption)
        self.assertNotIn("#21房", caption)


if __name__ == "__main__":
    unittest.main()
