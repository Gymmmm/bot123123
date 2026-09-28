import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import collector_bot
from v2.qiaolian_publisher_v2.bot import Draft, PublisherBot


class IntakeRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_collector_single_photo_keeps_caption(self):
        message = SimpleNamespace(
            text="BKK1 一房出租，租金 $680/月",
            message="BKK1 一房出租，租金 $680/月",
            media=object(),
            id=88,
            grouped_id=None,
        )
        event = SimpleNamespace(message=message, client=object(), chat_id=-100123)
        source = {"source_name": "test", "source_type": "telegram_channel"}

        async def append_photo(_client, _message, raw_images, _raw_videos):
            raw_images.append({"local_path": "/tmp/test-photo.jpg"})
            return "photo"

        with patch.object(
            collector_bot,
            "_append_image_or_video",
            new=AsyncMock(side_effect=append_photo),
        ), patch.object(
            collector_bot,
            "persist_source_post",
            new=AsyncMock(return_value={"status": "inserted"}),
        ) as persist:
            await collector_bot.handle_single_message(event, source)

        self.assertEqual(persist.await_args.kwargs["raw_text"], message.message)

    async def test_management_album_keeps_all_four_file_ids(self):
        draft = Draft(listing_id="l_test")
        bot = object.__new__(PublisherBot)
        bot._merge_caption_into_draft = lambda target, caption: None
        messages = []
        for i, (width, height) in enumerate(((800, 600), (1600, 900), (1200, 800), (900, 1200)), start=1):
            photo = SimpleNamespace(file_id=f"photo-{i}", width=width, height=height)
            messages.append(
                SimpleNamespace(
                    message_id=i,
                    photo=[photo],
                    caption="洪森大道两房 $680/月" if i == 1 else "",
                    text="",
                )
            )

        best = bot._store_album_messages(draft, list(reversed(messages)))

        self.assertEqual(draft.media_file_ids, ["photo-1", "photo-2", "photo-3", "photo-4"])
        self.assertEqual(draft.media_file_id, "photo-2")
        self.assertEqual(best.message_id, 2)
        self.assertEqual(draft.to_dict(1)["media_file_ids"], draft.media_file_ids)


if __name__ == "__main__":
    unittest.main()
