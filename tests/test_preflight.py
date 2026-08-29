import tempfile
import unittest
from pathlib import Path

from qiaolian_dual.preflight import validate_environment


VALID_TOKEN = "123456789:" + "abcdefghijklmnopqrstuvwxyz_ABCDE"


class PreflightTests(unittest.TestCase):
    def _env(self, text: str) -> tuple[tempfile.TemporaryDirectory, Path]:
        td = tempfile.TemporaryDirectory()
        path = Path(td.name) / ".env"
        path.write_text(text, encoding="utf-8")
        return td, path

    def test_missing_env_has_actionable_error(self):
        result = validate_environment(Path("/definitely/missing/.env"))
        self.assertFalse(result.ok)
        self.assertIn("缺少 .env", result.errors[0])

    def test_user_configuration_passes(self):
        td, path = self._env(
            f"USER_BOT_TOKEN={VALID_TOKEN}\n"
            "USER_BOT_USERNAME=QiaolianHouseBot\n"
            "ADVISOR_TG=@advisor\n"
        )
        try:
            result = validate_environment(path)
        finally:
            td.cleanup()
        self.assertTrue(result.ok)

    def test_publisher_requires_admin_and_channel(self):
        td, path = self._env(
            f"USER_BOT_TOKEN={VALID_TOKEN}\n"
            f"PUBLISHER_BOT_TOKEN={VALID_TOKEN}\n"
            "USER_BOT_USERNAME=QiaolianHouseBot\n"
        )
        try:
            result = validate_environment(path, with_publisher=True)
        finally:
            td.cleanup()
        self.assertIn("ADMIN_IDS 未配置或包含非数字 ID", result.errors)
        self.assertIn("CHANNEL_ID 未配置", result.errors)

    def test_collector_requires_telethon_credentials_and_sources(self):
        td, path = self._env(
            f"USER_BOT_TOKEN={VALID_TOKEN}\n"
            "USER_BOT_USERNAME=QiaolianHouseBot\n"
        )
        try:
            result = validate_environment(path, with_collector=True)
        finally:
            td.cleanup()
        self.assertIn("TG_API_ID 未配置或不是数字", result.errors)
        self.assertIn("TG_API_HASH 未配置", result.errors)
        self.assertTrue(any("采集源配置不存在" in item for item in result.errors))


if __name__ == "__main__":
    unittest.main()
