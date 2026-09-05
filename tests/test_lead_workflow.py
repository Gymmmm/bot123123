import tempfile
import unittest
from pathlib import Path

from qiaolian_dual.db import Database
from qiaolian_dual.user_bot import admin_lead_keyboard


class LeadWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.td.name) / "workflow.db")

    def tearDown(self):
        self.td.cleanup()

    def test_advisor_can_claim_lead_and_update_appointment(self):
        lead_id = self.db.create_lead(
            {
                "user_id": 1001,
                "action": "appointment_submit",
                "source": "channel",
                "created_at": "2026-07-29 12:00:00",
            }
        )
        appointment_id = self.db.create_appointment(
            {
                "user_id": 1001,
                "listing_id": "l_100",
                "viewing_mode": "offline",
                "appointment_date": "07-30",
                "appointment_time": "pm",
                "status": "pending",
                "created_at": "2026-07-29 12:00:00",
            }
        )

        self.assertTrue(
            self.db.update_lead_workflow(
                lead_id,
                status="claimed",
                advisor_id="9001",
                advisor_name="侨联顾问",
            )
        )
        self.assertTrue(self.db.update_appointment_status(appointment_id, "assigned"))

        with self.db.connect() as conn:
            lead = conn.execute(
                "SELECT lead_status, advisor_id, advisor_name FROM leads WHERE id=?",
                (lead_id,),
            ).fetchone()
            appointment = conn.execute(
                "SELECT status FROM appointments WHERE id=?",
                (appointment_id,),
            ).fetchone()
        self.assertEqual(lead["lead_status"], "claimed")
        self.assertEqual(lead["advisor_id"], "9001")
        self.assertEqual(appointment["status"], "assigned")

    def test_admin_lead_keyboard_contains_actionable_callbacks(self):
        keyboard = admin_lead_keyboard(lead_id=8, appointment_id=9, user_id=1001, listing_id="l_1")
        callbacks = [
            button.callback_data
            for row in keyboard.inline_keyboard
            for button in row
        ]
        self.assertIn("adminlead:claim:8:9:1001", callbacks)
        self.assertIn("adminlead:contacted:8:9:1001", callbacks)
        self.assertIn("adminlead:done:8:9:1001", callbacks)
        self.assertIn("adminlead:invalid:8:9:1001", callbacks)
        self.assertIn("adminlead:view:8:9:1001", callbacks)
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks))
        self.assertTrue(all(len(row) <= 2 for row in keyboard.inline_keyboard))
        self.assertEqual([button.text for button in keyboard.inline_keyboard[0]], ["✅ 我来跟进", "📞 已联系"])
        self.assertEqual([button.text for button in keyboard.inline_keyboard[1]], ["✅ 完成", "🚫 结束跟进"])

        hidden = admin_lead_keyboard(lead_id=8, appointment_id=9, user_id=1001)
        hidden_callbacks = [
            button.callback_data
            for row in hidden.inline_keyboard
            for button in row
        ]
        self.assertNotIn("adminlead:view:8:9:1001", hidden_callbacks)
        self.assertIn("adminlead:invalid:8:9:1001", hidden_callbacks)


if __name__ == "__main__":
    unittest.main()
