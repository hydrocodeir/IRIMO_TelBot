import importlib
import os
import tempfile
import unittest
from unittest import mock


class DownloadLimitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        os.environ["TELEGRAM_BOT_TOKEN"] = "123456:test-token"
        os.environ["ADMIN_ID"] = "999999999"
        os.environ["DB_PATH"] = os.path.join(cls.temp_dir.name, "limits-test.db")
        cls.app = importlib.import_module("bot_app")
        cls.app.REGIONS = ["البرز", "تهران", "قم"]

    @classmethod
    def tearDownClass(cls):
        cls.app.conn.close()
        cls.temp_dir.cleanup()

    def setUp(self):
        with self.app.DB_LOCK:
            cursor = self.app.conn.cursor()
            cursor.execute("DELETE FROM downloads")
            cursor.execute("DELETE FROM download_limit_exemptions")
            cursor.execute("DELETE FROM daily_download_overrides")
            self.app.conn.commit()

    def add_downloads(self, user_id, count, date=None):
        download_date = date or self.app._today()
        with self.app.DB_LOCK:
            self.app.conn.executemany(
                """
                INSERT INTO downloads(user_id, username, station_name, download_date)
                VALUES (?, 'tester', ?, ?)
                """,
                [(user_id, f"station-{number}", download_date) for number in range(count)]
            )
            self.app.conn.commit()

    def test_permanent_mode_has_no_limit_and_replaces_daily_mode(self):
        user_id = 101
        self.app.set_daily_station_limit(user_id, 2)
        self.assertTrue(self.app.add_download_limit_exemption(user_id))
        self.add_downloads(user_id, 20)

        self.assertTrue(self.app.check_download_access(user_id, "تهران")[0])
        self.assertIsNone(self.app.get_active_daily_override(user_id))

    def test_daily_station_limit_expires_and_returns_to_normal(self):
        user_id = 102
        self.app.set_daily_station_limit(user_id, 3)
        self.add_downloads(user_id, 2)
        self.assertTrue(self.app.check_download_access(user_id, "تهران")[0])

        self.add_downloads(user_id, 1)
        allowed, reason = self.app.check_download_access(user_id, "تهران")
        self.assertFalse(allowed)
        self.assertIn("download quota for today", reason)

        self.app._db_execute(
            "UPDATE daily_download_overrides SET override_date='2000-01-01' WHERE user_id=?",
            (user_id,)
        )
        allowed, reason = self.app.check_download_access(user_id, "تهران")
        self.assertFalse(allowed)
        self.assertIn("daily limit", reason)

    def test_one_day_override_is_inactive_on_the_next_iran_day(self):
        user_id = 105
        with mock.patch.object(self.app, "_today", return_value="2026-08-20"):
            self.app.set_daily_station_limit(user_id, 5)
            self.add_downloads(user_id, 1, date="2026-08-20")
            self.assertTrue(self.app.get_active_daily_override(user_id))

        with mock.patch.object(self.app, "_today", return_value="2026-08-21"):
            self.assertIsNone(self.app.get_active_daily_override(user_id))
            self.assertTrue(self.app.check_download_access(user_id, "تهران")[0])

    def test_user_is_notified_once_when_station_quota_is_consumed(self):
        user_id = 106
        self.app.set_daily_station_limit(user_id, 2)
        self.add_downloads(user_id, 2)

        with mock.patch.object(self.app, "send_limit_notification", return_value=True) as send:
            self.assertEqual(self.app.process_completed_override_notifications(user_id), 1)
            self.assertEqual(self.app.process_completed_override_notifications(user_id), 0)

        send.assert_called_once()
        self.assertEqual(send.call_args.args[0], user_id)
        self.assertIn("used all", send.call_args.args[1])
        self.assertIsNone(self.app.get_active_daily_override(user_id))

    def test_user_is_notified_once_after_end_of_day(self):
        user_id = 107
        with mock.patch.object(self.app, "_today", return_value="2026-08-20"):
            self.app.set_daily_region_override(user_id, ["تهران"])

        with (
            mock.patch.object(self.app, "_today", return_value="2026-08-21"),
            mock.patch.object(self.app, "send_limit_notification", return_value=True) as send,
        ):
            self.assertEqual(self.app.process_completed_override_notifications(user_id), 1)
            self.assertEqual(self.app.process_completed_override_notifications(user_id), 0)

        send.assert_called_once()
        self.assertIn("has expired", send.call_args.args[1])

    def test_region_mode_only_allows_selected_regions(self):
        user_id = 103
        self.app.add_download_limit_exemption(user_id)
        self.app.set_daily_region_override(user_id, ["تهران", "قم"])
        self.add_downloads(user_id, 12)

        self.assertFalse(self.app.is_download_limit_exempt(user_id))
        self.assertTrue(self.app.check_download_access(user_id, "تهران")[0])
        self.assertTrue(self.app.check_download_access(user_id, "قم")[0])
        allowed, reason = self.app.check_download_access(user_id, "البرز")
        self.assertFalse(allowed)
        self.assertIn("تهران", reason)
        self.assertIn("قم", reason)

        markup = self.app.build_region_menu(user_id)
        region_callbacks = {
            button.callback_data
            for row in markup.keyboard
            for button in row
            if button.callback_data and button.callback_data.startswith("region|")
        }
        self.assertEqual(region_callbacks, {"region|تهران", "region|قم"})

    def test_region_parser_accepts_persian_commas_and_normalizes_letters(self):
        resolved, invalid = self.app._resolve_regions("تهران، قم, البرز")
        self.assertEqual(resolved, ["تهران", "قم", "البرز"])
        self.assertEqual(invalid, [])

    def test_admin_can_select_multiple_regions_with_buttons(self):
        admin_user_id = int(os.environ["ADMIN_ID"])
        session_id, markup = self.app.start_limit_region_selection(admin_user_id, 108)

        region_buttons = [
            button
            for row in markup.keyboard
            for button in row
            if button.callback_data and button.callback_data.startswith("lrs|")
        ]
        self.assertEqual(len(region_buttons), len(self.app.REGIONS))

        selected = self.app.toggle_limit_region_selection(
            admin_user_id,
            session_id,
            "تهران"
        )
        self.assertEqual(selected, {"تهران"})
        state = self.app.pop_limit_region_selection(admin_user_id, session_id)
        self.assertEqual(state["target_user_id"], 108)
        self.assertEqual(state["selected_regions"], {"تهران"})

    def test_remove_clears_any_managed_mode(self):
        user_id = 104
        self.app.set_daily_region_override(user_id, ["تهران"])
        self.assertTrue(self.app.remove_download_limit_override(user_id))
        self.assertIsNone(self.app.get_active_daily_override(user_id))
        self.assertFalse(self.app.remove_download_limit_override(user_id))


if __name__ == "__main__":
    unittest.main()
