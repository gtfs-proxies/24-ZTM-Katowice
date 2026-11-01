from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "extract-feed-dates.py"


class ExtractFeedDatesTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.feed = self.root / "feed"
        self.feed.mkdir()

    def write_file(self, name, content):
        (self.feed / name).write_text(content, encoding="utf-8-sig")

    def run_extractor(self):
        return subprocess.run(
            [sys.executable, str(SCRIPT)],
            cwd=self.root,
            capture_output=True,
            text=True,
        )

    def assert_dates(self, start_date, end_date):
        result = self.run_extractor()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout, f"START_DATE={start_date}\nEND_DATE={end_date}\n"
        )

    def test_calendar_uses_named_columns_and_csv_quoting(self):
        self.write_file(
            "calendar.txt",
            'service_id,end_date,description,start_date\n'
            'S1,20261121,"Monday, Friday",20261101\n'
            'S2,20261115,Weekend,20261025\n',
        )

        self.assert_dates("20261025", "20261121")

    def test_exception_only_feed_uses_date_not_exception_type(self):
        self.write_file(
            "calendar_dates.txt",
            "service_id,exception_type,date\nS1,1,20261103\nS2,1,20261101\n",
        )

        self.assert_dates("20261101", "20261103")

    def test_additions_extend_calendar_but_removals_do_not(self):
        self.write_file(
            "calendar.txt", "start_date,end_date,service_id\n20261101,20261121,S1\n"
        )
        self.write_file(
            "calendar_dates.txt",
            "date,service_id,exception_type\n"
            "20261025,S2,1\n20261130,S2,1\n"
            "20260101,S1,2\n20261231,S1,2\n",
        )

        self.assert_dates("20261025", "20261130")

    def test_header_only_calendar_allows_exception_dates(self):
        self.write_file("calendar.txt", "service_id,start_date,end_date\n")
        self.write_file(
            "calendar_dates.txt", "service_id,exception_type,date\nS1,1,20261101\n"
        )

        self.assert_dates("20261101", "20261101")

    def test_missing_calendars_fail_without_outputs(self):
        result = self.run_extractor()

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("No service dates found", result.stderr)

    def test_empty_calendars_fail_without_outputs(self):
        self.write_file("calendar.txt", "service_id,start_date,end_date\n")
        self.write_file("calendar_dates.txt", "service_id,exception_type,date\n")

        result = self.run_extractor()

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("No service dates found", result.stderr)

    def test_removal_only_calendar_does_not_create_coverage(self):
        self.write_file(
            "calendar_dates.txt", "service_id,exception_type,date\nS1,2,20261101\n"
        )

        result = self.run_extractor()

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("No service dates found", result.stderr)

    def test_invalid_dates_fail_without_outputs(self):
        for value in ("20260230", "2026111", "", "not-a-date"):
            with self.subTest(value=value):
                self.write_file(
                    "calendar.txt",
                    f"service_id,start_date,end_date\nS1,20261101,{value}\n",
                )
                result = self.run_extractor()

                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertIn("invalid end_date", result.stderr)


if __name__ == "__main__":
    unittest.main()
