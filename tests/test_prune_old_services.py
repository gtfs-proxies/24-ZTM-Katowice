import csv
import datetime as dt
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "prune-old-services.py"


class PruneOldServicesTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.feed = self.root / "feed"
        self.feed.mkdir()
        self.expired = (dt.date.today() - dt.timedelta(days=30)).strftime("%Y%m%d")
        self.cutoff = (dt.date.today() - dt.timedelta(days=7)).strftime("%Y%m%d")

    def write_file(self, name, content):
        (self.feed / name).write_text(content, encoding="utf-8")

    def read_rows(self, name):
        with (self.feed / name).open(encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))

    def run_pruner(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT)],
            cwd=self.root,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout

    def write_trips(self):
        self.write_file("trips.txt", "service_id,trip_id\nold,T1\nkeep,T2\n")
        self.write_file(
            "stop_times.txt", "trip_id,stop_id,stop_sequence\nT1,P1,1\nT2,P1,1\n"
        )

    def test_skip_preserves_every_file_byte_for_byte(self):
        self.write_file("calendar.txt", f"service_id,end_date\nold,{self.expired}\n")
        self.write_file(
            "calendar_dates.txt",
            f"service_id,exception_type,date\nkeep,1,{self.expired}\n",
        )
        self.write_trips()
        before = {path.name: path.read_bytes() for path in self.feed.iterdir()}

        self.assertIn("would remove all trips", self.run_pruner())

        after = {path.name: path.read_bytes() for path in self.feed.iterdir()}
        self.assertEqual(before, after)

    def test_prunes_expired_rows_and_keeps_cutoff_date(self):
        self.write_file(
            "calendar.txt",
            f"service_id,end_date\nold,{self.expired}\nkeep,{self.cutoff}\n",
        )
        self.write_file(
            "calendar_dates.txt",
            f"service_id,exception_type,date\nold,1,{self.expired}\nkeep,1,{self.cutoff}\n",
        )
        self.write_trips()

        self.assertIn("Pruned by cutoff", self.run_pruner())

        for name in ("calendar.txt", "calendar_dates.txt", "trips.txt"):
            self.assertEqual([row["service_id"] for row in self.read_rows(name)], ["keep"])
        self.assertEqual([row["trip_id"] for row in self.read_rows("stop_times.txt")], ["T2"])

    def test_supports_calendar_dates_without_calendar(self):
        self.write_file(
            "calendar_dates.txt",
            f"service_id,exception_type,date\nold,1,{self.expired}\nkeep,1,{self.cutoff}\n",
        )
        self.write_trips()

        self.run_pruner()

        self.assertFalse((self.feed / "calendar.txt").exists())
        self.assertEqual(
            [row["service_id"] for row in self.read_rows("calendar_dates.txt")], ["keep"]
        )
        self.assertEqual([row["trip_id"] for row in self.read_rows("trips.txt")], ["T2"])
        self.assertEqual([row["trip_id"] for row in self.read_rows("stop_times.txt")], ["T2"])


if __name__ == "__main__":
    unittest.main()
