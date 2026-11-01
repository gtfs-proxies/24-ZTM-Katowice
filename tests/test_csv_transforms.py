import csv
from io import StringIO
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from csv_transform import rewrite_csv


ROOT = Path(__file__).resolve().parents[1]


class CsvTransformTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        environment = patch.dict(os.environ, {"FEED_NAME": str(self.root.relative_to("/tmp"))})
        environment.start()
        self.addCleanup(environment.stop)
        self.feed = self.root / "feed"
        self.feed.mkdir()

    def run_script(self, name, check=True):
        return subprocess.run(
            [sys.executable, str(ROOT / name)], cwd=self.root,
            check=check, capture_output=True, text=True,
        )

    def write_file(self, name, content):
        path = self.feed / name
        path.write_text(content, encoding="utf-8-sig", newline="")
        return path

    def assert_csv(self, path, expected):
        output = StringIO(newline="")
        csv.writer(output).writerows(expected)
        self.assertEqual(path.read_bytes(), output.getvalue().encode("utf-8-sig"))

    def test_cleaning_preserves_csv_rules_and_removes_existing_characters(self):
        path = self.write_file(
            "stops.txt",
            'stop_id,stop_name,description\r\n'
            ' P1 ,"  A, B\u00a0\u200b\u200c\u200d\ufeff  ","line 1\r\nline 2"\r\n'
            'P2,"Quoted ""name""",\r\n',
        )

        self.run_script("clean-feed.py")

        self.assert_csv(path, [
            ["stop_id", "stop_name", "description"],
            ["P1", "A, B", "line 1\nline 2"],
            ["P2", 'Quoted "name"', ""],
        ])
        self.assertEqual(sorted(p.name for p in self.feed.iterdir()), ["stops.txt"])
        self.assertEqual([p.name for p in self.root.iterdir()], ["feed"])

    def test_rounding_preserves_precision_invalid_values_and_empty_cells(self):
        shapes = self.write_file(
            "shapes.txt", "shape_id,shape_pt_lat,shape_pt_lon,shape_dist_traveled\n"
            "A,50.12345678,18.76543219,123.456\nB,invalid,, \n",
        )
        times = self.write_file(
            "stop_times.txt", "trip_id,arrival_time,shape_dist_traveled\nT1,08:00:00,9.876\n"
        )

        self.run_script("round-shapes.py")

        self.assert_csv(shapes, [
            ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_dist_traveled"],
            ["A", "50.123457", "18.765432", "123.46"],
            ["B", "invalid", "", " "],
        ])
        self.assert_csv(times, [
            ["trip_id", "arrival_time", "shape_dist_traveled"],
            ["T1", "08:00:00", "9.88"],
        ])

    def test_rounding_without_target_columns_preserves_other_fields(self):
        path = self.write_file("stop_times.txt", "trip_id,arrival_time\nT1,08:00:00\n")

        self.run_script("round-shapes.py")

        self.assert_csv(path, [["trip_id", "arrival_time"], ["T1", "08:00:00"]])

    def test_routes_preserve_casing_rules_extra_columns_and_counts(self):
        path = self.write_file(
            "routes.txt", "route_id,route_long_name,route_desc,extra\n"
            'R1,KATOWICE - TYCHY,"Katowice - Tychy, express",untouched\n'
            "R2,ŁÓDŹ,Łódź,keep\nR3,ALL CAPS,,keep\n"
            "R4,Already Mixed,already mixed,keep\n"
            "R5,UNKNOWN - TYCHY,Via Tychy,keep\n",
        )

        result = self.run_script("fix-routes.py")

        self.assert_csv(path, [
            ["route_id", "route_long_name", "route_desc", "extra"],
            ["R1", "Katowice - Tychy", "Katowice - Tychy, express", "untouched"],
            ["R2", "Łódź", "", "keep"],
            ["R3", "ALL CAPS", "", "keep"],
            ["R4", "Already Mixed", "", "keep"],
            ["R5", "Unknown - Tychy", "Via Tychy", "keep"],
        ])
        self.assertIn("Converted 3 route names", result.stdout)
        self.assertIn("Removed 2 duplicate route descriptions", result.stdout)

    def test_blocks_clear_overlaps_but_preserve_services_and_touching_intervals(self):
        path = self.write_file(
            "trips.txt", "trip_id,service_id,block_id,extra\n"
            "T1,S1,B,keep\nT2,S1,B,keep\nT3,S1,B,keep\n"
            "T4,S2,B,keep\nT5,S1,,keep\n",
        )
        self.write_file(
            "stop_times.txt", "trip_id,arrival_time\n"
            "T1,24:00:00\nT1,24:20:00\nT2,24:10:00\nT2,24:30:00\n"
            "T3,24:30:00\nT3,25:00:00\nT4,24:05:00\nT4,24:25:00\n",
        )

        result = self.run_script("fix-blocks.py")

        self.assert_csv(path, [
            ["trip_id", "service_id", "block_id", "extra"],
            ["T1", "S1", "", "keep"], ["T2", "S1", "", "keep"],
            ["T3", "S1", "B", "keep"], ["T4", "S2", "B", "keep"],
            ["T5", "S1", "", "keep"],
        ])
        self.assertIn("Cleared block_id for 2 trips", result.stdout)

    def test_route_and_block_rewrites_preserve_header_only_files(self):
        for script, name, header in (
            ("fix-routes.py", "routes.txt", ["route_id", "route_long_name"]),
            ("fix-blocks.py", "trips.txt", ["trip_id", "service_id", "block_id"]),
        ):
            with self.subTest(script=script):
                path = self.write_file(name, ','.join(header) + '\n')
                self.write_file("stop_times.txt", "trip_id,arrival_time\n")
                self.run_script(script)
                self.assert_csv(path, [header])

    def test_failed_transformation_keeps_original_and_cleans_temporary_files(self):
        path = self.write_file("stops.txt", "stop_id\nP1\nP2\n")
        before = path.read_bytes()

        def fail_on_second_row(row):
            if row["stop_id"] == "P2":
                raise RuntimeError("Transformation failed")
            return {"stop_id": "Modified"}

        with self.assertRaisesRegex(RuntimeError, "Transformation failed"):
            rewrite_csv(path, fail_on_second_row)

        self.assertEqual(path.read_bytes(), before)
        self.assertEqual([p.name for p in self.feed.iterdir()], ["stops.txt"])
        self.assertEqual([p.name for p in self.root.iterdir()], ["feed"])

    def test_replacement_preserves_file_permissions(self):
        path = self.write_file("stops.txt", "stop_id\nP1\n")
        path.chmod(0o640)

        rewrite_csv(path, lambda row: row)

        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o640)

    def test_failed_replacement_keeps_original_and_cleans_temporary_files(self):
        path = self.write_file("stops.txt", "stop_id\nP1\n")
        before = path.read_bytes()

        with patch.object(Path, "replace", side_effect=OSError("Replacement failed")):
            with self.assertRaisesRegex(OSError, "Replacement failed"):
                rewrite_csv(path, lambda row: {"stop_id": "Modified"})

        self.assertEqual(path.read_bytes(), before)
        self.assertEqual([p.name for p in self.root.iterdir()], ["feed"])

    def test_malformed_rows_do_not_truncate_source_files(self):
        for script, name, content in (
            ("clean-feed.py", "stops.txt", "stop_id,stop_name\nP1,First\nP2,Extra,column\n"),
            ("round-shapes.py", "shapes.txt", "shape_id,shape_pt_lat\nA,50.12345678\nB,50,extra\n"),
            ("fix-routes.py", "routes.txt", "route_id,route_long_name\nR1,First\nR2,Extra,column\n"),
            ("fix-blocks.py", "trips.txt", "trip_id,service_id,block_id\nT1,S1,B\nT2,S1,B,extra\n"),
        ):
            with self.subTest(script=script):
                if script == "fix-blocks.py":
                    times = self.write_file("stop_times.txt", "trip_id,arrival_time\n")
                path = self.write_file(name, content)
                before = path.read_bytes()

                result = self.run_script(script, check=False)

                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(path.read_bytes(), before)
                expected = sorted([name, "stop_times.txt"]) if script == "fix-blocks.py" else [name]
                self.assertEqual(sorted(p.name for p in self.feed.iterdir()), expected)
                self.assertEqual([p.name for p in self.root.iterdir()], ["feed"])
                path.unlink()
                if script == "fix-blocks.py":
                    times.unlink()

    def test_header_only_file_stays_header_only(self):
        path = self.write_file("stops.txt", "stop_id,stop_name\n")

        self.run_script("clean-feed.py")

        self.assert_csv(path, [["stop_id", "stop_name"]])

    def test_empty_file_fails_without_changes_or_temporary_files(self):
        path = self.write_file("stops.txt", "")
        before = path.read_bytes()

        result = self.run_script("clean-feed.py", check=False)

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual([p.name for p in self.feed.iterdir()], ["stops.txt"])
        self.assertEqual([p.name for p in self.root.iterdir()], ["feed"])


if __name__ == "__main__":
    unittest.main()
