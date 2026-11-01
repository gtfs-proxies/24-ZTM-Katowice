import csv
from io import TextIOWrapper
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile


SCRIPT = Path(__file__).resolve().parents[1] / "gtfsmerge.py"


class GtfsMergeTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)

    def archive(self, name, files):
        path = self.root / name
        with zipfile.ZipFile(path, "w") as archive:
            for filename, content in files.items():
                archive.writestr(filename, content)
        return path

    def merge(self, *inputs):
        output = self.root / "merged.zip"
        subprocess.run([sys.executable, str(SCRIPT), *map(str, inputs), str(output)], check=True)
        with zipfile.ZipFile(output) as archive:
            result = {}
            for name in archive.namelist():
                with archive.open(name) as raw:
                    result[name] = list(csv.DictReader(TextIOWrapper(raw, encoding="utf-8-sig")))
            return result

    def test_first_archive_wins_and_cache_only_records_survive(self):
        fresh = self.archive("fresh.zip", {
            "routes.txt": "route_id,route_long_name\nR1,Corrected\n",
        })
        cached = self.archive("cached.zip", {
            "routes.txt": "route_id,route_long_name\nR1,Old\nR2,Historical\n",
        })

        rows = self.merge(fresh, cached)["routes.txt"]

        self.assertEqual(rows, [
            {"route_id": "R1", "route_long_name": "Corrected"},
            {"route_id": "R2", "route_long_name": "Historical"},
        ])

    def test_replaces_whole_trip_timeline_and_optional_frequencies(self):
        fresh = self.archive("fresh.zip", {
            "trips.txt": "trip_id,service_id\nT1,S1\n",
            "stop_times.txt": "trip_id,stop_sequence,arrival_time\nT1,1,09:00:00\n",
        })
        cached = self.archive("cached.zip", {
            "trips.txt": "trip_id,service_id\nT1,S1\nT2,S2\n",
            "stop_times.txt": "trip_id,stop_sequence,arrival_time\n"
            "T1,1,08:00:00\nT1,2,08:10:00\nT2,1,10:00:00\n",
            "frequencies.txt": "trip_id,start_time,end_time,headway_secs\n"
            "T1,08:00:00,09:00:00,600\nT2,10:00:00,11:00:00,600\n",
        })

        result = self.merge(fresh, cached)

        self.assertEqual(result["stop_times.txt"], [
            {"trip_id": "T1", "stop_sequence": "1", "arrival_time": "09:00:00"},
            {"trip_id": "T2", "stop_sequence": "1", "arrival_time": "10:00:00"},
        ])
        self.assertEqual([row["trip_id"] for row in result["frequencies.txt"]], ["T2"])

    def test_replaces_whole_shape_not_just_matching_sequences(self):
        fresh = self.archive("fresh.zip", {
            "shapes.txt": "shape_id,shape_pt_sequence,shape_pt_lat\nA,1,50.1\nA,2,50.2\n",
        })
        cached = self.archive("cached.zip", {
            "shapes.txt": "shape_id,shape_pt_sequence,shape_pt_lat\n"
            "A,1,49.1\nA,2,49.2\nA,3,49.3\nB,1,51.0\n",
        })

        rows = self.merge(fresh, cached)["shapes.txt"]

        self.assertEqual([(r["shape_id"], r["shape_pt_sequence"]) for r in rows],
                         [("A", "1"), ("A", "2"), ("B", "1")])
        self.assertEqual(rows[0]["shape_pt_lat"], "50.1")

    def test_stale_technical_stop_does_not_override_corrected_stop(self):
        fresh = self.archive("fresh.zip", {
            "stops.txt": "stop_id,stop_name\nP1,Public stop\n",
            "stop_times.txt": "trip_id,stop_sequence,stop_id\nT1,1,P1\n",
        })
        cached = self.archive("cached.zip", {
            "stops.txt": "stop_id,stop_name\nP1,Old [tech]\n",
        })

        result = self.merge(fresh, cached)

        self.assertEqual(result["stops.txt"][0]["stop_name"], "Public stop")
        self.assertEqual(result["stop_times.txt"][0]["stop_id"], "P1")

    def test_filtered_fresh_timeline_does_not_resurrect_cached_rows(self):
        fresh = self.archive("fresh.zip", {
            "stops.txt": "stop_id,stop_name\nP1,Depot [tech]\nP2,Public stop\n",
            "stop_times.txt": "trip_id,stop_sequence,stop_id\nT1,1,P1\n",
        })
        cached = self.archive("cached.zip", {
            "stop_times.txt": "trip_id,stop_sequence,stop_id\nT1,1,P2\nT1,2,P2\n",
        })

        self.assertEqual(self.merge(fresh, cached)["stop_times.txt"], [])

    def test_keeps_distinct_calendar_ranges_for_shared_service_ids(self):
        fresh = self.archive("fresh.zip", {
            "calendar.txt": "service_id,start_date,end_date\nS1,20261112,20261121\n",
        })
        cached = self.archive("cached.zip", {
            "calendar.txt": "service_id,start_date,end_date\nS1,20261111,20261111\n",
        })

        rows = self.merge(fresh, cached)["calendar.txt"]

        self.assertEqual([row["start_date"] for row in rows], ["20261112", "20261111"])


if __name__ == "__main__":
    unittest.main()
