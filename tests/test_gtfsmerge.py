import csv
import datetime as dt
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

    def merge(self, *inputs, keep_past_days=None):
        output = self.root / "merged.zip"
        options = [] if keep_past_days is None else ["--keep-past-days", str(keep_past_days)]
        subprocess.run(
            [sys.executable, str(SCRIPT), *options, *map(str, inputs), str(output)],
            check=True,
            capture_output=True,
            text=True,
        )
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

    def test_optional_table_preserves_priority_and_unions_headers(self):
        newest = self.archive("newest.zip", {"agency.txt": "agency_id\nA\n"})
        older = self.archive("older.zip", {
            "routes.txt": "route_id,route_long_name\nR1,Preferred\n",
        })
        cached = self.archive("cached.zip", {
            "routes.txt": "route_id,route_desc,route_long_name\n"
            "R1,Stale,Old\nR2,Cache only,Historical\n",
        })

        rows = self.merge(newest, older, cached)["routes.txt"]

        self.assertEqual(list(rows[0]), ["route_id", "route_long_name", "route_desc"])
        self.assertEqual(rows, [
            {"route_id": "R1", "route_long_name": "Preferred", "route_desc": ""},
            {"route_id": "R2", "route_long_name": "Historical", "route_desc": "Cache only"},
        ])

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

    def test_pruning_removes_expired_dependencies_and_unused_shapes(self):
        expired = (dt.date.today() - dt.timedelta(days=8)).strftime("%Y%m%d")
        cutoff = (dt.date.today() - dt.timedelta(days=7)).strftime("%Y%m%d")
        source = self.archive("source.zip", {
            "calendar.txt": f"service_id,end_date\nold,{expired}\nkeep,{cutoff}\n",
            "calendar_dates.txt": f"service_id,date,exception_type\nold,{expired},1\n",
            "trips.txt": "trip_id,service_id,shape_id\nT1,old,A\nT2,keep,B\n",
            "stop_times.txt": "trip_id,stop_sequence\nT1,1\nT2,1\n",
            "shapes.txt": "shape_id,shape_pt_sequence\nA,1\nB,1\nC,1\n",
            "frequencies.txt": "trip_id,start_time\nT1,08:00:00\nT2,09:00:00\n",
            "trips_ext.txt": "trip_id,operator_id\nT1,O1\nT2,O2\n",
            "trip_order_ext.txt": "trip_id,order\nT1,1\nT2,1\n",
            "service_ext.txt": "service_id,name\nold,Expired\nkeep,Retained\n",
            "agency.txt": "agency_id,agency_name\nGZM,Unchanged\n",
        })

        result = self.merge(source, keep_past_days=7)

        self.assertEqual(result["calendar.txt"], [{"service_id": "keep", "end_date": cutoff}])
        self.assertEqual(result["calendar_dates.txt"], [])
        for name in ("trips.txt", "stop_times.txt", "frequencies.txt",
                     "trips_ext.txt", "trip_order_ext.txt"):
            self.assertEqual([row["trip_id"] for row in result[name]], ["T2"], name)
        self.assertEqual(result["shapes.txt"], [{"shape_id": "B", "shape_pt_sequence": "1"}])
        self.assertEqual(result["service_ext.txt"], [{"service_id": "keep", "name": "Retained"}])
        self.assertEqual(result["agency.txt"], [{"agency_id": "GZM", "agency_name": "Unchanged"}])

    def test_pruning_preserves_services_defined_only_by_exceptions(self):
        today = dt.date.today().strftime("%Y%m%d")
        source = self.archive("source.zip", {
            "calendar_dates.txt": f"service_id,date,exception_type\nS1,{today},1\n",
            "trips.txt": "trip_id,service_id,shape_id\nT1,S1,A\n",
            "stop_times.txt": "trip_id,stop_sequence\nT1,1\n",
            "shapes.txt": "shape_id,shape_pt_sequence\nA,1\nunused,1\n",
        })

        result = self.merge(source, keep_past_days=7)

        self.assertEqual([row["trip_id"] for row in result["trips.txt"]], ["T1"])
        self.assertEqual([row["shape_id"] for row in result["shapes.txt"]], ["A"])
        self.assertNotIn("calendar.txt", result)

    def test_pruning_all_trips_safeguard_keeps_every_table(self):
        expired = (dt.date.today() - dt.timedelta(days=30)).strftime("%Y%m%d")
        source = self.archive("source.zip", {
            "calendar.txt": f"service_id,end_date\nS1,{expired}\n",
            "calendar_dates.txt": f"service_id,date,exception_type\nS1,{expired},1\n",
            "trips.txt": "trip_id,service_id,shape_id\nT1,S1,A\n",
            "stop_times.txt": "trip_id,stop_sequence\nT1,1\n",
            "shapes.txt": "shape_id,shape_pt_sequence\nA,1\nunused,1\n",
        })

        self.assertEqual(self.merge(source, keep_past_days=7), self.merge(source))

    def test_pruning_is_opt_in_and_invalid_dates_are_preserved(self):
        expired = (dt.date.today() - dt.timedelta(days=30)).strftime("%Y%m%d")
        source = self.archive("source.zip", {
            "calendar.txt": f"service_id,end_date\nold,{expired}\nunknown,invalid\n",
            "trips.txt": "trip_id,service_id\nT1,old\nT2,unknown\n",
            "stop_times.txt": "trip_id,stop_sequence\nT1,1\nT2,1\n",
        })

        unpruned = self.merge(source)
        pruned = self.merge(source, keep_past_days=7)

        self.assertEqual(len(unpruned["trips.txt"]), 2)
        self.assertEqual([row["trip_id"] for row in pruned["trips.txt"]], ["T2"])
        self.assertEqual(pruned["calendar.txt"][0]["end_date"], "invalid")

    def test_pruning_keeps_trips_when_service_id_has_a_retained_range(self):
        expired = (dt.date.today() - dt.timedelta(days=30)).strftime("%Y%m%d")
        today = dt.date.today().strftime("%Y%m%d")
        source = self.archive("source.zip", {
            "calendar.txt": "service_id,start_date,end_date\n"
            f"S1,{expired},{expired}\nS1,{today},{today}\n",
            "trips.txt": "trip_id,service_id\nT1,S1\nT2,S1\n",
        })

        result = self.merge(source, keep_past_days=7)

        self.assertEqual(len(result["calendar.txt"]), 1)
        self.assertEqual([row["trip_id"] for row in result["trips.txt"]], ["T1", "T2"])


if __name__ == "__main__":
    unittest.main()
