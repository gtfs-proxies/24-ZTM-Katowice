#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import glob
import logging
import zipfile
from contextlib import ExitStack
from io import TextIOWrapper

FILE_INDEXES: dict[str, set[str]] = {
    "agency.txt": {"agency_id"},
    "calendar.txt": {"service_id", "start_date", "end_date"},
    "calendar_dates.txt": {"service_id", "date"},
    "fare_attributes.txt": {"fare_id"},
    "fare_rules.txt": {"fare_id"},
    "feed_info.txt": {"feed_publisher_name"},
    "frequencies.txt": {"trip_id", "start_time"},
    "routes.txt": {"route_id"},
    "shapes.txt": {"shape_id", "shape_pt_sequence"},
    "stop_times.txt": {"trip_id", "stop_sequence"},
    "stops.txt": {"stop_id"},
    "trips.txt": {"trip_id"},
}


logging.basicConfig(
    format="[%(asctime)s] [%(levelname)8s] --- %(message)s",
    level=logging.ERROR,
)

ENCODING = "utf-8-sig"

DROP_COLUMNS: dict[str, set[str]] = {
    "stop_times.txt": {"timepoint", "shape_dist_traveled"},
}
GROUP_COLUMNS: dict[str, str] = {
    "stop_times.txt": "trip_id",
    "frequencies.txt": "trip_id",
    "shapes.txt": "shape_id",
}
VIRTUAL_STOP_CODE_PREFIX = "GR"
VIRTUAL_STOP_NAME_PREFIX = "granica"
TECH_STOP_NAME_MARKER = "[tech]"


def is_recent(value: str, cutoff: dt.date) -> bool:
    try:
        return dt.datetime.strptime(value.strip(), "%Y%m%d").date() >= cutoff
    except ValueError:
        # Preserve the existing pruner's conservative handling of invalid dates.
        return True


def main():
    """Merge inputs in priority order: the first archive defining an ID wins."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument(
        "--keep-past-days", type=int,
        help="prune expired services and unused shapes while writing the merged archive",
    )
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("output")
    args = parser.parse_args()
    if args.keep_past_days is not None and args.keep_past_days < 0:
        parser.error("--keep-past-days must not be negative")
    cutoff = (
        dt.date.today() - dt.timedelta(days=args.keep_past_days)
        if args.keep_past_days is not None else None
    )
    gtfs_archive_paths: list[str] = [
        path for arg in args.inputs for path in sorted(glob.glob(arg))
    ]
    output_path: str = args.output

    if len(gtfs_archive_paths) < 1:
        raise ValueError("Missing arguments.")

    with ExitStack() as stack:
        zipfiles: list[tuple[str, zipfile.ZipFile]] = []
        for path in gtfs_archive_paths:
            zipfiles.append((path, stack.enter_context(zipfile.ZipFile(path))))

        service_ids: set[str] = set()
        if cutoff is not None:
            # Reused service IDs remain active if any retained range uses them.
            for _, archive_zf in zipfiles:
                for filename, date_column in (
                    ("calendar.txt", "end_date"), ("calendar_dates.txt", "date")
                ):
                    if filename not in archive_zf.namelist():
                        continue
                    with archive_zf.open(filename) as in_raw:
                        reader = csv.DictReader(
                            TextIOWrapper(in_raw, encoding=ENCODING, newline="")
                        )
                        for row in reader:
                            if is_recent(row.get(date_column) or "", cutoff):
                                service_ids.add(row["service_id"])

        # Keep dependent trip rows in the same archive as the winning trip.
        trip_owners: dict[str, str] = {}
        trip_ids: set[str] = set()
        shape_ids: set[str] = set()
        for archive_path, archive_zf in zipfiles:
            if "trips.txt" in archive_zf.namelist():
                with archive_zf.open("trips.txt") as in_raw:
                    reader = csv.DictReader(
                        TextIOWrapper(in_raw, encoding=ENCODING, newline="")
                    )
                    for row in reader:
                        trip_id = row["trip_id"]
                        if trip_id in trip_owners:
                            continue
                        trip_owners[trip_id] = archive_path
                        if cutoff is not None and row["service_id"] in service_ids:
                            trip_ids.add(trip_id)
                            shape_ids.add(row.get("shape_id") or "")

        if cutoff is not None and trip_owners and not trip_ids:
            print(f"Skip pruning by cutoff {cutoff:%Y%m%d}: would remove all trips.")
            cutoff = None
        retained_ids = {
            "trips.txt": ("trip_id", trip_ids),
            "stop_times.txt": ("trip_id", trip_ids),
            "frequencies.txt": ("trip_id", trip_ids),
            "trips_ext.txt": ("trip_id", trip_ids),
            "trip_order_ext.txt": ("trip_id", trip_ids),
            "shapes.txt": ("shape_id", shape_ids),
            "service_ext.txt": ("service_id", service_ids),
        }

        drop_stop_ids: set[str] = set()
        seen_stop_ids: set[str] = set()
        for archive_path, archive_zf in zipfiles:
            if "stops.txt" not in archive_zf.namelist():
                continue
            try:
                with archive_zf.open("stops.txt") as in_raw:
                    in_wrapper = TextIOWrapper(in_raw, encoding=ENCODING, newline="")
                    reader = csv.DictReader(in_wrapper)
                    if not reader.fieldnames:
                        continue
                    for row in reader:
                        if not row:
                            continue
                        stop_code = (row.get("stop_code") or "").strip()
                        stop_name = (row.get("stop_name") or "").strip().lower()
                        stop_id = (row.get("stop_id") or "").strip()
                        if stop_id in seen_stop_ids:
                            continue
                        seen_stop_ids.add(stop_id)
                        
                        # Drop virtual border stops
                        if stop_code.startswith(VIRTUAL_STOP_CODE_PREFIX) and stop_name.startswith(
                            VIRTUAL_STOP_NAME_PREFIX
                        ):
                            if stop_id:
                                drop_stop_ids.add(stop_id)
                                logging.info(
                                    "Marking virtual stop for removal: %s (%s)",
                                    stop_id,
                                    stop_code,
                                )
                        
                        # Drop technical stops
                        elif TECH_STOP_NAME_MARKER in stop_name:
                            if stop_id:
                                drop_stop_ids.add(stop_id)
                                logging.info(
                                    "Marking technical stop for removal: %s (%s)",
                                    stop_id,
                                    stop_name,
                                )
            except KeyError:
                continue

        all_files: set[str] = set()
        for _, zf in zipfiles:
            all_files.update(
                name for name in zf.namelist() if name.endswith(".txt")
            )

        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as result:
            for gtfs_file in sorted(all_files):
                logging.info("Processing %s...", gtfs_file)

                # Filtering preserves input priority, including optional tables
                # found only in older archives or the cached master.
                archives = [
                    (path, zf) for path, zf in zipfiles
                    if gtfs_file in zf.namelist()
                ]
                headers: list[list[str]] = []
                for archive_path, archive_zf in archives:
                    try:
                        with archive_zf.open(gtfs_file) as in_raw:
                            in_wrapper = TextIOWrapper(
                                in_raw, encoding=ENCODING, newline=""
                            )
                            reader = csv.DictReader(in_wrapper)
                            archive_header = list(reader.fieldnames or [])
                            if archive_header:
                                drop = DROP_COLUMNS.get(gtfs_file, set())
                                headers.append(
                                    [
                                        col
                                        for col in archive_header
                                        if col not in drop
                                    ]
                                )
                    except KeyError:
                        continue

                if not headers:
                    logging.error("\tSkipping %s (empty header).", gtfs_file)
                    continue

                header: list[str] = []
                for archive_header in headers:
                    for col in archive_header:
                        if col not in header:
                            header.append(col)

                if not header:
                    logging.error("\tSkipping %s (empty header after drop).", gtfs_file)
                    continue

                with result.open(gtfs_file, "w") as out_raw:
                    out_wrapper = TextIOWrapper(out_raw, encoding=ENCODING, newline="")
                    writer = csv.DictWriter(out_wrapper, fieldnames=header)
                    writer.writeheader()

                    seen_ids: set[tuple[str, ...]] = set()
                    group_column = GROUP_COLUMNS.get(gtfs_file)
                    group_owners: dict[str, str] = {}
                    retention = retained_ids.get(gtfs_file) if cutoff is not None else None
                    date_column = {
                        "calendar.txt": "end_date",
                        "calendar_dates.txt": "date",
                    }.get(gtfs_file)
                    pruned_rows = 0

                    if gtfs_file not in FILE_INDEXES:
                        logging.warning("\t\tUsing first column as index.")
                        index_positions = [header[0]]
                    else:
                        missing_index = [
                            index
                            for index in sorted(FILE_INDEXES[gtfs_file])
                            if index not in header
                        ]
                        if missing_index:
                            logging.warning(
                                "\t\tMissing index columns in %s, using first column.",
                                gtfs_file,
                            )
                            index_positions = [header[0]]
                        else:
                            index_positions = sorted(FILE_INDEXES[gtfs_file])

                    for archive_path, archive_zf in archives:
                        try:
                            with archive_zf.open(gtfs_file) as in_raw:
                                in_wrapper = TextIOWrapper(
                                    in_raw, encoding=ENCODING, newline=""
                                )
                                reader = csv.DictReader(in_wrapper)
                                archive_header = list(reader.fieldnames or [])
                                if not archive_header:
                                    logging.error(
                                        "\tSkipping %s from %s (empty header).",
                                        gtfs_file,
                                        archive_path,
                                    )
                                    continue

                                for row in reader:
                                    if not row:
                                        continue
                                    if group_column:
                                        group_id = row[group_column]
                                        owner = group_owners.setdefault(group_id, archive_path)
                                        if group_column == "trip_id":
                                            owner = trip_owners.get(group_id, owner)
                                        if owner != archive_path:
                                            continue
                                    if cutoff is not None:
                                        if date_column and not is_recent(
                                            row.get(date_column) or "", cutoff
                                        ):
                                            pruned_rows += 1
                                            continue
                                        if retention and row.get(retention[0]) not in retention[1]:
                                            pruned_rows += 1
                                            continue
                                    if "stop_id" in header and drop_stop_ids:
                                        stop_id = (row.get("stop_id") or "").strip()
                                        if stop_id in drop_stop_ids:
                                            logging.info(
                                                "\t\tDropping row with virtual stop %s in %s",
                                                stop_id,
                                                gtfs_file,
                                            )
                                            continue
                                    filtered_row = {k: row.get(k, "") for k in header}
                                    index_tuple = tuple(
                                        filtered_row.get(col, "")
                                        for col in index_positions
                                    )
                                    if index_tuple not in seen_ids:
                                        writer.writerow(filtered_row)
                                        seen_ids.add(index_tuple)
                                    else:
                                        logging.info(
                                            "\t\tAvoiding row with duplicate IDs: %s",
                                            index_tuple,
                                        )
                        except KeyError:
                            logging.info("\tSkipping missing %s in %s", gtfs_file, archive_path)
                            continue

                    out_wrapper.flush()
                    if pruned_rows:
                        print(f"Pruned {pruned_rows} expired/unreferenced rows from {gtfs_file}")


if __name__ == "__main__":
    main()
