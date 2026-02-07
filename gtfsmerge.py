#!/usr/bin/env python3

import csv
import glob
import logging
import sys
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
VIRTUAL_STOP_CODE_PREFIX = "GR"
VIRTUAL_STOP_NAME_PREFIX = "granica"
TECH_STOP_NAME_MARKER = "[tech]"


def main():
    """Run the program."""
    gtfs_archive_paths: list[str] = [
        path for arg in sys.argv[1:-1] for path in glob.glob(arg)
    ]
    output_path: str = sys.argv[-1]

    if len(gtfs_archive_paths) < 1:
        raise ValueError("Missing arguments.")

    with ExitStack() as stack:
        zipfiles: list[tuple[str, zipfile.ZipFile]] = []
        for path in gtfs_archive_paths:
            zipfiles.append((path, stack.enter_context(zipfile.ZipFile(path))))

        drop_stop_ids: set[str] = set()
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

                reference: tuple[str, zipfile.ZipFile] | None = None
                for path, zf in zipfiles:
                    if gtfs_file in zf.namelist():
                        reference = (path, zf)
                        break

                if reference is None:
                    continue

                reference_path, reference_zf = reference

                headers: list[list[str]] = []
                archives = [(reference_path, reference_zf)] + [
                    pair for pair in zipfiles if pair[0] != reference_path
                ]
                for archive_path, archive_zf in archives:
                    if gtfs_file not in archive_zf.namelist():
                        continue
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
                        if gtfs_file not in archive_zf.namelist():
                            logging.info("\tSkipping missing %s in %s", gtfs_file, archive_path)
                            continue

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


if __name__ == "__main__":
    main()
