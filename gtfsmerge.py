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

        all_files: set[str] = set()
        for _, zf in zipfiles:
            all_files.update(zf.namelist())

        with zipfile.ZipFile(output_path, "w") as result:
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
                with result.open(gtfs_file, "w") as out_raw:
                    out_wrapper = TextIOWrapper(out_raw, encoding=ENCODING, newline="")
                    writer = csv.writer(out_wrapper)

                    try:
                        with reference_zf.open(gtfs_file) as ref_raw:
                            ref_wrapper = TextIOWrapper(
                                ref_raw, encoding=ENCODING, newline=""
                            )
                            reference_reader = csv.reader(ref_wrapper)
                            header = next(reference_reader)
                    except KeyError:
                        continue

                    writer.writerow(header)

                    seen_rows: set[tuple[str, ...]] = set()
                    seen_ids: set[tuple[str, ...]] = set()

                    if gtfs_file not in FILE_INDEXES:
                        logging.warning("\t\tUsing first column as index.")
                        index_positions = [0]
                    else:
                        try:
                            index_positions = [
                                header.index(index)
                                for index in sorted(FILE_INDEXES[gtfs_file])
                            ]
                        except ValueError:
                            logging.warning(
                                "\t\tMissing index column in %s, using first column.",
                                gtfs_file,
                            )
                            index_positions = [0]

                    # process reference first, then the rest
                    archives = [(reference_path, reference_zf)] + [
                        pair for pair in zipfiles if pair[0] != reference_path
                    ]

                    for archive_path, archive_zf in archives:
                        if gtfs_file not in archive_zf.namelist():
                            logging.info("\tSkipping missing %s in %s", gtfs_file, archive_path)
                            continue

                        try:
                            with archive_zf.open(gtfs_file) as in_raw:
                                in_wrapper = TextIOWrapper(
                                    in_raw, encoding=ENCODING, newline=""
                                )
                                reader = csv.reader(in_wrapper)
                                archive_header = next(reader)

                                if archive_header != header:
                                    logging.error(
                                        "\tSkipping %s from %s (header mismatch).",
                                        gtfs_file,
                                        archive_path,
                                    )
                                    continue

                                for row in reader:
                                    index_tuple = tuple(row[i] for i in index_positions)
                                    if index_tuple not in seen_ids:
                                        writer.writerow(row)
                                        seen_ids.add(index_tuple)
                                        seen_rows.add(tuple(row))
                                    else:
                                        if tuple(row) in seen_rows:
                                            logging.debug(
                                                "\t\tAvoiding exact row duplicate: %s",
                                                row,
                                            )
                                        else:
                                            logging.info(
                                                "\t\tAvoiding row with duplicate IDs: %s",
                                                row,
                                            )
                        except KeyError:
                            logging.info("\tSkipping missing %s in %s", gtfs_file, archive_path)
                            continue


if __name__ == "__main__":
    main()
