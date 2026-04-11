#!/usr/bin/env python3

import csv
import datetime as dt
from pathlib import Path

FEED_DIR = Path("feed")
ENCODING = "utf-8-sig"
KEEP_PAST_DAYS = 7
DATE_FMT = "%Y%m%d"


def load_csv(path: Path):
    if not path.exists():
        return [], []
    with open(path, encoding=ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return reader.fieldnames or [], rows


def save_csv(path: Path, headers, rows):
    if not headers:
        return
    with open(path, "w", encoding=ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def parse_gtfs_date(value: str):
    try:
        return dt.datetime.strptime((value or "").strip(), DATE_FMT).date()
    except ValueError:
        return None


def main():
    cutoff = dt.date.today() - dt.timedelta(days=KEEP_PAST_DAYS)

    calendar_path = FEED_DIR / "calendar.txt"
    calendar_headers, calendar_rows = load_csv(calendar_path)
    kept_calendar = []
    service_ids = set()
    for row in calendar_rows:
        end_date = parse_gtfs_date(row.get("end_date", ""))
        if end_date is None or end_date >= cutoff:
            kept_calendar.append(row)
            service_id = (row.get("service_id") or "").strip()
            if service_id:
                service_ids.add(service_id)
    if calendar_rows:
        save_csv(calendar_path, calendar_headers, kept_calendar)

    calendar_dates_path = FEED_DIR / "calendar_dates.txt"
    cald_headers, cald_rows = load_csv(calendar_dates_path)
    kept_calendar_dates = []
    for row in cald_rows:
        service_date = parse_gtfs_date(row.get("date", ""))
        if service_date is None or service_date >= cutoff:
            kept_calendar_dates.append(row)
            service_id = (row.get("service_id") or "").strip()
            if service_id:
                service_ids.add(service_id)
    if cald_rows:
        save_csv(calendar_dates_path, cald_headers, kept_calendar_dates)

    trips_path = FEED_DIR / "trips.txt"
    trips_headers, trips_rows = load_csv(trips_path)
    kept_trips = []
    trip_ids = set()
    for row in trips_rows:
        service_id = (row.get("service_id") or "").strip()
        if service_id in service_ids:
            kept_trips.append(row)
            trip_id = (row.get("trip_id") or "").strip()
            if trip_id:
                trip_ids.add(trip_id)
    if trips_rows and not kept_trips:
        print(
            f"Skip pruning by cutoff {cutoff:%Y%m%d}: would remove all trips "
            f"({len(trips_rows)} rows)."
        )
        return

    if trips_rows:
        save_csv(trips_path, trips_headers, kept_trips)

    stop_times_path = FEED_DIR / "stop_times.txt"
    stop_headers, stop_rows = load_csv(stop_times_path)
    kept_stop_times = []
    for row in stop_rows:
        trip_id = (row.get("trip_id") or "").strip()
        if trip_id in trip_ids:
            kept_stop_times.append(row)
    if stop_rows:
        save_csv(stop_times_path, stop_headers, kept_stop_times)

    print(
        f"Pruned by cutoff {cutoff:%Y%m%d}: "
        f"calendar {len(calendar_rows)}->{len(kept_calendar)}, "
        f"calendar_dates {len(cald_rows)}->{len(kept_calendar_dates)}, "
        f"trips {len(trips_rows)}->{len(kept_trips)}, "
        f"stop_times {len(stop_rows)}->{len(kept_stop_times)}"
    )


if __name__ == "__main__":
    main()
