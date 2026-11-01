#!/usr/bin/env python3

import csv
import datetime as dt
from pathlib import Path
import sys


def extract_feed_dates(feed_dir: Path):
    dates = []
    for filename, columns in (
        ("calendar.txt", ("start_date", "end_date")),
        ("calendar_dates.txt", ("date",)),
    ):
        path = feed_dir / filename
        if not path.exists():
            continue
        with path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                # Removed service dates must not extend the feed's coverage.
                if filename == "calendar_dates.txt" and row.get("exception_type") != "1":
                    continue
                for column in columns:
                    value = (row.get(column) or "").strip()
                    try:
                        if len(value) != 8 or not value.isdigit():
                            raise ValueError
                        dt.datetime.strptime(value, "%Y%m%d")
                    except ValueError:
                        raise ValueError(f"{path}: invalid {column}: {value!r}") from None
                    dates.append(value)
    if not dates:
        raise ValueError("No service dates found in calendar.txt or calendar_dates.txt")
    return min(dates), max(dates)


def main():
    try:
        start_date, end_date = extract_feed_dates(Path("feed"))
    except ValueError as error:
        sys.exit(str(error))
    print(f"START_DATE={start_date}")
    print(f"END_DATE={end_date}")
    print(f"Feed date range: {start_date} to {end_date}", file=sys.stderr)


if __name__ == "__main__":
    main()
