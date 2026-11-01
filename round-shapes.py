#!/usr/bin/env python3
"""
Round geographic and distance values in GTFS feed.
- shapes.txt: lat/lon to 6 decimals, shape_dist_traveled to 2 decimals
- stop_times.txt: shape_dist_traveled to 2 decimals
"""

from pathlib import Path

from csv_transform import rewrite_csv

FEED_DIR = Path("feed")

def round_file(filepath: Path, columns_precision: dict[str, int]):
    """
    Round specified columns in a CSV file.
    columns_precision: dict of {column_name: decimal_places}
    """
    if not filepath.exists():
        print(f"Skipping {filepath.name} (not found)")
        return
    
    def round_row(row):
        for col, decimals in columns_precision.items():
            val = row.get(col, '')
            if val and val.strip():
                try:
                    num = float(val)
                    row[col] = f"{num:.{decimals}f}"
                except ValueError:
                    pass
        return row

    rewrite_csv(filepath, round_row)
    
    print(f"Rounded values in {filepath.name}")

# Round shapes.txt: lat/lon to 6 decimals, distance to 2 decimals
round_file(FEED_DIR / "shapes.txt", {
    'shape_pt_lat': 6,
    'shape_pt_lon': 6,
    'shape_dist_traveled': 2,
})

# Round stop_times.txt: distance to 2 decimals
round_file(FEED_DIR / "stop_times.txt", {
    'shape_dist_traveled': 2,
})
