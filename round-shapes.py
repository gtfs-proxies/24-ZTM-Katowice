#!/usr/bin/env python3
"""
Round geographic and distance values in GTFS feed.
- shapes.txt: lat/lon to 6 decimals, shape_dist_traveled to 2 decimals
- stop_times.txt: shape_dist_traveled to 2 decimals
"""

import csv
from pathlib import Path

FEED_DIR = Path("feed")

def round_file(filepath: Path, columns_precision: dict[str, int]):
    """
    Round specified columns in a CSV file.
    columns_precision: dict of {column_name: decimal_places}
    """
    if not filepath.exists():
        print(f"Skipping {filepath.name} (not found)")
        return
    
    rows: list[dict[str, str]] = []
    with open(filepath, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        
        for row in reader:
            for col, decimals in columns_precision.items():
                val = row.get(col, '')
                if val and val.strip():
                    try:
                        num = float(val)
                        row[col] = f"{num:.{decimals}f}"
                    except ValueError:
                        pass
            rows.append(row)
    
    # Write back
    with open(filepath, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    
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
