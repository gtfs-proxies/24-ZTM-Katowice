#!/usr/bin/env python3
"""
Fix overlapping block assignments.
Clear block_id for trips that have overlapping stop times in the same block.
"""

import csv
from pathlib import Path
from collections import defaultdict

FEED_DIR = Path("feed")


def parse_gtfs_time(time_str: str) -> int:
    """Convert GTFS time (HH:MM:SS) to seconds since midnight."""
    if not time_str:
        return 0
    parts = time_str.split(':')
    hours = int(parts[0])
    minutes = int(parts[1])
    seconds = int(parts[2])
    return hours * 3600 + minutes * 60 + seconds


def fix_overlapping_blocks(trips_filepath: Path, stop_times_filepath: Path):
    """Find and fix trips with overlapping times in the same block."""
    
    # Load all trips with block_id
    trips_by_block: dict[str, list[dict[str, str]]] = defaultdict(list)
    all_trips: dict[str, dict[str, str]] = {}
    
    with open(trips_filepath, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            trip_id = row['trip_id']
            block_id = row.get('block_id', '').strip()
            service_id = row['service_id']
            
            all_trips[trip_id] = row
            
            if block_id:
                trips_by_block[block_id].append({
                    'trip_id': trip_id,
                    'service_id': service_id,
                    'block_id': block_id
                })
    
    # Load stop times for trips
    trip_times: dict[str, dict[str, int | None]] = {}
    with open(stop_times_filepath, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            trip_id = row['trip_id']
            arrival_time = row.get('arrival_time', '')
            
            if trip_id not in trip_times:
                trip_times[trip_id] = {'start': None, 'end': None}
            
            arrival_seconds = parse_gtfs_time(arrival_time)
            
            if trip_times[trip_id]['start'] is None:
                trip_times[trip_id]['start'] = arrival_seconds
            trip_times[trip_id]['end'] = arrival_seconds
    
    # Find overlapping trips in each block
    trips_to_clear = set()
    
    for block_id, trips in trips_by_block.items():
        # Group by service_id first
        by_service = defaultdict(list)
        for trip in trips:
            by_service[trip['service_id']].append(trip)
        
        # Check overlaps within each service
        for service_id, service_trips in by_service.items():
            # Get times for each trip
            trip_intervals = []
            for trip in service_trips:
                trip_id = trip['trip_id']
                if trip_id in trip_times:
                    times = trip_times[trip_id]
                    if times['start'] is not None and times['end'] is not None:
                        trip_intervals.append({
                            'trip_id': trip_id,
                            'start': times['start'],
                            'end': times['end']
                        })
            
            # Sort by start time
            trip_intervals.sort(key=lambda x: x['start'])
            
            # Check for overlaps
            for i in range(len(trip_intervals) - 1):
                current = trip_intervals[i]
                next_trip = trip_intervals[i + 1]
                
                # If current trip ends after next trip starts, they overlap
                if current['end'] > next_trip['start']:
                    # Mark both trips for block_id clearing
                    trips_to_clear.add(current['trip_id'])
                    trips_to_clear.add(next_trip['trip_id'])
    
    # Update trips.txt - clear block_id for problematic trips
    rows = []
    with open(trips_filepath, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        
        for row in reader:
            if row['trip_id'] in trips_to_clear:
                row['block_id'] = ''
            rows.append(row)
    
    # Write back
    with open(trips_filepath, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    
    print("Fixed overlapping blocks:")
    print(f"  - Cleared block_id for {len(trips_to_clear)} trips with time overlaps")


# Fix trips.txt
fix_overlapping_blocks(FEED_DIR / "trips.txt", FEED_DIR / "stop_times.txt")
