#!/bin/bash

set -euo pipefail

FILE_LOCATION=/tmp/$FEED_NAME
SOURCE_DIR="$FILE_LOCATION/original"
OUTPUT_ZIP="$FILE_LOCATION/output.zip"
TEMP_ZIP="$FILE_LOCATION/output.tmp.zip"

shopt -s nullglob
archives=("$SOURCE_DIR"/*.zip)

if [ ${#archives[@]} -eq 0 ]; then
    echo "No GTFS archives found in $SOURCE_DIR" >&2
    exit 1
fi

IFS=$'\n' sorted_archives=($(printf '%s\n' "${archives[@]}" | sort))
unset IFS

if [ -f "$OUTPUT_ZIP" ]; then
    cp -f "$OUTPUT_ZIP" "$TEMP_ZIP"
    mv -f "$TEMP_ZIP" "$OUTPUT_ZIP"
else
    cp -f "${sorted_archives[0]}" "$OUTPUT_ZIP"
    sorted_archives=("${sorted_archives[@]:1}")
fi

if [ ${#sorted_archives[@]} -gt 0 ]; then
    for archive in "${sorted_archives[@]}"; do
        ./gtfsmerge.py "$OUTPUT_ZIP" "$archive" "$TEMP_ZIP"
        mv -f "$TEMP_ZIP" "$OUTPUT_ZIP"
    done
fi
