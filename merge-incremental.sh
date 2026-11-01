#!/bin/bash

set -euo pipefail

FILE_LOCATION=/tmp/$FEED_NAME
SOURCE_DIR="$FILE_LOCATION/original"
OUTPUT_ZIP="$FILE_LOCATION/output.zip"
TEMP_ZIP="$FILE_LOCATION/output.tmp.zip"

shopt -s nullglob
INPUTS=("$SOURCE_DIR"/*.zip)
if [ "${#INPUTS[@]}" -eq 0 ]; then
    echo "No source GTFS archives found in $SOURCE_DIR" >&2
    exit 1
fi
# Dated source filenames sort newest first; cached records are only a fallback.
mapfile -t INPUTS < <(printf '%s\n' "${INPUTS[@]}" | LC_ALL=C sort -r)
if [ -f "$OUTPUT_ZIP" ]; then
    INPUTS+=("$OUTPUT_ZIP")
fi
./gtfsmerge.py "${INPUTS[@]}" "$TEMP_ZIP"
mv -f "$TEMP_ZIP" "$OUTPUT_ZIP"
