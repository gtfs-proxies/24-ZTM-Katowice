#!/bin/bash

set -euo pipefail

FILE_LOCATION=/tmp/$FEED_NAME
SOURCE_DIR="$FILE_LOCATION/original"
OUTPUT_ZIP="$FILE_LOCATION/output.zip"
TEMP_ZIP="$FILE_LOCATION/output.tmp.zip"

if [ -f "$OUTPUT_ZIP" ]; then
    # Merge cached output with all new downloads in one pass
    ./gtfsmerge.py "$OUTPUT_ZIP" "$SOURCE_DIR"/*.zip "$TEMP_ZIP"
    mv -f "$TEMP_ZIP" "$OUTPUT_ZIP"
else
    # No cache: merge all downloads into new output
    ./gtfsmerge.py "$SOURCE_DIR"/*.zip "$OUTPUT_ZIP"
fi
