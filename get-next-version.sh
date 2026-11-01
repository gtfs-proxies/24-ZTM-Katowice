#!/bin/bash

set -euo pipefail

YEAR=$(date +'%Y')
# List remote tag names without downloading tagged commits or their feed data.
LAST_NUMBER=$(git ls-remote --tags --refs origin "refs/tags/${YEAR}.*" |
    awk -v year="$YEAR" '$2 ~ ("^refs/tags/" year "\\.[0-9]+$") {
        sub(/.*\./, "", $2)
        print $2
    }' |
    sort -n | tail -1)
printf '%s.%s\n' "$YEAR" "$((10#${LAST_NUMBER:-0} + 1))"
