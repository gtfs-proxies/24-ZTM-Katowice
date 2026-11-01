#!/usr/bin/env python3
"""
Clean up invisible/problematic whitespace characters in GTFS files.
Removes:
- Non-breaking spaces (U+00A0)
- Zero-width spaces (U+200B)
- Control characters
"""

from pathlib import Path

from csv_transform import rewrite_csv

FEED_DIR = Path("feed")

# Characters to remove
CHARS_TO_REMOVE = {
    '\u00A0',  # Non-breaking space
    '\u200B',  # Zero-width space
    '\u200C',  # Zero-width non-joiner
    '\u200D',  # Zero-width joiner
    '\uFEFF',  # Zero-width no-break space (BOM)
}

def clean_value(value):
    """Remove problematic characters from a string."""
    if not isinstance(value, str):
        return value
    for char in CHARS_TO_REMOVE:
        value = value.replace(char, '')
    return value.strip()

def clean_file(filepath):
    """Clean a CSV file."""
    if not filepath.exists():
        print(f"Skipping {filepath.name} (not found)")
        return
    
    rewrite_csv(filepath, lambda row: {k: clean_value(v) for k, v in row.items()})
    
    print(f"Cleaned {filepath.name}")

# Clean all txt files in feed directory
for txt_file in sorted(FEED_DIR.glob("*.txt")):
    clean_file(txt_file)

print("Done!")
