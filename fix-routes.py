#!/usr/bin/env python3
"""
Fix routes.txt issues:
- Use route_desc as source for proper Title Case when route_long_name is ALL CAPS
- Remove route_desc when it duplicates route_long_name
"""

import csv
from pathlib import Path
from typing import Optional

FEED_DIR = Path("feed")

def find_fragment_in_text(fragment: str, text: str) -> Optional[str]:
    """
    Find a fragment in text (case-insensitive) and return it with original casing from text.
    Returns None if not found.
    """
    fragment_lower = fragment.lower()
    text_lower = text.lower()
    
    # Try to find the fragment in the text
    pos = text_lower.find(fragment_lower)
    if pos != -1:
        # Return the fragment with original casing from text
        return text[pos:pos + len(fragment)]
    
    return None

def extract_matching_prefix(all_caps: str, title_case: str) -> str:
    """
    Split all_caps by " - " separator, find each part in title_case,
    and reconstruct with proper casing from title_case.
    """
    if not title_case:
        # No desc to use - fallback to simple title case
        return ' '.join(word.capitalize() for word in all_caps.split())
    
    # Split route name by " - " (the standard GTFS separator for route endpoints)
    parts = all_caps.split(' - ')
    
    fixed_parts = []
    for part in parts:
        # Try to find this part in the desc
        matched = find_fragment_in_text(part, title_case)
        
        if matched:
            # Found it - use the version from desc
            fixed_parts.append(matched)
        else:
            # Not found - use simple title case
            fixed_parts.append(' '.join(word.capitalize() for word in part.split()))
    
    return ' - '.join(fixed_parts)

def fix_routes(filepath: Path):
    """Fix route_long_name case using route_desc as reference, and remove duplicate route_desc."""
    if not filepath.exists():
        print(f"Skipping {filepath.name} (not found)")
        return
    
    rows = []
    fixed_from_desc = 0
    removed_desc = 0
    
    with open(filepath, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        
        for row in reader:
            long_name = row.get('route_long_name', '').strip()
            desc = row.get('route_desc', '').strip()
            
            # Fix route_long_name using route_desc if available
            if long_name and long_name.isupper() and desc:
                # Extract matching prefix from desc
                new_long_name = extract_matching_prefix(long_name, desc)
                if new_long_name != long_name:
                    row['route_long_name'] = new_long_name
                    fixed_from_desc += 1
            
            # Remove route_desc if it now duplicates route_long_name
            long_name = row.get('route_long_name', '').strip()
            if desc and long_name and desc.lower() == long_name.lower():
                row['route_desc'] = ''
                removed_desc += 1
            
            rows.append(row)
    
    # Write back
    with open(filepath, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Fixed {filepath.name}:")
    print(f"  - Converted {fixed_from_desc} route names using route_desc as reference")
    print(f"  - Removed {removed_desc} duplicate route descriptions")

# Fix routes.txt
fix_routes(FEED_DIR / "routes.txt")
