"""Stream a CSV transformation and replace the source only after success."""

import csv
import os
from pathlib import Path
import stat
import tempfile


def rewrite_csv(filepath: Path, transform_row):
    work_dir = Path("/tmp") / os.environ.get("FEED_NAME", "24-ZTM-Katowice")
    work_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f"{filepath.name}.", dir=work_dir) as directory:
        temporary = Path(directory) / filepath.name
        with filepath.open(encoding="utf-8-sig") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames is None:
                raise ValueError(f"Missing CSV header in {filepath}")
            with temporary.open("w", encoding="utf-8-sig", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=reader.fieldnames)
                writer.writeheader()
                for row in reader:
                    writer.writerow(transform_row(row))
        temporary.chmod(stat.S_IMODE(filepath.stat().st_mode))
        # Fail safely if the filesystems differ; never truncate the source.
        temporary.replace(filepath)
