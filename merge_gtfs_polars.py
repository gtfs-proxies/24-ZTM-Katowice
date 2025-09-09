# merge_gtfs_polars_progress.py
import io
import zipfile
import datetime as dt
from pathlib import Path

import polars as pl
from tqdm import tqdm

# ---- Config ---------------------------------------------------------
PATTERN = "schedule_ztm*.zip"  # adjust if needed
OUT_ZIP = "ztm_merged_polars.zip"

PATH = '/tmp/original/'

CORE = [
    "agency.txt", "stops.txt", "routes.txt", "shapes.txt",
    "trips.txt", "stop_times.txt", "calendar.txt", "calendar_dates.txt",
    "fare_attributes.txt", "feed_info.txt",
]

# ---- Helpers --------------------------------------------------------
def read_txt_from_zip(zpath: Path, name: str) -> pl.DataFrame:
    # Read bytes -> Polars (keep all as strings: GTFS-safe)
    with zipfile.ZipFile(zpath) as zf:
        data = zf.read(name)
    return pl.read_csv(io.BytesIO(data))

def df_to_csv_str(df: pl.DataFrame) -> str:
    buf = io.StringIO()
    df.write_csv(buf)
    return buf.getvalue()

# ---- Load phase (with progress bars) --------------------------------
feeds = sorted(Path(PATH).glob(PATTERN))
if not feeds:
    raise SystemExit(f"No files matched: {PATTERN}")

store: dict[str, list[pl.DataFrame]] = {t: [] for t in CORE}
extras: dict[str, list[pl.DataFrame]] = {}

tqdm.write(f"Found {len(feeds)} GTFS zips.")

for z in tqdm(feeds, desc="Reading feeds", unit="zip"):
    with zipfile.ZipFile(z) as zf:
        names = [n for n in zf.namelist() if n.endswith(".txt")]
    for name in tqdm(names, desc=f"{z.name}", unit="file", leave=False):
        df = read_txt_from_zip(z, name)
        (store if name in CORE else extras).setdefault(name, []).append(df)

# ---- Merge & de-duplicate ------------------------------------------
merged: dict[str, pl.DataFrame] = {}
for name, dfs in tqdm(store.items(), desc="Concatenating core tables", unit="table"):
    if not dfs:
        continue
    # Fast vertical concat + drop exact duplicates
    merged[name] = pl.concat(dfs, how="vertical_relaxed").unique(maintain_order=True)


# Build feed_info from full date span
if "calendar_dates.txt" in merged and merged["calendar_dates.txt"].height > 0:
    dates = merged["calendar_dates.txt"].get_column("date")
    feed_info = pl.DataFrame({
        "feed_publisher_name": ["GZM ZTM"],
        "feed_publisher_url": ["https://metropoliaztm.pl"],
        "feed_lang": ["pl"],
        "feed_start_date": [dates.min()],   # YYYYMMDD strings -> lexical min/max work
        "feed_end_date":   [dates.max()],
        "feed_version":    [f"ztm_combined_{dt.date.today().isoformat()}"],
    })
    merged["feed_info.txt"] = feed_info

# Merge extras (tables not in CORE) if any
for name, dfs in tqdm(extras.items(), desc="Concatenating extra tables", unit="table"):
    merged[name] = pl.concat(dfs, how="vertical_relaxed").unique(maintain_order=True)

# ---- Write out ------------------------------------------------------
with zipfile.ZipFile(OUT_ZIP, "w", zipfile.ZIP_DEFLATED) as out:
    for name, df in tqdm(sorted(merged.items()), desc="Writing GTFS zip", unit="table"):
        out.writestr(name, df_to_csv_str(df))

tqdm.write(f"✅ Wrote {OUT_ZIP}")
