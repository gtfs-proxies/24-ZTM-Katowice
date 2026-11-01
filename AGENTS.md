# Agent guide

## Scope and safety
- This repository publishes the ZTM Katowice GTFS proxy. See `README.md` for the
  pipeline and `docs/review.md` for known risks and follow-up work.
- Fetch upstream before starting a branch. Do not commit directly to `main`.
- Keep commits small and focused. Include this trailer in every commit:
  `Co-authored-by: GPT-6.1 Sol (OpenCode) <noreply@openai.com>`.
- Do not push, merge, trigger publishing, or regenerate tracked `feed/` data
  unless explicitly requested. Preserve unrelated changes and local ZIP files.
- Do not remove legacy entry points just because the workflow no longer uses
  them; check callers and document any intentional compatibility break.

## Data invariants
- Merge inputs are highest priority first: newest downloads, then cached master.
  A winning trip owns its complete stop-time/frequency group; a winning shape
  owns its complete point sequence. Do not resurrect stale trailing rows.
- Preserve distinct calendar ranges for reused service IDs. Source archives are
  not interchangeable full snapshots. Seven-day retention is conservative, not
  a strict trip-age bound. Preserve the all-trips pruning safeguard.
- Keep CSV headers, quoting, UTF-8 BOM, precision rules, and extension tables
  compatible. Use `csv_transform.rewrite_csv` for row-wise rewrites; failed
  transformations must leave the source intact. Stream large tables.
- Use `/tmp/$FEED_NAME/` for pipeline intermediates. CSV replacement requires
  this location and the checkout to be on the same filesystem; otherwise it
  fails safely. No additional Python dependencies are needed for tests.
- Keep download/cache behavior deterministic. Pin and document new dependencies.

## Verification
Run from the repository root on Linux with Python 3.14 and Git:

```sh
PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp python3 -B -m unittest discover -s tests -v
for script in *.sh; do bash -n "$script" || exit; done
git diff --check
```

Tests use synthetic feeds and temporary directories, not the live `feed/`.
Set `TMPDIR` to an existing writable directory under `/tmp` if needed. Add
regression tests for changed merge/CSV behavior. Unit tests are not a full GTFS
validation; publishing changes also need a reviewed Actions run and release ZIP.
