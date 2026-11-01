# ZTM Katowice GTFS proxy

Downloads complementary GZM/ZTM schedules, merges them with a cached master,
normalizes the feed, and publishes versioned GitHub releases. The generated
`feed/` is tracked; `shapes.txt` and `stop_times.txt` use Git LFS.

## Pipeline

`.github/workflows/download-feeds.yml` runs on pushes to `main` and daily at
23:36 UTC:

1. Calculate the next `YEAR.NUMBER` version from remote tag names; restore source
   archives and the merged master from Actions caches.
2. `get-release-url.sh` selects up to ten schedule URLs from the extended GZM
   dataset. `download.sh` downloads them into `/tmp/$FEED_NAME/original/`.
3. Discard source archives dated more than seven days ago. `merge-incremental.sh`
   passes newest archives first and the previous master last to `gtfsmerge.py`.
   The merger prunes expired-only services and unused shapes while writing a
   temporary archive, then replaces `/tmp/$FEED_NAME/output.zip` on success.
4. Cache the merged master; run `gtfstidy` into `tidied.zip`; extract into `feed/`.
5. Clean whitespace, normalize routes, clear detected overlapping blocks, round
   geometry/distances, and derive service dates. Generate feed metadata.
6. Commit generated changes, tag the version, and publish `24-ZTM-Katowice.zip`.

Calendar ranges are unioned because archives cover different periods and reuse
service IDs. Trip timelines and shapes are replaced as whole groups by priority.
Retention therefore does **not** guarantee every retained trip is recent.
`prune-old-services.py` is a standalone legacy directory pruner, not a pipeline
step; running it after merging would duplicate retention work.

## Development

Use Linux, Python 3.10+, Bash, and Git for the offline tests. The publishing
pipeline additionally needs Git LFS, curl, jq, wget, zip/unzip, Go, and `gtfstidy`;
the workflow records its current install commands. `home-pl.pem` is the CA bundle
used for upstream HTTPS requests.

```sh
PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp python3 -B -m unittest discover -s tests -v
for script in *.sh; do bash -n "$script" || exit; done
git diff --check
```

Tests are offline and use temporary synthetic feeds. Run processing scripts only
in a disposable checkout/feed directory: they rewrite files relative to the
current directory. Set `FEED_NAME=24-ZTM-Katowice` for pipeline shell scripts.
CSV rewrites stage files under `/tmp/$FEED_NAME/` (default `24-ZTM-Katowice`);
the staging directory must share a filesystem with the source for replacement.

See [agent guidance](AGENTS.md) and the [repository review](docs/review.md).
