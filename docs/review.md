# Repository review — 2026-11-01

Scope: processing scripts, regression tests, and publishing workflow, based on
`0d84670` and Actions run [868](https://github.com/gtfs-proxies/24-ZTM-Katowice/actions/runs/41313365341).
This is a code/operational review, not an independent validation of the release ZIP.

## Prioritized follow-ups

| Priority | Finding | Recommended change / verification |
| --- | --- | --- |
| High | `download.sh` has no strict failure handling; metadata failure or an earlier failed wget can be hidden by a later successful command. Resume/timestamp downloads write directly into the input directory. | Fail on metadata/HTTP/download errors, stage and validate ZIPs before promotion. Test empty metadata, malformed JSON, an early failed download followed by success, and interrupted archives. Preserve timestamp/cache behavior deliberately. |
| High | `unzip.sh` overwrites files but never removes tables absent from the new archive. | Extract into a fresh staging directory and replace the generated directory only after validation. Test an optional table disappearing, a corrupt ZIP, and safe failure recovery. |
| High | `fix-blocks.py` treats file order as trip order, uses arrival times only, and compares adjacent intervals only. A long interval containing several short ones can leave some overlapping trips assigned. Different service IDs are never compared, even if their operating dates overlap. | Define the intended service/calendar semantics, then test unsorted stop sequences, nested intervals, blank times, departures, and times beyond 24:00. Keep this correctness change separate from storage refactors. |
| High | Tag errors are ignored and publishing depends on a new auto-commit. Regenerating `feed_version` also makes otherwise unchanged inputs look changed. | Design explicit no-change detection and recovery for failure after commit/tag but before asset upload. Do not simply skip unchanged source URLs: retention can change with time. |
| Medium | `gtfstidy@latest` and moving action tags are not fully reproducible; Go 1.20 and several action runtimes are old. Run 868 reports Node 20 deprecation and a missing `go.sum` cache warning. | Pin a tested tidy revision, select a supported Go version, upgrade actions after checking their interfaces, and disable setup-go's module cache for this non-module repo (or introduce a deliberate tool cache). |
| Medium | The cached master cannot have a strict historical bound while reused service IDs retain all associated trips. Both source and master caches plus LFS history add storage cost. | Measure retained records/cache sizes before redesigning provenance. Check downstream consumers before changing distribution or removing tracked feed data. |
| Medium | `gtfsmerge.py` catches `KeyError` around table processing and logs at ERROR level; malformed rows may silently truncate a table's contribution. Unknown extension tables use their first column as a key. | Validate required columns, distinguish missing members from malformed rows, and make intentional fallbacks visible. Add malformed-row and extension-key tests before changing behavior. |

## Low-risk simplifications

- Reuse the safe streaming CSV writer for routes and block updates instead of
  keeping multiple in-memory read/write implementations.
- Iterate merge inputs in their existing priority order; there is no need to
  search for a reference archive and rebuild the same ordering for each table.
- Run the offline regression suite in a separate read-only PR workflow. It needs
  neither LFS downloads nor publishing permissions.
- Keep existing command-line entry points for now. Avoid a framework, packaging
  migration, or a single large rewrite: the important complexity is GTFS source
  semantics, not the number of scripts.

## Validation boundary

Run 868 succeeded and published `2026.197`; its tidy log reported no records
dropped due to errors. Warnings include upstream route contrast, duplicate route
descriptions (partly addressed by the later route fix), and a calendar ending
yesterday (expected with seven-day retention). These are not proof that the
published feed has no semantic defects. A full GTFS validator and checks against
the actual release asset remain useful follow-up work.
