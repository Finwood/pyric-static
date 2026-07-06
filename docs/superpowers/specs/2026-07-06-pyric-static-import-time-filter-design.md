# pyric-static import time filter — Design Specification

**Status:** Approved (2026-07-06)
**Repo:** `pyric-static` (extend existing import mode)

## Background & context

The batch `pyric-static import` command (see
`docs/superpowers/specs/2026-07-06-pyric-static-transfer-import-design.md`)
discovers transfer parquet under positional hive roots, deletes existing Influx
points for each `(logger, session)` within the parquet time range, and re-uploads
decoded **Message** rows.

Today the import reads **every row** in every discovered parquet file. For large
sessions or targeted backfills, operators need to restrict import to a time window
without narrowing positional roots via shell globs.

Transfer parquet stores `timestamp` as UTC microseconds (`pa.timestamp("us", tz="UTC")`).
PyArrow can push timestamp predicates down to parquet row groups using column
statistics, avoiding full-file decode when data falls outside the window.

## Goals

- Add optional `--start` and `--stop` CLI flags to `pyric-static import`.
- Keep positional hive roots unchanged (`ROOT ...`).
- When both flags are set, import only rows with `timestamp >= start AND timestamp < stop`.
- Scope Influx delete to the same window (partial re-import; safe to re-run).
- Use PyArrow filter pushdown (Dataset scanner) plus lightweight file pruning via
  parquet row-group metadata.
- Preserve current behavior when neither flag is set.

## Non-goals

- Open-ended ranges (start-only or stop-only).
- Filtering by logger or session on the CLI (shell globs on roots remain sufficient).
- Changing live/replay mode.
- Parallel processing or Dagster integration.

## CLI

Add two optional flags to `import_main` in `cli.py`:

```bash
# Full import (unchanged)
pyric-static import --config pyric-static.toml /mnt/data/transfers

# Single local calendar day (Apr 18 00:00 .. Apr 19 00:00 local, converted to UTC)
pyric-static import --config pyric-static.toml \
  --start 2026-04-18 --stop 2026-04-19 /mnt/data/transfers

# Explicit sub-day window
pyric-static import --config pyric-static.toml \
  --start 2026-04-18T08:00:00 --stop 2026-04-18T12:00:00 /mnt/data/transfers
```

| Argument | Required | Description |
| --- | --- | --- |
| `--start` | paired | Lower bound, inclusive. Must appear together with `--stop`. |
| `--stop` | paired | Upper bound, exclusive. Must appear together with `--start`. |
| `ROOT ...` | yes | Unchanged: one or more hive roots |
| `--config`, `--dry-run`, `--log-level` | — | Unchanged |

### Flag pairing and validation

- **Both or neither:** passing only `--start` or only `--stop` is a CLI error.
- **`start < stop`:** if `start >= stop` after parsing, CLI error.
- **Unparseable values:** CLI error naming the offending argument.

When neither flag is set, `ImportRunner` behavior is identical to today.

## Timestamp parsing

New function `parse_time_bound(value: str) -> datetime` in `transfers.py` (or a
small shared helper module if preferred). **Start and stop use identical parsing
rules** — there is no `is_stop` parameter.

| Input form | Parsed bound |
| --- | --- |
| Full ISO 8601 with timezone | Absolute instant, normalized to UTC |
| Full ISO 8601 without timezone | Local timezone assumed; normalized to UTC |
| Date only (`YYYY-MM-DD`) | `00:00:00` on that date in **local** timezone; normalized to UTC |

Examples (local TZ = `Europe/Berlin`, UTC+2):

| Flag | Input | Normalized UTC |
| --- | --- | --- |
| `--start` | `2026-04-18` | `2026-04-17T22:00:00Z` |
| `--stop` | `2026-04-19` | `2026-04-18T22:00:00Z` |
| `--start` | `2026-04-18T08:00:00` | `2026-04-18T06:00:00Z` |
| `--stop` | `2026-04-18T12:00:00+02:00` | `2026-04-18T10:00:00Z` |

Date-only `--stop` uses midnight local on that date as the exclusive upper bound —
**not** end-of-day. To cover a full calendar day, set `--stop` to the next date.

Use `datetime.fromisoformat` where possible; accept `Z` suffix by normalizing to
`+00:00`. Wire parsing through `argparse` `type=parse_time_bound`.

## Architecture

```text
pyric-static import [--start T0] [--stop T1] ROOT ...
          │
          ▼
ImportRunner(start?, stop?)     # both None, or both UTC datetimes
  ├── discover sessions (unchanged)
  └── for each (logger, session):
        if filtered:
          ├── prune files via row-group timestamp stats
          ├── skip session if no files overlap [start, stop)
          ├── Influx delete [start, stop) for (logger, session)
          └── for each overlapping file:
                Dataset scanner (filter pushdown) → batches → decode → write
        else:
          ├── scan full session time range (unchanged)
          ├── Influx delete [t_min, t_max] (unchanged)
          └── read all rows (unchanged ParquetFile.iter_batches path)
```

## Import logic (filtered mode)

When `start` and `stop` are set (UTC, `start < stop`):

1. Discover and group sessions from positional roots — unchanged.
2. For each session, iterate parquet files in sorted order.
3. **File pruning (lightweight C):** read parquet footer metadata for each file.
   For each row group, inspect `timestamp` column statistics (min/max) when present.
   If **all** row groups with stats show `max < start` or `min >= stop`, skip the
   file. If any row group lacks timestamp stats, treat the file as overlapping
   (safe fallback — filtered read still applies).
4. If no files remain, log at INFO and **skip the session** (no delete, no write).
5. Otherwise:
   - **Delete** Influx points: `start` inclusive, `stop` exclusive, predicate
     `logger="{logger}" AND session="{session}"`.
   - **Read** rows via filtered Dataset scanner (see below).
   - Decode and write Message rows — unchanged from current `_handle_row`.

On `--dry-run` with filters: log `(logger, session, start, stop, overlapping_file_count)`
and count rows that would be written; no delete or Influx write.

### Idempotency

Filtered import deletes and re-uploads only within `[start, stop)`. Re-running the
same command with the same roots and bounds is idempotent for that window. Data
outside the window in Influx is untouched.

## PyArrow implementation

### Filtered read (approach A)

Extend `iter_transfer_batches` in `transfers.py`:

```python
def iter_transfer_batches(
    path: Path,
    *,
    batch_size: int = 10_000,
    start: datetime | None = None,
    stop: datetime | None = None,
) -> Iterator[pa.RecordBatch]:
```

When `start` and `stop` are both set:

```python
import pyarrow.dataset as ds

ts_type = TransferSchema.field("timestamp").type
filter_expr = (
    (pc.field("timestamp") >= pa.scalar(start, type=ts_type))
    & (pc.field("timestamp") < pa.scalar(stop, type=ts_type))
)
scanner = ds.dataset(path, format="parquet").scanner(
    filter=filter_expr,
    batch_size=batch_size,
)
yield from scanner.to_batches()
```

When neither bound is set, keep the existing unfiltered path:

```python
yield from pq.ParquetFile(path).iter_batches(batch_size=batch_size)
```

Always call `assert_transfer_schema(path)` before reading.

### File overlap check (lightweight C)

New function:

```python
def file_overlaps_range(path: Path, start: datetime, stop: datetime) -> bool:
```

Return `False` only when row-group `timestamp` statistics prove no row can fall in
`[start, stop)`. Return `True` when stats are missing or any row group might overlap.

Implementation sketch: `pq.ParquetFile(path).metadata.row_group(i)` → column chunk
statistics for the `timestamp` column index from schema.

### Unfiltered mode unchanged

`scan_session_time_range(files)` continues to drive delete bounds when no CLI filters
are set. Do not call it for filtered mode — use the user-supplied `start`/`stop`
directly for Influx delete.

## Modified modules

| Module | Change |
| --- | --- |
| `cli.py` | Add `--start` / `--stop` with pairing validation; pass bounds to `ImportRunner` |
| `transfers.py` | `parse_time_bound`, `file_overlaps_range`, filtered `iter_transfer_batches` |
| `import_app.py` | Accept optional `start`/`stop`; branch filtered vs full session path |
| `README.md` | Document new flags and date-only semantics |
| `tests/test_transfers.py` | Parsing, overlap pruning, filtered batch iteration |
| `tests/test_import_app.py` | Filtered import integration |
| `tests/test_cli.py` | Flag pairing and forwarding |

No new dependencies (`pyarrow>=19` already present).

## Error handling

| Condition | Behavior |
| --- | --- |
| Only one of `--start` / `--stop` | `argparse` error |
| `start >= stop` | `argparse` error |
| Unparseable timestamp | `argparse` error |
| Parquet schema mismatch | Fail file; abort session (unchanged) |
| Filtered session, files overlap but zero rows match | Log INFO; delete still runs (idempotent empty re-import) |
| Influx delete/write failure | Fail session; stop run (unchanged) |

## Testing

| Test | Scope |
| --- | --- |
| `parse_time_bound` | ISO with/without TZ; date-only → local midnight; UTC normalization |
| `file_overlaps_range` | Skip file when stats show no overlap; include when stats absent |
| `iter_transfer_batches` filtered | Only rows in `[start, stop)` returned |
| CLI pairing | One flag alone errors; both passed through to runner |
| Import filtered | Writes subset of rows; dry-run counts match |
| Import idempotent window | Second run with same bounds produces same result |

Fixtures continue to use in-memory parquet with `TransferSchema`; set explicit
timestamps per row to exercise window boundaries.

## Alternatives considered

| Approach | Verdict |
| --- | --- |
| Tuple filters on `pq.read_table` only | Rejected: less natural streaming; Dataset scanner preferred |
| Metadata pruning only, no row filter | Rejected: stats can be missing; row-level filter required |
| Open-ended start or stop | Rejected: user requires both-or-neither |
| Date-only stop = end of day | Rejected: user requires midnight local as exclusive bound |
| Full-session delete with filtered upload | Rejected: would wipe data outside the import window |

## Relationship to transfer import v1

This spec extends import mode only. The original transfer import design listed CLI
time filters as a post-v1 item. This design implements that item. Positional root
discovery, hive tagging, schema validation, and Message-only upload policy are
unchanged.
