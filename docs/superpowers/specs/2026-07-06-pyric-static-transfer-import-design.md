# pyric-static transfer import — Design Specification

**Status:** Approved (2026-07-06)
**Repo:** `pyric-static` (extend in place; no new sibling project)

## Background & context

`pyric-static` is a passive Cyphal/CAN-to-InfluxDB logger. Today it reads live or
recorded **CAN frames**, reassembles them with `pycyphal.transport.can.CANTracer`,
deserializes DSDL payloads, and writes InfluxDB points using the same measurement,
tag, and field layout as `pyric`'s `NodeExplorer`.

The `frame-decoding-pipeline` (a separate project) already produces **pre-assembled
Cyphal transfer parquet** files as step 2 of its batch workflow:

0. Download CANedge MF4 files from S3.
1. MF4 → frame parquet.
2. Reassemble Cyphal frames → transfer parquet.
3. Session context extraction.

Transfer files follow a hive layout and a stable Arrow schema (originally defined in
the private `sc-schema` package used by the pipeline):

```text
{root}/logger={device_id}/session={session:08d}/{stem}.parquet
```

Each row is one completed Cyphal transfer (`Message`, `Request`, or `Response`) with
metadata and raw payload bytes. Reassembly is already done; the remaining work is
DSDL resolution, deserialization, and Influx upload — the same core logic
`pyric-static` already implements for live/replay mode.

This design adds a **batch import mode** to `pyric-static`: a CLI job that walks one
or more hive roots, reads transfer parquet files, and uploads decoded messages to
InfluxDB.

### Relationship to live `pyric-static`

| Concern | Live / replay mode | Batch import mode |
| --- | --- | --- |
| Input | CAN frames (`python-can`) | Transfer parquet (pyarrow) |
| Reassembly | `CANTracer` | None (already reassembled) |
| DSDL resolution | TOML `[[nodes]]` / implicit standard ports | Same |
| Influx schema | pyric `NodeExplorer` layout | Same, plus `session` tag |
| `logger` / `iface` tags | From TOML `[logger]` section | From hive partition / transfer `channel` |
| Idempotency | N/A (streaming) | Scoped delete + re-upload per session |

## Goals

- Batch CLI: point at one or more folder trees of transfer parquet files and upload
  decoded **Message** transfers to InfluxDB.
- Reuse existing `Config`, DSDL resolution, deserialize, flatten, and `InfluxWriter`
  logic — no duplicate project.
- Derive Influx tags from CANedge hive metadata and transfer rows, not from `[logger]`.
- Idempotent re-runs: delete existing points for `(logger, session)` within the
  parquet time range, then re-upload.
- No dependency on private packages (`sc-schema` is vendored locally).

## Non-goals (v1)

- Importing `Request` / `Response` service transfers.
- Parallel processing (single-process, sequential sessions and files).
- Dagster orchestration or pipeline integration.
- Writing unresolved/raw payloads when DSDL mapping is missing.
- Filtering by session or logger on the CLI (shell globs on positional roots suffice).

## Architecture

```text
transfer hive root(s)/
  logger=3544BCD3/
    session=00000509/
      6604293D.parquet
      A1B2C3D4.parquet
          │
          ▼
ImportRunner
  ├── discover: glob logger=*/session=*/*.parquet per positional root
  ├── group & dedupe by (logger, session)
  └── for each session (sequential):
        1. scan timestamps → [t_min, t_max]
        2. Influx delete_api (scoped window + tag predicate)
        3. for each parquet file (sorted):
             read batches → Message rows only
             resolve PortSpec → deserialize → InfluxWriter.write_message
          │
          ▼
InfluxDB (pyric schema + session tag)
```

### New modules

| Module | Responsibility |
| --- | --- |
| `transfer_schema.py` | Vendored `TransferSchema` (from `sc-schema/sc_schema/transfers/arrow.py`) |
| `transfers.py` | Hive discovery, path parsing, parquet batch iteration, time-range scan |
| `import_app.py` | `ImportRunner`: orchestrate delete-then-upload per session |
| `cli.py` | Add `import` subcommand alongside existing live/replay entry |

### Modified modules

| Module | Change |
| --- | --- |
| `influx.py` | Accept optional `session` tag on writes; add `delete_session(logger, session, start, stop)` |
| `pyproject.toml` | Add `pyarrow` dependency |

### Unchanged

`config.py`, `dsdl.py`, `flatten.py`, `metrics.py`, `reassembly.py`, `sources.py`,
`logger_app.py` (live path untouched).

## Transfer schema (vendored)

Copy the Arrow schema into `pyric_static/transfer_schema.py`. Source of truth:
`sc-schema/sc_schema/transfers/arrow.py` in the `canedge` monorepo. Do **not** add
`sc-schema` as a package dependency.

```python
TransferSchema = pa.schema([
    pa.field("channel", pa.string()),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("type", pa.string(), nullable=False),       # Message | Request | Response
    pa.field("id", pa.int16(), nullable=False),          # subject or service ID
    pa.field("source", pa.uint8()),
    pa.field("dest", pa.uint8()),
    pa.field("priority", pa.uint8()),
    pa.field("transfer_id", pa.uint8()),
    pa.field("payload", pa.binary()),
    pa.field("length", pa.int32()),
])
```

When reading a parquet file, validate that the on-disk schema is compatible with
`TransferSchema`. On mismatch, fail the file with a clear error showing path and
schema diff.

## CLI

Add an `import` subcommand. Live/replay mode stays on the top-level `pyric-static`
entry (unchanged).

```bash
pyric-static import --config pyric-static.toml /mnt/data/transfers
pyric-static import --config pyric-static.toml /mnt/a /mnt/b /mnt/c
pyric-static import --config pyric-static.toml ~/canedge/transfers/logger=3544BCD3/session=00000509
pyric-static import --config pyric-static.toml /mnt/data/transfers --dry-run
```

| Argument | Required | Description |
| --- | --- | --- |
| `--config` | yes | Path to TOML (`[[nodes]]`, `[influx]`; `[logger]` ignored) |
| `ROOT ...` | yes | One or more positional hive roots (`nargs="+"`); shell expansion works |
| `--dry-run` | no | Decode and count; no delete, no write |
| `--log-level` | no | Same as live mode (default `INFO`) |

### File discovery

For each positional root:

1. If `{root}/logger=*/session=*/*.parquet` matches, use that glob.
2. Else if `{root}` is itself a `session=*` directory, glob `{root}/*.parquet` and
   parse `logger` from the grandparent `logger=*` directory.
3. Else if `{root}` is a `logger=*` directory, glob `{root}/session=*/*.parquet`.

Merge results from all roots, dedupe by resolved absolute path, group by
`(logger, session)`. Process sessions in deterministic order (logger asc, session asc);
within a session, process parquet files in sorted filename order.

Logger and session values are parsed from directory names:

- `logger=3544BCD3` → tag `logger=3544BCD3`
- `session=00000509` → tag `session=00000509`

## Configuration

Reuse the existing TOML format unchanged:

- `[[nodes]]` / `[[nodes.ports]]` — DSDL type mappings (required for vendor ports).
- `[influx]` — bucket name (default `pyric`).
- `[logger]` — **ignored** in import mode (tags come from data).

Influx connection uses standard `INFLUXDB_V2_*` environment variables (same as live
mode).

## Data flow

For each transfer row where `type == "Message"`:

1. `port_id = row["id"]`, `src = row["source"]`.
2. `Config.resolve(src, port_id)` → `PortSpec` or skip (count as unresolved).
3. Trim payload to `row["length"]` bytes if zero-padded.
4. `pycyphal.dsdl.deserialize(port_spec.dtype, payload)`.
5. `InfluxWriter.write_message(...)` with tags:

| Tag | Source |
| --- | --- |
| `logger` | Hive partition (`logger={id}`) |
| `session` | Hive partition (`session={id}`) |
| `iface` | Transfer `channel` field (e.g. `CAN1`) |
| `node_id` | Transfer `source` (or `"anonymous"`) |
| `port_id`, `port_name` | From `PortSpec` |
| `app_name`, `device_uid` | From `[[nodes]]` metadata if source node is configured |

**Timestamp:** transfer `timestamp` (microsecond UTC) → nanoseconds for Influx write.

**Skipped transfer types:** `Request` and `Response` are counted in metrics but not
written (same policy as live `PassiveLogger`).

Implicit standard ports (heartbeat, time sync) resolve by `port_id` alone, same as
live mode.

## Idempotency: scoped delete before upload

For each `(logger, session)` group:

### Phase 1 — scan time range

Read the `timestamp` column from all parquet files in the session (column-only read
is sufficient). Compute:

```python
t_min = min(timestamp)  # earliest Message row, or earliest row if scanning all types
t_max = max(timestamp)  # latest row
```

If the session has no readable rows, skip delete and upload for that session.

Use the timestamp range across **all** transfer types when scanning (not only
Messages), so delete covers any previously imported data in the same window even if
type filters change later.

### Phase 2 — delete, then upload

```python
delete_api.delete(
    start=t_min,              # ISO-8601, inclusive
    stop=t_max + timedelta(microseconds=1),  # exclusive upper bound
    predicate=f'logger="{logger}" AND session="{session}"',
    bucket=bucket,
    org=org,
)
```

Then read and upload all parquet files for the session.

- Delete runs **once** per session before any uploads.
- Flush the Influx writer after each session completes.
- On `--dry-run`: log `(logger, session, t_min, t_max, file_count, message_count)`
  instead of deleting or writing.

**Re-run behavior:** pointing at the same roots re-processes all discovered sessions.
Each session is wiped within its parquet time window and re-uploaded. To import a
subset, pass narrower positional roots via shell globs.

## Error handling & metrics

| Condition | Behavior |
| --- | --- |
| Parquet schema mismatch | Fail the file; abort the session |
| Unresolved subject / unlisted node | Count in metrics; skip row (same as live) |
| Deserialize failure | Log exception; count; continue |
| Influx delete failure | Fail the session; stop the run |
| Influx write failure | Fail the session; stop the run |

Log per session on completion: `logger`, `session`, file count, time range,
messages written, skipped (non-message, unresolved, deserialize failed).

Exit code `0` if all sessions succeed; `1` if any session fails.

## Testing

| Test | Scope |
| --- | --- |
| Hive path parsing | Unit: directory names → `(logger, session)` tags |
| Multi-root discovery | Unit: merge, dedupe, grouping |
| Time-range scan | Unit: min/max across multiple in-memory parquet files |
| Import dry-run | Integration: small parquet table → assert decode counts |
| Delete API call | Unit (mocked): verify predicate, start, stop per session |

Test fixtures build parquet in memory with `TransferSchema`; no dependency on real
CANedge data files in the repo.

## Alternatives considered

| Approach | Verdict |
| --- | --- |
| Separate sibling project copying shared logic | Rejected: duplication and drift risk |
| Shared library extraction (`pyric-influx-core`) | Rejected: over-engineered for current scope |
| `sc-schema` package dependency | Rejected: private dependency; schema vendored instead |
| All-history delete per session | Rejected: scoped to parquet time range |
| Parallel per-session workers | Rejected for v1: sequential is sufficient |

## Open items (post-v1, not blocking)

- CLI filter flags (`--logger`, `--session`) if shell globs become awkward.
- Service transfer import (Request/Response) with service port mappings.
- Optional `[logger]` fallback for `iface` when transfer `channel` is null.
