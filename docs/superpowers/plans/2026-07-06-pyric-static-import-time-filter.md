# pyric-static Import Time Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional `--start` / `--stop` flags to `pyric-static import` so operators can import a time window from hive roots, with PyArrow filter pushdown and scoped Influx delete for idempotent partial re-import.

**Architecture:** Extend `transfers.py` with timestamp parsing, parquet row-group overlap pruning, and filtered Dataset scanner reads. `ImportRunner` branches between full-session import (unchanged) and filtered import (prune files → delete window → read filtered batches). CLI validates both-or-neither bounds and `start < stop`.

**Tech Stack:** Python 3.14, pyarrow>=19, pytest, uv.

**Reference spec:** `docs/superpowers/specs/2026-07-06-pyric-static-import-time-filter-design.md`

---

## File Structure

| File | Change |
| --- | --- |
| `src/pyric_static/transfers.py` | Add `parse_time_bound`, `file_overlaps_range`, `filter_session_files`; extend `iter_transfer_batches` |
| `src/pyric_static/import_app.py` | Accept `start`/`stop`; filtered session path |
| `src/pyric_static/cli.py` | Add `--start` / `--stop`; pairing validation |
| `tests/test_transfers.py` | Parsing, overlap, filtered batches |
| `tests/test_import_app.py` | Filtered dry-run + delete-range integration |
| `tests/test_cli.py` | Flag pairing and forwarding |
| `README.md` | Document time filter flags |

---

## Task 1: `parse_time_bound`

**Files:**
- Modify: `src/pyric_static/transfers.py`
- Modify: `tests/test_transfers.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_transfers.py`:

```python
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from pyric_static.transfers import parse_time_bound


def test_parse_time_bound_utc_z_suffix():
    assert parse_time_bound("2026-04-18T10:27:31Z") == datetime(
        2026, 4, 18, 10, 27, 31, tzinfo=timezone.utc
    )


def test_parse_time_bound_date_only_uses_local_midnight(monkeypatch):
    monkeypatch.setenv("TZ", "Europe/Berlin")
    # Force local TZ reload for this process if needed; fromisoformat uses system TZ
    dt = parse_time_bound("2026-04-18")
    assert dt == datetime(2026, 4, 17, 22, 0, 0, tzinfo=timezone.utc)


def test_parse_time_bound_naive_datetime_uses_local_tz(monkeypatch):
    monkeypatch.setenv("TZ", "Europe/Berlin")
    dt = parse_time_bound("2026-04-18T08:00:00")
    assert dt == datetime(2026, 4, 18, 6, 0, 0, tzinfo=timezone.utc)


def test_parse_time_bound_invalid_raises():
    import pytest

    with pytest.raises(ValueError, match="invalid time bound"):
        parse_time_bound("not-a-date")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/lasse/work/canedge/pyric-static && uv run pytest tests/test_transfers.py::test_parse_time_bound_utc_z_suffix -v`
Expected: FAIL with `ImportError` or `AttributeError: parse_time_bound`

- [ ] **Step 3: Implement `parse_time_bound`**

Add imports at top of `src/pyric_static/transfers.py`:

```python
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
```

Add after `_SESSION_RE`:

```python
def _local_tz() -> ZoneInfo:
    return datetime.now().astimezone().tzinfo or timezone.utc  # type: ignore[return-value]


def parse_time_bound(value: str) -> datetime:
    """Parse an ISO 8601 CLI time bound; normalize to UTC.

    Date-only values (YYYY-MM-DD) map to 00:00:00 local on that date.
    Naive datetimes assume local timezone. Accepts trailing ``Z``.
    """
    text = value.strip()
    if not text:
        raise ValueError(f"invalid time bound: {value!r}")
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        if len(text) == 10 and text[4] == "-" and text[7] == "-":
            dt = datetime.fromisoformat(text).replace(tzinfo=_local_tz())
        else:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_local_tz())
    except ValueError as exc:
        raise ValueError(f"invalid time bound: {value!r}") from exc
    return dt.astimezone(timezone.utc)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/lasse/work/canedge/pyric-static && TZ=Europe/Berlin uv run pytest tests/test_transfers.py -k parse_time_bound -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pyric_static/transfers.py tests/test_transfers.py
git commit -m "feat: add parse_time_bound for import CLI timestamps"
```

---

## Task 2: `file_overlaps_range`

**Files:**
- Modify: `src/pyric_static/transfers.py`
- Modify: `tests/test_transfers.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_transfers.py`:

```python
from pyric_static.transfers import file_overlaps_range
from tests.transfer_fixtures import make_transfer_row, write_transfer_parquet


def test_file_overlaps_range_true_when_rows_inside_window(tmp_path):
    path = tmp_path / "inside.parquet"
    write_transfer_parquet(
        path,
        [
            make_transfer_row(timestamp=datetime(2026, 4, 18, 10, 0, 0, tzinfo=timezone.utc)),
        ],
    )
    start = datetime(2026, 4, 18, 9, 0, 0, tzinfo=timezone.utc)
    stop = datetime(2026, 4, 18, 11, 0, 0, tzinfo=timezone.utc)
    assert file_overlaps_range(path, start, stop) is True


def test_file_overlaps_range_false_when_stats_prove_no_overlap(tmp_path):
    path = tmp_path / "outside.parquet"
    write_transfer_parquet(
        path,
        [
            make_transfer_row(timestamp=datetime(2026, 4, 18, 8, 0, 0, tzinfo=timezone.utc)),
        ],
    )
    start = datetime(2026, 4, 18, 9, 0, 0, tzinfo=timezone.utc)
    stop = datetime(2026, 4, 18, 11, 0, 0, tzinfo=timezone.utc)
    assert file_overlaps_range(path, start, stop) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_transfers.py -k file_overlaps_range -v`
Expected: FAIL with `ImportError` / `AttributeError`

- [ ] **Step 3: Implement `file_overlaps_range` and `filter_session_files`**

Add to `src/pyric_static/transfers.py`:

```python
def _timestamp_column_index(schema: pa.Schema) -> int:
    return schema.get_field_index("timestamp")


def file_overlaps_range(path: Path, start: datetime, stop: datetime) -> bool:
    """Return False only when row-group stats prove no row in [start, stop)."""
    assert_transfer_schema(path)
    pf = pq.ParquetFile(path)
    metadata = pf.metadata
    if metadata is None or metadata.num_row_groups == 0:
        return True
    ts_index = _timestamp_column_index(pf.schema_arrow)
    saw_stats = False
    for rg in range(metadata.num_row_groups):
        col_meta = metadata.row_group(rg).column(ts_index)
        stats = col_meta.statistics
        if stats is None or not stats.has_min_max:
            return True
        saw_stats = True
        ts_type = TransferSchema.field("timestamp").type
        rg_min = pa.scalar(stats.min, type=ts_type).as_py()
        rg_max = pa.scalar(stats.max, type=ts_type).as_py()
        if rg_max >= start and rg_min < stop:
            return True
    return not saw_stats


def filter_session_files(
    files: Sequence[Path],
    start: datetime,
    stop: datetime,
) -> list[Path]:
    return [path for path in files if file_overlaps_range(path, start, stop)]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_transfers.py -k file_overlaps_range -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pyric_static/transfers.py tests/test_transfers.py
git commit -m "feat: prune parquet files by row-group timestamp stats"
```

---

## Task 3: Filtered `iter_transfer_batches`

**Files:**
- Modify: `src/pyric_static/transfers.py`
- Modify: `tests/test_transfers.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_transfers.py`:

```python
from pyric_static.transfers import iter_transfer_batches


def test_iter_transfer_batches_filtered_returns_window_rows_only(tmp_path):
    path = tmp_path / "mixed.parquet"
    write_transfer_parquet(
        path,
        [
            make_transfer_row(timestamp=datetime(2026, 4, 18, 8, 0, 0, tzinfo=timezone.utc)),
            make_transfer_row(timestamp=datetime(2026, 4, 18, 10, 0, 0, tzinfo=timezone.utc)),
            make_transfer_row(timestamp=datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)),
        ],
    )
    start = datetime(2026, 4, 18, 9, 0, 0, tzinfo=timezone.utc)
    stop = datetime(2026, 4, 18, 11, 0, 0, tzinfo=timezone.utc)
    rows = []
    for batch in iter_transfer_batches(path, start=start, stop=stop):
        rows.extend(batch.to_pylist())
    assert len(rows) == 1
    assert rows[0]["timestamp"] == datetime(2026, 4, 18, 10, 0, 0, tzinfo=timezone.utc)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_transfers.py::test_iter_transfer_batches_filtered_returns_window_rows_only -v`
Expected: FAIL (returns 3 rows or TypeError on unexpected kwargs)

- [ ] **Step 3: Extend `iter_transfer_batches`**

Replace `iter_transfer_batches` in `src/pyric_static/transfers.py`:

```python
import pyarrow.dataset as ds


def iter_transfer_batches(
    path: Path,
    *,
    batch_size: int = 10_000,
    start: datetime | None = None,
    stop: datetime | None = None,
) -> Iterator[pa.RecordBatch]:
    assert_transfer_schema(path)
    if (start is None) != (stop is None):
        raise ValueError("start and stop must both be set or both omitted")
    if start is not None and stop is not None:
        ts_type = TransferSchema.field("timestamp").type
        filter_expr = (pc.field("timestamp") >= pa.scalar(start, type=ts_type)) & (
            pc.field("timestamp") < pa.scalar(stop, type=ts_type)
        )
        scanner = ds.dataset(path, format="parquet").scanner(
            filter=filter_expr,
            batch_size=batch_size,
        )
        yield from scanner.to_batches()
        return
    yield from pq.ParquetFile(path).iter_batches(batch_size=batch_size)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_transfers.py -v`
Expected: PASS (all transfer tests)

- [ ] **Step 5: Commit**

```bash
git add src/pyric_static/transfers.py tests/test_transfers.py
git commit -m "feat: filter transfer parquet batches by timestamp window"
```

---

## Task 4: Filtered `ImportRunner` path

**Files:**
- Modify: `src/pyric_static/import_app.py`
- Modify: `tests/test_import_app.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_import_app.py`:

```python
from unittest.mock import MagicMock, patch

from pyric_static.transfers import filter_session_files


def test_import_filtered_dry_run_skips_outside_window(tmp_path, monkeypatch):
    spec = PortSpec(port_id=7509, port_name="heartbeat", type_str="uavcan.node.Heartbeat.1.0", dtype=object)
    cfg = Config(
        logger=None,
        influx=InfluxSection(bucket="pyric"),
        nodes={},
        explicit_ports={},
        standard_ports={7509: spec},
    )
    monkeypatch.setattr(
        "pyric_static.import_app.pycyphal.dsdl.deserialize",
        lambda _dtype, _payload: {"uptime": 123},
    )
    root = make_hive_session(
        tmp_path,
        "3544BCD3",
        "00000509",
        files={
            "a.parquet": [
                make_transfer_row(
                    timestamp=datetime(2026, 4, 18, 8, 0, 0, tzinfo=timezone.utc),
                    source=42,
                    payload=b"\x00" * 7,
                    subject_id=7509,
                ),
                make_transfer_row(
                    timestamp=datetime(2026, 4, 18, 10, 0, 0, tzinfo=timezone.utc),
                    source=42,
                    payload=b"\x00" * 7,
                    subject_id=7509,
                ),
            ],
        },
    )
    start = datetime(2026, 4, 18, 9, 0, 0, tzinfo=timezone.utc)
    stop = datetime(2026, 4, 18, 11, 0, 0, tzinfo=timezone.utc)
    stats = ImportRunner(cfg, roots=[root], dry_run=True, start=start, stop=stop).run()
    assert stats.sessions == 1
    assert stats.written == 1


def test_import_filtered_calls_delete_with_window(tmp_path, monkeypatch):
    spec = PortSpec(port_id=7509, port_name="heartbeat", type_str="uavcan.node.Heartbeat.1.0", dtype=object)
    cfg = Config(
        logger=None,
        influx=InfluxSection(bucket="pyric"),
        nodes={},
        explicit_ports={},
        standard_ports={7509: spec},
    )
    monkeypatch.setattr(
        "pyric_static.import_app.pycyphal.dsdl.deserialize",
        lambda _dtype, _payload: {"uptime": 123},
    )
    root = make_hive_session(
        tmp_path,
        "L",
        "S",
        files={
            "a.parquet": [
                make_transfer_row(
                    timestamp=datetime(2026, 4, 18, 10, 0, 0, tzinfo=timezone.utc),
                    source=42,
                    payload=b"\x00" * 7,
                    subject_id=7509,
                ),
            ],
        },
    )
    start = datetime(2026, 4, 18, 9, 0, 0, tzinfo=timezone.utc)
    stop = datetime(2026, 4, 18, 11, 0, 0, tzinfo=timezone.utc)
    mock_writer = MagicMock()
    with patch("pyric_static.import_app.InfluxWriter.from_import", return_value=mock_writer):
        ImportRunner(cfg, roots=[root], dry_run=False, start=start, stop=stop).run()
    mock_writer.delete_range.assert_called_once()
    kwargs = mock_writer.delete_range.call_args.kwargs
    assert kwargs["start"] == start
    assert kwargs["stop"] == stop
    assert kwargs["logger"] == "L"
    assert kwargs["session"] == "S"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_import_app.py -k filtered -v`
Expected: FAIL with `TypeError: ImportRunner.__init__() got an unexpected keyword argument 'start'`

- [ ] **Step 3: Implement filtered path in `ImportRunner`**

Update imports in `src/pyric_static/import_app.py`:

```python
from datetime import datetime

from .transfers import (
    delete_stop_exclusive,
    discover_sessions,
    filter_session_files,
    iter_transfer_batches,
    scan_session_time_range,
    timestamp_to_ns,
    trim_payload,
)
```

Update `ImportRunner.__init__`:

```python
class ImportRunner:
    def __init__(
        self,
        cfg: Config,
        *,
        roots: list[Path],
        dry_run: bool = False,
        start: datetime | None = None,
        stop: datetime | None = None,
    ) -> None:
        if (start is None) != (stop is None):
            raise ValueError("start and stop must both be set or both omitted")
        if start is not None and stop is not None and start >= stop:
            raise ValueError("start must be before stop")
        self.cfg = cfg
        self.roots = roots
        self.dry_run = dry_run
        self.start = start
        self.stop = stop
        self.stats = ImportStats()
        self.writer: InfluxWriter | None = None
```

Replace `_import_session` body:

```python
    def _import_session(self, logger: str, session: str, files: list[Path]) -> None:
        if self.start is not None and self.stop is not None:
            files = filter_session_files(files, self.start, self.stop)
            if not files:
                _logger.info(
                    "skip session (no files overlap window): logger=%s session=%s start=%s stop=%s",
                    logger,
                    session,
                    self.start.isoformat(),
                    self.stop.isoformat(),
                )
                return
            t_min, t_max, t_stop = self.start, self.stop, self.stop
        else:
            t_min, t_max = scan_session_time_range(files)
            t_stop = delete_stop_exclusive(t_max)

        session_written = 0
        session_skipped = 0

        if self.dry_run:
            _logger.info(
                "dry-run: logger=%s session=%s files=%d range=%s..%s",
                logger,
                session,
                len(files),
                t_min.isoformat(),
                t_max.isoformat(),
            )
        else:
            assert self.writer is not None
            self.writer.delete_range(logger=logger, session=session, start=t_min, stop=t_stop)

        read_kwargs: dict = {}
        if self.start is not None and self.stop is not None:
            read_kwargs = {"start": self.start, "stop": self.stop}

        for path in files:
            for batch in iter_transfer_batches(path, **read_kwargs):
                for row in batch.to_pylist():
                    written, skipped = self._handle_row(logger, session, row)
                    session_written += written
                    session_skipped += skipped

        self.stats.written += session_written
        self.stats.skipped_non_message += session_skipped
        self.stats.sessions += 1

        m = self.stats.metrics
        _logger.info(
            "session complete: logger=%s session=%s written=%d non_msg=%d unresolved=%d deserialize_failed=%d",
            logger,
            session,
            session_written,
            session_skipped,
            sum(m.unresolved_subject.values()),
            sum(m.deserialize_failed.values()),
        )
```

Also move `self.stats.sessions += 1` out of the outer `run()` loop — it now increments inside `_import_session` only when the session is processed (including filtered empty skip which returns early without incrementing). Verify `run()` no longer has `self.stats.sessions += 1` in the `for` loop:

```python
            for (logger, session), files in sessions.items():
                try:
                    self._import_session(logger, session, files)
                except Exception:
                    ...
```

Remove the old `self.stats.sessions += 1` from the `for` loop in `run()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_import_app.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pyric_static/import_app.py tests/test_import_app.py
git commit -m "feat: scoped import delete and read for time-filtered sessions"
```

---

## Task 5: CLI `--start` / `--stop`

**Files:**
- Modify: `src/pyric_static/cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_cli.py`:

```python
from datetime import datetime, timezone

from pyric_static.cli import import_main


def test_import_requires_start_and_stop_together(tmp_path: Path):
    cfg = tmp_path / "c.toml"
    cfg.write_text("[influx]\nbucket = 'pyric'\n")
    hive = tmp_path / "logger=L" / "session=S"
    hive.mkdir(parents=True)
    with pytest.raises(SystemExit):
        import_main(["--config", str(cfg), "--start", "2026-04-18", str(hive)])


def test_import_forwards_time_bounds(tmp_path: Path, monkeypatch):
    cfg = tmp_path / "c.toml"
    cfg.write_text("[influx]\nbucket = 'pyric'\n")
    hive = tmp_path / "logger=L" / "session=S"
    hive.mkdir(parents=True)
    called: dict = {}

    class FakeRunner:
        def __init__(self, _cfg, *, roots, dry_run, start, stop):
            called["roots"] = roots
            called["dry_run"] = dry_run
            called["start"] = start
            called["stop"] = stop

        def run(self):
            class R:
                failed_sessions = 0

            return R()

    monkeypatch.setattr("pyric_static.cli.ImportRunner", FakeRunner)
    rc = import_main(
        [
            "--config",
            str(cfg),
            "--start",
            "2026-04-18T08:00:00Z",
            "--stop",
            "2026-04-18T12:00:00Z",
            str(hive),
        ]
    )
    assert rc == 0
    assert called["start"] == datetime(2026, 4, 18, 8, 0, 0, tzinfo=timezone.utc)
    assert called["stop"] == datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cli.py -k import -v`
Expected: FAIL (unexpected keyword `start` or no error on lone `--start`)

- [ ] **Step 3: Wire CLI flags**

Update `import_main` in `src/pyric_static/cli.py`:

```python
from .transfers import parse_time_bound
```

Inside `import_main`, after existing args:

```python
    parser.add_argument(
        "--start",
        type=parse_time_bound,
        default=None,
        metavar="TIME",
        help="inclusive lower bound (ISO 8601; date-only = local midnight); requires --stop",
    )
    parser.add_argument(
        "--stop",
        type=parse_time_bound,
        default=None,
        metavar="TIME",
        help="exclusive upper bound (ISO 8601; date-only = local midnight); requires --start",
    )
    args = parser.parse_args(argv)
    if (args.start is None) != (args.stop is None):
        parser.error("--start and --stop must be given together")
    if args.start is not None and args.start >= args.stop:
        parser.error("--start must be before --stop")
```

Update runner construction:

```python
        stats = ImportRunner(
            cfg,
            roots=list(args.roots),
            dry_run=args.dry_run,
            start=args.start,
            stop=args.stop,
        ).run()
```

Update existing `test_main_import_dispatches` FakeRunner to accept `start=None, stop=None`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cli.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pyric_static/cli.py tests/test_cli.py
git commit -m "feat: add --start and --stop flags to import subcommand"
```

---

## Task 6: README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document time filter flags**

In the **Import CANedge transfer parquet** section, after the existing examples, add:

```markdown
Optional time window (both flags required; bounds are `[start, stop)` in UTC after parsing):

```bash
# Single local calendar day
pyric-static import --config pyric-static.toml \
  --start 2026-04-18 --stop 2026-04-19 /mnt/data/transfers

# Sub-day window (ISO 8601; naive values use local timezone)
pyric-static import --config pyric-static.toml \
  --start 2026-04-18T08:00:00 --stop 2026-04-18T12:00:00 /mnt/data/transfers
```

Date-only values map to `00:00:00` local on that date. Filtered import deletes and
re-uploads only within the window (idempotent partial re-import).
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document import --start and --stop time filters"
```

---

## Task 7: Full verification

**Files:** (none)

- [ ] **Step 1: Run full test suite**

Run: `cd /home/lasse/work/canedge/pyric-static && uv run pytest -v`
Expected: all tests PASS

- [ ] **Step 2: Run linter**

Run: `uv run ruff check src tests`
Expected: no errors

---

## Spec Coverage Checklist

| Spec requirement | Task |
| --- | --- |
| `--start` / `--stop` CLI flags, both-or-neither | Task 5 |
| `start < stop` validation | Task 5 |
| ISO 8601 + local TZ + date-only midnight | Task 1 |
| Filter pushdown via Dataset scanner | Task 3 |
| Row-group metadata file pruning | Task 2 |
| Skip session when no overlapping files | Task 4 |
| Influx delete scoped to window | Task 4 |
| Unfiltered mode unchanged | Tasks 3–4 (branch when bounds None) |
| Dry-run with filters | Task 4 |
| README documentation | Task 6 |
