# pyric-static Transfer Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a batch `pyric-static import` CLI that reads CANedge transfer parquet files from hive directory trees, deletes existing Influx points for each `(logger, session)` within the parquet time window, and uploads decoded Message transfers using the same Influx schema as live mode.

**Architecture:** Extend `pyric-static` in place. Vendored `TransferSchema` + `transfers.py` handle hive discovery and parquet I/O. `ImportRunner` in `import_app.py` orchestrates per-session delete-then-upload. Reuse `Config.resolve`, `pycyphal.dsdl.deserialize`, `flatten`, and `RunMetrics`. `InfluxWriter` gains import-specific factory, scoped delete, and per-point `logger`/`session`/`iface` tags. Live/replay CLI stays backward-compatible (no subcommand required).

**Tech Stack:** Python 3.14, pyarrow, pycyphal, influxdb-client, pytest, uv.

**Reference spec:** `docs/superpowers/specs/2026-07-06-pyric-static-transfer-import-design.md`

**Already done (skip):** Optional `[logger]` in `config.py`; live/replay requires it (`cli.py`, `influx.py`); tests in `tests/test_config.py` and `tests/test_cli.py`.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `src/pyric_static/transfer_schema.py` | Vendored Arrow `TransferSchema` |
| `src/pyric_static/transfers.py` | Hive path parsing, file discovery, schema check, time-range scan, batch iteration |
| `src/pyric_static/import_app.py` | `ImportRunner`: delete → decode → write per session |
| `src/pyric_static/influx.py` | Add `from_import()`, `delete_range()`, extend `write_message()` with import tags |
| `src/pyric_static/cli.py` | Dispatch `import` subcommand; keep live/replay entry unchanged |
| `tests/transfer_fixtures.py` | Helpers to write in-memory transfer parquet for tests |
| `tests/test_transfers.py` | Discovery, path parsing, time-range scan |
| `tests/test_influx_import.py` | Delete API + import write tags (mocked Influx client) |
| `tests/test_import_app.py` | End-to-end dry-run import against temp hive |
| `pyproject.toml` | Add `pyarrow` dependency |
| `README.md` | Document `import` subcommand |

---

## Task 1: Add pyarrow dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add pyarrow to project dependencies**

In `pyproject.toml`, add to `[project].dependencies`:

```toml
    "pyarrow>=19",
```

- [ ] **Step 2: Sync lockfile**

Run: `cd /home/lasse/work/canedge/pyric-static && uv sync`
Expected: resolves without error; `pyarrow` importable.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "deps: add pyarrow for transfer parquet import"
```

---

## Task 2: Vendored transfer schema

**Files:**
- Create: `src/pyric_static/transfer_schema.py`
- Create: `tests/test_transfer_schema.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_transfer_schema.py`:

```python
import pyarrow as pa

from pyric_static.transfer_schema import TransferSchema


def test_transfer_schema_field_count():
    assert len(TransferSchema) == 10


def test_transfer_schema_timestamp_is_utc_microseconds():
    field = TransferSchema.field("timestamp")
    assert field.type == pa.timestamp("us", tz="UTC")
    assert field.nullable is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_transfer_schema.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pyric_static.transfer_schema'`

- [ ] **Step 3: Write minimal implementation**

Create `src/pyric_static/transfer_schema.py`:

```python
"""Vendored Cyphal transfer Arrow schema.

Source: sc-schema/sc_schema/transfers/arrow.py in the canedge monorepo.
Do not add sc-schema as a package dependency.
"""

from __future__ import annotations

import pyarrow as pa

_TRANSFER_FIELDS = [
    pa.field("channel", pa.string()),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("type", pa.string(), nullable=False),
    pa.field("id", pa.int16(), nullable=False),
    pa.field("source", pa.uint8()),
    pa.field("dest", pa.uint8()),
    pa.field("priority", pa.uint8()),
    pa.field("transfer_id", pa.uint8()),
    pa.field("payload", pa.binary()),
    pa.field("length", pa.int32()),
]

TransferSchema = pa.schema(_TRANSFER_FIELDS)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_transfer_schema.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/pyric_static/transfer_schema.py tests/test_transfer_schema.py
git commit -m "feat: vendored TransferSchema for CANedge transfer parquet"
```

---

## Task 3: Test fixtures helper

**Files:**
- Create: `tests/transfer_fixtures.py`

- [ ] **Step 1: Create parquet write helper**

Create `tests/transfer_fixtures.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pyric_static.transfer_schema import TransferSchema


def make_transfer_row(
    *,
    channel: str = "CAN1",
    timestamp: datetime | None = None,
    transfer_type: str = "Message",
    subject_id: int = 7509,
    source: int = 42,
    payload: bytes = b"\x00" * 7,
) -> dict:
    ts = timestamp or datetime(2026, 4, 18, 10, 27, 31, tzinfo=timezone.utc)
    return {
        "channel": channel,
        "timestamp": ts,
        "type": transfer_type,
        "id": subject_id,
        "source": source,
        "dest": None,
        "priority": 4,
        "transfer_id": 0,
        "payload": payload,
        "length": len(payload),
    }


def write_transfer_parquet(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=TransferSchema)
    pq.write_table(table, path)


def make_hive_session(tmp_path: Path, logger: str, session: str, *, files: dict[str, list[dict]]) -> Path:
    """Create logger=X/session=Y/*.parquet under tmp_path; return hive root."""
    root = tmp_path / "transfers"
    session_dir = root / f"logger={logger}" / f"session={session}"
    for name, rows in files.items():
        write_transfer_parquet(session_dir / name, rows)
    return root
```

- [ ] **Step 2: Commit**

```bash
git add tests/transfer_fixtures.py
git commit -m "test: add transfer parquet hive fixtures"
```

---

## Task 4: Hive discovery and path parsing

**Files:**
- Create: `src/pyric_static/transfers.py`
- Create: `tests/test_transfers.py`

- [ ] **Step 1: Write failing tests for path parsing and discovery**

Create `tests/test_transfers.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone

from pyric_static.transfers import discover_sessions, parse_hive_tags, scan_session_time_range
from tests.transfer_fixtures import make_hive_session, make_transfer_row, write_transfer_parquet


def test_parse_hive_tags_from_session_file(tmp_path):
    root = make_hive_session(
        tmp_path,
        "3544BCD3",
        "00000509",
        files={"a.parquet": [make_transfer_row()]},
    )
    path = root / "logger=3544BCD3" / "session=00000509" / "a.parquet"
    assert parse_hive_tags(path) == ("3544BCD3", "00000509")


def test_discover_sessions_full_hive(tmp_path):
    root = make_hive_session(
        tmp_path,
        "AAAA",
        "00000001",
        files={"1.parquet": [make_transfer_row()], "2.parquet": [make_transfer_row()]},
    )
    make_hive_session(
        tmp_path,
        "BBBB",
        "00000002",
        files={"x.parquet": [make_transfer_row()]},
    )
    sessions = discover_sessions([root])
    assert set(sessions.keys()) == {("AAAA", "00000001"), ("BBBB", "00000002")}
    assert len(sessions[("AAAA", "00000001")]) == 2


def test_discover_sessions_session_dir_root(tmp_path):
    session_dir = tmp_path / "logger=3544BCD3" / "session=00000509"
    write_transfer_parquet(session_dir / "only.parquet", [make_transfer_row()])
    sessions = discover_sessions([session_dir])
    assert list(sessions.keys()) == [("3544BCD3", "00000509")]


def test_discover_sessions_dedupes_across_roots(tmp_path):
    root = make_hive_session(tmp_path, "L", "S", files={"a.parquet": [make_transfer_row()]})
    path = root / "logger=L" / "session=S" / "a.parquet"
    sessions = discover_sessions([root, path.parent])
    assert len(sessions[("L", "S")]) == 1


def test_scan_session_time_range(tmp_path):
    root = make_hive_session(
        tmp_path,
        "L",
        "S",
        files={
            "a.parquet": [
                make_transfer_row(timestamp=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
            ],
            "b.parquet": [
                make_transfer_row(timestamp=datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)),
                make_transfer_row(
                    transfer_type="Request",
                    timestamp=datetime(2026, 1, 3, 0, 0, 0, tzinfo=timezone.utc),
                ),
            ],
        },
    )
    files = discover_sessions([root])[("L", "S")]
    t_min, t_max = scan_session_time_range(files)
    assert t_min == datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert t_max == datetime(2026, 1, 3, 0, 0, 0, tzinfo=timezone.utc)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_transfers.py -v`
Expected: FAIL with import error for `pyric_static.transfers`

- [ ] **Step 3: Implement transfers.py**

Create `src/pyric_static/transfers.py`:

```python
"""CANedge transfer parquet discovery and I/O."""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .transfer_schema import TransferSchema

_logger = logging.getLogger(__name__)

_LOGGER_RE = re.compile(r"^logger=(.+)$", re.IGNORECASE)
_SESSION_RE = re.compile(r"^session=(.+)$", re.IGNORECASE)


def parse_hive_tags(path: Path) -> tuple[str, str]:
    """Return (logger, session) from a parquet file inside a CANedge hive."""
    resolved = path.resolve()
    logger: str | None = None
    session: str | None = None
    for part in resolved.parents:
        name = part.name
        if session is None and (m := _SESSION_RE.match(name)):
            session = m.group(1)
        elif logger is None and (m := _LOGGER_RE.match(name)):
            logger = m.group(1)
        if logger is not None and session is not None:
            break
    if logger is None or session is None:
        raise ValueError(f"cannot parse hive tags from path: {path}")
    return logger, session


def _glob_parquet_files(root: Path) -> list[Path]:
    root = root.resolve()
    patterns = [
        root.glob("logger=*/session=*/*.parquet"),
        root.glob("session=*/*.parquet") if _SESSION_RE.match(root.name) else iter(()),
        root.glob("*.parquet") if _SESSION_RE.match(root.name) else iter(()),
    ]
    if _LOGGER_RE.match(root.name):
        patterns.append(root.glob("session=*/*.parquet"))
    found: dict[Path, Path] = {}
    for matches in patterns:
        for path in matches:
            if path.is_file():
                found[path.resolve()] = path.resolve()
    return sorted(found.values())


def discover_sessions(roots: Sequence[Path]) -> dict[tuple[str, str], list[Path]]:
    """Discover parquet files grouped by (logger, session), deduped and sorted."""
    grouped: dict[tuple[str, str], dict[Path, Path]] = defaultdict(dict)
    for root in roots:
        for path in _glob_parquet_files(root):
            logger, session = parse_hive_tags(path)
            grouped[(logger, session)][path.resolve()] = path.resolve()
    return {
        key: sorted(files.values())
        for key, files in sorted(grouped.items(), key=lambda kv: kv[0])
    }


def assert_transfer_schema(path: Path) -> None:
    schema = pq.read_schema(path)
    if not schema.equals(TransferSchema):
        raise ValueError(
            f"{path}: transfer parquet schema mismatch\n"
            f"expected: {TransferSchema}\n"
            f"actual:   {schema}"
        )


def scan_session_time_range(files: Sequence[Path]) -> tuple[datetime, datetime]:
    """Return [t_min, t_max] across all rows in the given parquet files."""
    t_min: datetime | None = None
    t_max: datetime | None = None
    for path in files:
        assert_transfer_schema(path)
        table = pq.read_table(path, columns=["timestamp"])
        if table.num_rows == 0:
            continue
        col = table.column("timestamp")
        batch_min = pc.min(col).as_py()
        batch_max = pc.max(col).as_py()
        if batch_min is None or batch_max is None:
            continue
        t_min = batch_min if t_min is None else min(t_min, batch_min)
        t_max = batch_max if t_max is None else max(t_max, batch_max)
    if t_min is None or t_max is None:
        raise ValueError("session has no timestamp rows")
    return t_min, t_max


def delete_stop_exclusive(t_max: datetime) -> datetime:
    return t_max + timedelta(microseconds=1)


def iter_transfer_batches(path: Path, *, batch_size: int = 10_000) -> Iterator[pa.RecordBatch]:
    assert_transfer_schema(path)
    reader = pq.ParquetFile(path).iter_batches(batch_size=batch_size)
    yield from reader


def timestamp_to_ns(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000_000)


def trim_payload(payload: bytes | None, length: int | None) -> bytes:
    if not payload:
        return b""
    if length is None or length < 0:
        return bytes(payload)
    return bytes(payload[:length])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_transfers.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/pyric_static/transfers.py tests/test_transfers.py
git commit -m "feat: discover transfer parquet files in CANedge hive layout"
```

---

## Task 5: Influx import writer and scoped delete

**Files:**
- Modify: `src/pyric_static/influx.py`
- Create: `tests/test_influx_import.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_influx_import.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pyric_static.config import Config, InfluxSection
from pyric_static.dsdl import PortSpec
from pyric_static.influx import InfluxWriter


@pytest.fixture
def import_writer():
    with patch("pyric_static.influx.InfluxDBClient") as mock_cls:
        client = MagicMock()
        client.url = "http://localhost:8086"
        client.org = "org"
        client.default_tags = {}
        mock_cls.from_env_properties.return_value = client
        writer_api = MagicMock()
        client.write_api.return_value = writer_api
        client.delete_api.return_value = MagicMock()
        cfg = Config(
            logger=None,
            influx=InfluxSection(bucket="pyric"),
            nodes={},
            explicit_ports={},
            standard_ports={},
        )
        w = InfluxWriter.from_import(cfg)
        w._mock_client = client  # type: ignore[attr-defined]
        w._mock_writer = writer_api  # type: ignore[attr-defined]
        yield w


def test_from_import_sets_no_default_tags(import_writer: InfluxWriter):
    assert import_writer.client.default_tags == {}


def test_delete_range_calls_delete_api(import_writer: InfluxWriter):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    stop = datetime(2026, 1, 2, tzinfo=timezone.utc)
    import_writer.delete_range(
        logger="3544BCD3",
        session="00000509",
        start=start,
        stop=stop,
    )
    delete_api = import_writer.client.delete_api.return_value
    delete_api.delete.assert_called_once()
    _args, kwargs = delete_api.delete.call_args
    assert kwargs["predicate"] == 'logger="3544BCD3" AND session="00000509"'
    assert kwargs["bucket"] == "pyric"
    assert kwargs["org"] == "org"


def test_write_message_includes_import_tags(import_writer: InfluxWriter):
    spec = PortSpec(port_id=7509, port_name="heartbeat", type_str="t", dtype=object)
    import_writer.write_message(
        spec=spec,
        source_node_id=42,
        message={"uptime": 1},
        timestamp_ns=1_700_000_000_000_000_000,
        node_meta=None,
        import_tags={"logger": "L", "session": "S", "iface": "CAN1"},
    )
    import_writer.writer.write.assert_called_once()
    _bucket, record = import_writer.writer.write.call_args[0]
    assert record._tags["logger"] == "L"
    assert record._tags["session"] == "S"
    assert record._tags["iface"] == "CAN1"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_influx_import.py -v`
Expected: FAIL (`from_import` / `delete_range` / `import_tags` not defined)

- [ ] **Step 3: Extend influx.py**

Modify `src/pyric_static/influx.py`:

1. Add imports: `from datetime import datetime`

2. Add `from_import` classmethod after `from_config`:

```python
    @classmethod
    def from_import(cls, cfg: Config) -> "InfluxWriter":
        client = InfluxDBClient.from_env_properties()
        if client.default_tags is None:
            client.default_tags = {}
        writer = client.write_api(
            write_options=WriteOptions(batch_size=1000, flush_interval=4000, jitter_interval=2000)
        )
        _logger.info(
            "Influx import writer ready: url=%s org=%s bucket=%s",
            client.url,
            client.org,
            cfg.influx.bucket,
        )
        return cls(bucket=cfg.influx.bucket, client=client, writer=writer)
```

3. Add `delete_range` method:

```python
    def delete_range(
        self,
        *,
        logger: str,
        session: str,
        start: datetime,
        stop: datetime,
    ) -> None:
        predicate = f'logger="{logger}" AND session="{session}"'
        self.client.delete_api().delete(
            start=start,
            stop=stop,
            predicate=predicate,
            bucket=self.bucket,
            org=self.client.org,
        )
        _logger.info(
            "Influx delete: logger=%s session=%s start=%s stop=%s",
            logger,
            session,
            start.isoformat(),
            stop.isoformat(),
        )
```

4. Extend `write_message` signature with optional `import_tags`:

```python
    def write_message(
        self,
        spec: PortSpec,
        source_node_id: int | None,
        message: Any,
        timestamp_ns: int,
        node_meta: NodeMeta | None,
        *,
        import_tags: dict[str, str] | None = None,
    ) -> None:
        tags: dict[str, Any] = {
            "node_id": source_node_id if source_node_id is not None else "anonymous",
            "port_id": spec.port_id,
            "port_name": spec.port_name,
        }
        if import_tags is not None:
            tags.update(import_tags)
        ...
```

Live path continues to call `write_message` without `import_tags`; `from_config` still sets `logger`/`iface` on `client.default_tags`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_influx_import.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/pyric_static/influx.py tests/test_influx_import.py
git commit -m "feat: Influx import writer with scoped delete and per-point tags"
```

---

## Task 6: ImportRunner (dry-run path)

**Files:**
- Create: `src/pyric_static/import_app.py`
- Create: `tests/test_import_app.py`

- [ ] **Step 1: Write failing dry-run integration test**

Create `tests/test_import_app.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pycyphal.dsdl
import pytest
import uavcan.node

from pyric_static.config import Config, InfluxSection, load
from pyric_static.import_app import ImportRunner
from tests.transfer_fixtures import make_hive_session, make_transfer_row


def _write_config(tmp_path: Path) -> Path:
    cfg = tmp_path / "import.toml"
    cfg.write_text(
        """
[influx]
bucket = "pyric"
"""
    )
    return cfg


def test_import_dry_run_counts_messages(tmp_path: Path):
    msg = uavcan.node.Heartbeat_1_0(uptime=123)
    payload = bytes(pycyphal.dsdl.serialize(msg))
    root = make_hive_session(
        tmp_path,
        "3544BCD3",
        "00000509",
        files={
            "a.parquet": [
                make_transfer_row(source=42, payload=payload, subject_id=7509),
                make_transfer_row(
                    transfer_type="Request",
                    subject_id=430,
                    timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
                ),
            ],
        },
    )
    cfg = load(_write_config(tmp_path))
    runner = ImportRunner(cfg, roots=[root], dry_run=True)
    stats = runner.run()
    assert stats.sessions == 1
    assert stats.written == 1
    assert stats.skipped_non_message == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_import_app.py -v`
Expected: FAIL (module/class missing). Skip if `uavcan` not on PYTHONPATH — set `PYTHONPATH` to compiled DSDL root as in README.

- [ ] **Step 3: Implement import_app.py**

Create `src/pyric_static/import_app.py`:

```python
"""Batch import of CANedge transfer parquet into InfluxDB."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pycyphal.dsdl

from .config import Config, NodeMeta
from .influx import InfluxWriter
from .metrics import RunMetrics
from .transfers import (
    delete_stop_exclusive,
    discover_sessions,
    iter_transfer_batches,
    scan_session_time_range,
    timestamp_to_ns,
    trim_payload,
)

_logger = logging.getLogger(__name__)


@dataclass
class ImportStats:
    sessions: int = 0
    written: int = 0
    skipped_non_message: int = 0
    failed_sessions: int = 0
    metrics: RunMetrics = field(default_factory=RunMetrics)


class ImportRunner:
    def __init__(
        self,
        cfg: Config,
        *,
        roots: list[Path],
        dry_run: bool = False,
    ) -> None:
        self.cfg = cfg
        self.roots = roots
        self.dry_run = dry_run
        self.stats = ImportStats()
        self.writer: InfluxWriter | None = None

    def run(self) -> ImportStats:
        sessions = discover_sessions(self.roots)
        if not sessions:
            _logger.warning("no transfer parquet files found under %s", self.roots)
            return self.stats

        if not self.dry_run:
            self.writer = InfluxWriter.from_import(self.cfg)

        try:
            for (logger, session), files in sessions.items():
                try:
                    self._import_session(logger, session, files)
                    self.stats.sessions += 1
                except Exception:
                    self.stats.failed_sessions += 1
                    _logger.exception("session failed: logger=%s session=%s", logger, session)
                    return self.stats
        finally:
            if self.writer is not None:
                self.writer.close()

        if self.stats.failed_sessions:
            _logger.error("import finished with %d failed session(s)", self.stats.failed_sessions)
        else:
            _logger.info(
                "import done: sessions=%d written=%d skipped_non_message=%d",
                self.stats.sessions,
                self.stats.written,
                self.stats.skipped_non_message,
            )
        return self.stats

    def _import_session(self, logger: str, session: str, files: list[Path]) -> None:
        t_min, t_max = scan_session_time_range(files)
        t_stop = delete_stop_exclusive(t_max)

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

        for path in files:
            for batch in iter_transfer_batches(path):
                for row in batch.to_pylist():
                    self._handle_row(logger, session, row)

        if not self.dry_run and self.writer is not None:
            self.writer.writer.flush()  # type: ignore[union-attr]

        m = self.stats.metrics
        _logger.info(
            "session complete: logger=%s session=%s written=%d non_msg=%d unresolved=%d deserialize_failed=%d",
            logger,
            session,
            self.stats.written,
            self.stats.skipped_non_message,
            sum(m.unresolved_subject.values()),
            sum(m.deserialize_failed.values()),
        )

    def _handle_row(self, logger: str, session: str, row: dict[str, Any]) -> None:
        if row.get("type") != "Message":
            self.stats.skipped_non_message += 1
            return

        port_id = int(row["id"])
        src_raw = row.get("source")
        src = int(src_raw) if src_raw is not None else None

        if src is not None and src not in self.cfg.nodes:
            self.stats.metrics.note_unlisted_node(src)

        port_spec = self.cfg.resolve(src, port_id)
        if port_spec is None:
            self.stats.metrics.note_unresolved_subject(src, port_id)
            return

        payload = trim_payload(row.get("payload"), row.get("length"))
        try:
            message: Any = pycyphal.dsdl.deserialize(port_spec.dtype, payload)
        except Exception:
            self.stats.metrics.note_deserialize_failed(port_spec.type_str)
            _logger.exception("deserialize failed for %s on node %s", port_spec.type_str, src)
            return

        if message is None:
            self.stats.metrics.note_deserialize_failed(port_spec.type_str)
            return

        if self.dry_run:
            self.stats.written += 1
            return

        assert self.writer is not None
        channel = row.get("channel") or "unknown"
        node_meta: NodeMeta | None = self.cfg.nodes.get(src) if src is not None else None
        self.writer.write_message(
            spec=port_spec,
            source_node_id=src,
            message=message,
            timestamp_ns=timestamp_to_ns(row["timestamp"]),
            node_meta=node_meta,
            import_tags={"logger": logger, "session": session, "iface": str(channel)},
        )
        self.stats.written += 1
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_import_app.py -v`
Expected: PASS (1 test)

- [ ] **Step 5: Commit**

```bash
git add src/pyric_static/import_app.py tests/test_import_app.py
git commit -m "feat: ImportRunner with dry-run support for transfer parquet"
```

---

## Task 7: CLI import subcommand

**Files:**
- Modify: `src/pyric_static/cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Write failing CLI test**

Append to `tests/test_cli.py`:

```python
def test_main_import_dispatches(tmp_path: Path, monkeypatch):
    cfg = tmp_path / "c.toml"
    cfg.write_text("[influx]\nbucket = 'pyric'\n")
    hive = tmp_path / "logger=L" / "session=S"
    hive.mkdir(parents=True)

    called: dict = {}

    class FakeRunner:
        def __init__(self, _cfg, *, roots, dry_run):
            called["roots"] = roots
            called["dry_run"] = dry_run

        def run(self):
            class R:
                failed_sessions = 0

            return R()

    monkeypatch.setattr("pyric_static.cli.ImportRunner", FakeRunner)
    rc = main(["import", "--config", str(cfg), str(hive), "--dry-run"])
    assert rc == 0
    assert called["dry_run"] is True
    assert called["roots"] == [hive]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cli.py::test_main_import_dispatches -v`
Expected: FAIL

- [ ] **Step 3: Implement CLI dispatch**

Refactor `src/pyric_static/cli.py`:

1. Rename current `main` body to `live_main(argv) -> int`.
2. Add `import_main(argv) -> int`:

```python
def import_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pyric-static import")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("roots", nargs="+", type=Path)
    args = parser.parse_args(argv)
    _install_logging(args.log_level)
    try:
        from .import_app import ImportRunner

        cfg = load(args.config)
        stats = ImportRunner(cfg, roots=list(args.roots), dry_run=args.dry_run).run()
        return 1 if stats.failed_sessions else 0
    except Exception:
        logging.getLogger("pyric_static").exception("fatal error")
        return 1
```

3. Change top-level `main`:

```python
def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "import":
        return import_main(argv[1:])
    return live_main(argv)
```

4. Keep `live_main` identical to today's `main` logic (including `[logger]` required check).

- [ ] **Step 4: Run CLI tests**

Run: `uv run pytest tests/test_cli.py -v`
Expected: PASS (existing + new test). Note: tests calling `load()` still need DSDL on PYTHONPATH for `build_standard_ports()`.

- [ ] **Step 5: Commit**

```bash
git add src/pyric_static/cli.py tests/test_cli.py
git commit -m "feat: add pyric-static import subcommand"
```

---

## Task 8: README documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add import section to README**

Append after the live/replay **Run** section:

```markdown
## Import CANedge transfer parquet

Batch-upload pre-assembled transfer parquet from the frame-decoding pipeline hive layout.
Omit `[logger]` in the config — `logger`, `session`, and `iface` tags come from the hive
partitions and each transfer's `channel` field.

```bash
pyric-static import --config pyric-static.toml /mnt/data/transfers
pyric-static import --config pyric-static.toml /mnt/a /mnt/b --dry-run
```

Before uploading each `(logger, session)`, existing Influx points with matching tags in
the parquet time range are deleted. Only `Message` transfers are written (same as live mode).
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document pyric-static import subcommand"
```

---

## Task 9: Full test suite and lint

**Files:**
- (none new)

- [ ] **Step 1: Run full test suite**

Run: `cd /home/lasse/work/canedge/pyric-static && uv run pytest -v`
Expected: all tests PASS (DSDL types on `PYTHONPATH` as documented in README).

- [ ] **Step 2: Run ruff**

Run: `uv run ruff check src tests`
Expected: no errors.

- [ ] **Step 3: Fix any issues and commit if needed**

---

## Spec Coverage Checklist

| Spec requirement | Task |
| --- | --- |
| Vendored TransferSchema, no sc-schema dep | Task 2 |
| Hive glob `logger=*/session=*/*.parquet` | Task 4 |
| Positional multiple roots | Task 7 |
| Optional `[logger]` in config | Already done |
| Live requires `[logger]` | Already done |
| Messages only | Task 6 `_handle_row` |
| Tags logger/session/iface from data | Task 5, 6 |
| Scoped delete before upload | Task 5, 6 |
| `--dry-run` | Task 6, 7 |
| Sequential processing | Task 6 (single loop) |
| Schema mismatch fails session | Task 4 `assert_transfer_schema` |
| Exit code 1 on failure | Task 6, 7 |
| pyarrow dependency | Task 1 |
