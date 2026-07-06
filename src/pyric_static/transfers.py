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
    logger: str | None = None
    session: str | None = None
    for part in path.resolve().parents:
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
    patterns: list[Path] = []
    patterns.extend(root.glob("logger=*/session=*/*.parquet"))
    if _SESSION_RE.match(root.name):
        patterns.extend(root.glob("*.parquet"))
    if _LOGGER_RE.match(root.name):
        patterns.extend(root.glob("session=*/*.parquet"))
    found: dict[Path, Path] = {}
    for path in patterns:
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
    return {key: sorted(files.values()) for key, files in sorted(grouped.items(), key=lambda kv: kv[0])}


def assert_transfer_schema(path: Path) -> None:
    schema = pq.read_schema(path)
    if not schema.equals(TransferSchema):
        raise ValueError(
            f"{path}: transfer parquet schema mismatch\nexpected: {TransferSchema}\nactual:   {schema}"
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
        batch_min = pc.min(col).as_py()  # ty: ignore[unresolved-attribute]
        batch_max = pc.max(col).as_py()  # ty: ignore[unresolved-attribute]
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
    yield from pq.ParquetFile(path).iter_batches(batch_size=batch_size)


def timestamp_to_ns(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000_000)


def trim_payload(payload: bytes | None, length: int | None) -> bytes:
    if not payload:
        return b""
    if length is None or length < 0:
        return bytes(payload)
    return bytes(payload[:length])
