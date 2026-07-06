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
