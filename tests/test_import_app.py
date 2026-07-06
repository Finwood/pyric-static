from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from pyric_static.config import Config, InfluxSection
from pyric_static.dsdl import PortSpec
from pyric_static.import_app import ImportRunner
from tests.transfer_fixtures import make_hive_session, make_transfer_row


def test_import_dry_run_counts_messages(tmp_path: Path, monkeypatch):
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
                make_transfer_row(source=42, payload=b"\x00" * 7, subject_id=7509),
                make_transfer_row(
                    transfer_type="Request",
                    subject_id=430,
                    timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
                ),
            ],
        },
    )
    stats = ImportRunner(cfg, roots=[root], dry_run=True).run()
    assert stats.sessions == 1
    assert stats.written == 1
    assert stats.skipped_non_message == 1


def test_import_filtered_dry_run_skips_outside_window(tmp_path: Path, monkeypatch):
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


def test_import_filtered_calls_delete_with_window(tmp_path: Path, monkeypatch):
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
