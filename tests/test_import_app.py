from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

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
