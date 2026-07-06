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
        yield InfluxWriter.from_import(cfg), writer_api, client.delete_api.return_value


def test_from_import_sets_no_default_tags(import_writer):
    writer, _write_api, _delete_api = import_writer
    assert writer.client.default_tags == {}


def test_delete_range_calls_delete_api(import_writer):
    writer, _write_api, delete_api = import_writer
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    stop = datetime(2026, 1, 2, tzinfo=timezone.utc)
    writer.delete_range(logger="3544BCD3", session="00000509", start=start, stop=stop)
    delete_api.delete.assert_called_once()
    kwargs = delete_api.delete.call_args.kwargs
    assert kwargs["predicate"] == 'logger="3544BCD3" AND session="00000509"'
    assert kwargs["bucket"] == "pyric"
    assert kwargs["org"] == "org"


def test_write_message_includes_import_tags(import_writer):
    writer, write_api, _delete_api = import_writer
    spec = PortSpec(port_id=7509, port_name="heartbeat", type_str="uavcan.node.Heartbeat.1.0", dtype=object)
    with patch("pyric_static.influx.pycyphal.dsdl.to_builtin", return_value={"uptime": 1}):
        with patch("pyric_static.influx.flatten", return_value={"uptime": 1}):
            writer.write_message(
                spec=spec,
                source_node_id=42,
                message=object(),
                timestamp_ns=1_700_000_000_000_000_000,
                node_meta=None,
                import_tags={"logger": "L", "session": "S", "iface": "CAN1"},
            )
    write_api.write.assert_called_once()
    record = write_api.write.call_args.kwargs["record"]
    assert record._tags["logger"] == "L"
    assert record._tags["session"] == "S"
    assert record._tags["iface"] == "CAN1"
