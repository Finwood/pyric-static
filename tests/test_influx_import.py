from __future__ import annotations

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
        client.conf = MagicMock(timeout=10_000)
        mock_cls.from_env_properties.return_value = client
        writer_api = MagicMock()
        client.write_api.return_value = writer_api
        cfg = Config(
            logger=None,
            influx=InfluxSection(bucket="pyric"),
            nodes={},
            explicit_ports={},
            standard_ports={},
        )
        yield InfluxWriter.from_import(cfg), writer_api


def test_from_import_sets_no_default_tags(import_writer):
    writer, _write_api = import_writer
    assert writer.client.default_tags == {}


def test_from_import_uses_extended_timeout(import_writer):
    writer, _write_api = import_writer
    assert writer.client.conf.timeout == 120_000


def test_write_message_includes_import_tags(import_writer):
    writer, write_api = import_writer
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
