import pytest

from pyric_static.dsdl import build_standard_ports, resolve_type


def test_resolve_major_gt_zero_uses_latest_minor():
    pytest.importorskip("uavcan.node")
    import uavcan.node

    cls = resolve_type("uavcan.node.Heartbeat.1.0")
    assert cls is uavcan.node.Heartbeat_1


def test_resolve_major_zero_uses_exact_minor():
    pytest.importorskip("uavcan.time")
    import uavcan.time

    cls = resolve_type("uavcan.time.Synchronization.1.0")
    assert cls is uavcan.time.Synchronization_1_0


def test_resolve_invalid_type_raises():
    with pytest.raises(ValueError):
        resolve_type("not_a_type")


def test_standard_ports_include_heartbeat_and_time_sync():
    pytest.importorskip("uavcan.node")
    ports = build_standard_ports()
    assert 7509 in ports
    assert ports[7509].type_str == "uavcan.node.Heartbeat.1.0"
    time_sync = {p.port_name for p in ports.values()}
    assert "time_synchronization" in time_sync
