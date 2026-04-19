from pathlib import Path

import pytest

from pyric_static.config import Config, load


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "c.toml"
    p.write_text(body)
    return p


def test_load_explicit_ports(tmp_path: Path):
    p = _write(
        tmp_path,
        """
[logger]
name = "t"
iface = "can0"

[[nodes]]
id = 11
name = "foo"
uid = "ab"

[[nodes.ports]]
id = 113
name = "analog_inputs"
type = "uavcan.node.Heartbeat.1.0"
""",
    )
    cfg = load(p)
    assert cfg.nodes[11].name == "foo"
    assert (11, 113) in cfg.explicit_ports


def test_same_port_id_on_two_nodes_is_ok(tmp_path: Path):
    p = _write(
        tmp_path,
        """
[logger]
name = "t"
iface = "can0"

[[nodes]]
id = 11
[[nodes.ports]]
id = 100
name = "a"
type = "uavcan.node.Heartbeat.1.0"

[[nodes]]
id = 12
[[nodes.ports]]
id = 100
name = "b"
type = "uavcan.node.Heartbeat.1.0"
""",
    )
    cfg = load(p)
    assert (11, 100) in cfg.explicit_ports
    assert (12, 100) in cfg.explicit_ports
    assert cfg.explicit_ports[(11, 100)].port_name == "a"
    assert cfg.explicit_ports[(12, 100)].port_name == "b"


def test_duplicate_explicit_pair_errors(tmp_path: Path):
    p = _write(
        tmp_path,
        """
[logger]
name = "t"
iface = "can0"
[[nodes]]
id = 11
[[nodes.ports]]
id = 100
name = "a"
type = "uavcan.node.Heartbeat.1.0"
[[nodes.ports]]
id = 100
name = "b"
type = "uavcan.node.Heartbeat.1.0"
""",
    )
    with pytest.raises(ValueError):
        load(p)


def test_resolve_falls_back_to_implicit_standard(tmp_path: Path):
    p = _write(
        tmp_path,
        """
[logger]
name = "t"
iface = "can0"
""",
    )
    cfg: Config = load(p)
    spec = cfg.resolve(12, 7509)
    assert spec is not None
    assert spec.type_str == "uavcan.node.Heartbeat.1.0"
    assert cfg.resolve(12, 999) is None


def test_missing_dsdl_type_skips_port_but_loads(tmp_path: Path):
    p = _write(
        tmp_path,
        """
[logger]
name = "t"
iface = "can0"
[[nodes]]
id = 10
[[nodes.ports]]
id = 115
name = "bad"
type = "b17.NoSuchType.9.9"
[[nodes.ports]]
id = 116
name = "good"
type = "uavcan.node.Heartbeat.1.0"
""",
    )
    cfg = load(p)
    assert (10, 115) not in cfg.explicit_ports
    assert (10, 116) in cfg.explicit_ports


def test_obsolete_source_section_errors(tmp_path: Path):
    p = _write(
        tmp_path,
        """
[logger]
name = "t"
iface = "can0"
[source.replay]
path = "x.log"
""",
    )
    with pytest.raises(ValueError, match="obsolete"):
        load(p)
