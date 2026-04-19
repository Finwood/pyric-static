"""DSDL type resolution.

Maps a Cyphal port type string (``namespace.ShortName.major.minor``) to the
corresponding compiled Python class and provides the table of implicit
standard fixed-subject messages that the logger always recognizes.
"""

from __future__ import annotations

import importlib
import logging
import re
from dataclasses import dataclass
from typing import Any

_logger = logging.getLogger(__name__)

_TYPE_RE = re.compile(
    r"(?P<namespace>[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*)"
    r"\.(?P<shortname>[a-zA-Z_][a-zA-Z0-9_]*)"
    r"\.(?P<major>\d+)"
    r"\.(?P<minor>\d+)"
)


@dataclass(frozen=True)
class PortSpec:
    """Everything needed to decode a message and label it for Influx."""

    port_id: int
    port_name: str
    type_str: str
    dtype: type[Any]


def resolve_type(type_str: str) -> type[Any]:
    """Resolve a Cyphal type string to its compiled Python class.

    Uses the same versioning rule as pyric's ``NodeExplorer``:
    - major > 0  -> ``ShortName_Major`` (latest minor)
    - major == 0 -> ``ShortName_Major_Minor`` (exact version)
    """

    match = _TYPE_RE.fullmatch(type_str)
    if not match:
        raise ValueError(f"invalid Cyphal type string: {type_str!r}")
    namespace = match.group("namespace")
    shortname = match.group("shortname")
    major = match.group("major")
    minor = match.group("minor")
    py_name = f"{shortname}_{major}" if int(major) > 0 else f"{shortname}_{major}_{minor}"
    module = importlib.import_module(namespace)
    try:
        return getattr(module, py_name)
    except AttributeError as exc:
        raise AttributeError(f"{namespace} has no attribute {py_name} for type {type_str!r}") from exc


def build_standard_ports() -> dict[int, PortSpec]:
    """Return implicit fixed-subject ports matching pyric's ``static_subjects``.

    Currently includes Heartbeat and Time Synchronization using each DSDL type's
    ``_FIXED_PORT_ID_``.  Additional standard fixed-subject messages can be
    added here without touching user config.
    """

    import uavcan.node
    import uavcan.time

    out: dict[int, PortSpec] = {}
    for port_name, dtype, type_str in (
        ("heartbeat", uavcan.node.Heartbeat_1_0, "uavcan.node.Heartbeat.1.0"),
        ("time_synchronization", uavcan.time.Synchronization_1_0, "uavcan.time.Synchronization.1.0"),
    ):
        port_id = dtype._FIXED_PORT_ID_
        out[port_id] = PortSpec(port_id=port_id, port_name=port_name, type_str=type_str, dtype=dtype)
    return out
