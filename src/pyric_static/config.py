"""Configuration model and TOML loader.

The on-disk layout is node-centric: each ``[[nodes]]`` block has optional
metadata and zero or more ``[[nodes.ports]]`` entries for vendor/non-standard
subjects. Standard fixed-subject messages (heartbeat, time sync, ...) are
implicit and do not appear in the file.

Frame source (replay path or live bus) is configured on the CLI, not here.

The optional ``[logger]`` section supplies Influx ``logger`` / ``iface`` default tags
for live and replay mode. Batch import mode omits it and derives those tags from
transfer hive metadata instead.
"""

from __future__ import annotations

import logging
import platform
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .dsdl import PortSpec, build_standard_ports, resolve_type

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoggerSection:
    name: str
    iface: str


@dataclass(frozen=True)
class InfluxSection:
    bucket: str = "pyric"


@dataclass(frozen=True)
class NodeMeta:
    """Tag-only metadata for Influx ``app_name`` / ``device_uid``."""

    name: str | None
    uid: str | None


@dataclass
class Config:
    logger: LoggerSection | None
    influx: InfluxSection
    nodes: dict[int, NodeMeta]
    # Explicit (node_id, port_id) entries from [[nodes.ports]].
    explicit_ports: dict[tuple[int, int], PortSpec]
    # Implicit fixed-subject ports, keyed by port_id (pyric static_subjects).
    standard_ports: dict[int, PortSpec]

    def resolve(self, source_node_id: int | None, port_id: int) -> PortSpec | None:
        """Return the PortSpec for a transfer, or ``None`` if unknown.

        Resolution order:
        1. Explicit ``(source_node_id, port_id)`` — per-node override.
        2. Implicit standard port by ``port_id`` alone.
        """

        if source_node_id is not None:
            found = self.explicit_ports.get((source_node_id, port_id))
            if found is not None:
                return found
        return self.standard_ports.get(port_id)


def _parse_logger(raw: dict[str, Any]) -> LoggerSection | None:
    if "logger" not in raw:
        return None
    lg = raw["logger"] or {}
    name = lg.get("name") or f"pyric-{platform.node()}"
    iface = lg.get("iface") or "unknown"
    return LoggerSection(name=name, iface=iface)


def _parse_influx(raw: dict[str, Any]) -> InfluxSection:
    ix = raw.get("influx") or {}
    return InfluxSection(bucket=ix.get("bucket", "pyric"))


def _parse_nodes(
    raw: dict[str, Any],
) -> tuple[dict[int, NodeMeta], dict[tuple[int, int], PortSpec]]:
    nodes_raw = raw.get("nodes") or []
    nodes: dict[int, NodeMeta] = {}
    explicit: dict[tuple[int, int], PortSpec] = {}
    for entry in nodes_raw:
        node_id = entry.get("id")
        if not isinstance(node_id, int):
            raise ValueError(f"[[nodes]] entry missing integer 'id': {entry!r}")
        if node_id in nodes:
            raise ValueError(f"duplicate node id in config: {node_id}")
        nodes[node_id] = NodeMeta(name=entry.get("name"), uid=entry.get("uid"))

        for port in entry.get("ports", []) or []:
            pid = port.get("id")
            pname = port.get("name")
            ptype = port.get("type")
            if not isinstance(pid, int) or not pname or not ptype:
                raise ValueError(f"[[nodes.ports]] needs id/name/type: {port!r} (node {node_id})")
            key = (node_id, pid)
            if key in explicit:
                raise ValueError(f"duplicate port in config for node {node_id} port_id {pid}: {port!r}")
            try:
                dtype = resolve_type(ptype)
            except (ValueError, ImportError, AttributeError) as exc:
                _logger.warning(
                    "skipping [[nodes.ports]] on node %s: id=%s name=%r type=%r (%s)",
                    node_id,
                    pid,
                    pname,
                    ptype,
                    exc,
                )
                continue
            explicit[key] = PortSpec(port_id=pid, port_name=pname, type_str=ptype, dtype=dtype)
    return nodes, explicit


def load(path: Path | str) -> Config:
    raw = tomllib.loads(Path(path).read_text())
    if "source" in raw:
        raise ValueError(
            "obsolete [source] section: configure replay or live bus on the command line "
            "(--replay FILE or --interface / --channel)"
        )
    logger = _parse_logger(raw)
    influx = _parse_influx(raw)
    nodes, explicit = _parse_nodes(raw)
    standard = build_standard_ports()
    return Config(
        logger=logger,
        influx=influx,
        nodes=nodes,
        explicit_ports=explicit,
        standard_ports=standard,
    )
