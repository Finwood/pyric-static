"""InfluxDB writer that mirrors pyric's ``NodeExplorer`` schema.

- measurement = DSDL type string
- tags: node_id, port_id, port_name, optional app_name/device_uid, default tags logger/iface
- fields: flatten(to_builtin(msg)) with bool -> int
- timestamp: nanoseconds
- WriteOptions: batch_size=1000, flush_interval=4000, jitter_interval=2000
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, cast

import pycyphal.dsdl
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from influxdb_client.client.write_api import WriteApi

from .config import Config, NodeMeta
from .dsdl import PortSpec
from .flatten import flatten

_logger = logging.getLogger(__name__)


@dataclass
class InfluxWriter:
    bucket: str
    client: InfluxDBClient
    writer: WriteApi

    @classmethod
    def from_config(cls, cfg: Config) -> "InfluxWriter":
        client = InfluxDBClient.from_env_properties()
        if client.default_tags is None:
            client.default_tags = {}
        assert isinstance(client.default_tags, dict)
        client.default_tags |= {
            "logger": cfg.logger.name,
            "iface": cfg.logger.iface,
        }
        writer = client.write_api(
            write_options=WriteOptions(batch_size=1000, flush_interval=4000, jitter_interval=2000)
        )
        _logger.info(
            "Influx writer ready: url=%s org=%s bucket=%s tags=%s",
            client.url,
            client.org,
            cfg.influx.bucket,
            client.default_tags,
        )
        return cls(bucket=cfg.influx.bucket, client=client, writer=writer)

    def close(self) -> None:
        try:
            self.writer.close()
        finally:
            self.client.close()

    def __enter__(self) -> "InfluxWriter":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def write_message(
        self,
        spec: PortSpec,
        source_node_id: int | None,
        message: Any,
        timestamp_ns: int,
        node_meta: NodeMeta | None,
    ) -> None:
        tags: dict[str, Any] = {
            "node_id": source_node_id if source_node_id is not None else "anonymous",
            "port_id": spec.port_id,
            "port_name": spec.port_name,
        }
        if node_meta is not None:
            if node_meta.name:
                tags["app_name"] = node_meta.name
            if node_meta.uid:
                tags["device_uid"] = node_meta.uid

        builtin = pycyphal.dsdl.to_builtin(message)
        fields = {
            key: (int(val) if isinstance(val, bool) else val)
            for key, val in flatten(builtin).items()
            if val is not None
        }
        if not fields:
            return

        point = Point.from_dict(
            {
                "measurement": spec.type_str,
                "tags": tags,
                "fields": fields,
                "time": int(timestamp_ns),
            },
            write_precision=cast(WritePrecision, WritePrecision.NS),
        )
        self.writer.write(self.bucket, record=point)
