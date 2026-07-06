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
from datetime import datetime
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
        if cfg.logger is None:
            raise ValueError("InfluxWriter.from_config requires a [logger] section")
        client = InfluxDBClient.from_env_properties()
        if client.default_tags is None:
            client.default_tags = {}
        assert isinstance(client.default_tags, dict)
        logger = cfg.logger
        client.default_tags |= {
            "logger": logger.name,
            "iface": logger.iface,
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

    @classmethod
    def from_import(cls, cfg: Config) -> "InfluxWriter":
        client = InfluxDBClient.from_env_properties()
        if client.default_tags is None:
            client.default_tags = {}
        writer = client.write_api(
            write_options=WriteOptions(batch_size=1000, flush_interval=4000, jitter_interval=2000)
        )
        _logger.info(
            "Influx import writer ready: url=%s org=%s bucket=%s",
            client.url,
            client.org,
            cfg.influx.bucket,
        )
        return cls(bucket=cfg.influx.bucket, client=client, writer=writer)

    def delete_range(
        self,
        *,
        logger: str,
        session: str,
        start: datetime,
        stop: datetime,
    ) -> None:
        predicate = f'logger="{logger}" AND session="{session}"'
        self.client.delete_api().delete(
            start=start,
            stop=stop,
            predicate=predicate,
            bucket=self.bucket,
            org=self.client.org,
        )
        _logger.info(
            "Influx delete: logger=%s session=%s start=%s stop=%s",
            logger,
            session,
            start.isoformat(),
            stop.isoformat(),
        )

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
        *,
        import_tags: dict[str, str] | None = None,
    ) -> None:
        tags: dict[str, Any] = {
            "node_id": source_node_id if source_node_id is not None else "anonymous",
            "port_id": spec.port_id,
            "port_name": spec.port_name,
        }
        if import_tags is not None:
            tags.update(import_tags)
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
