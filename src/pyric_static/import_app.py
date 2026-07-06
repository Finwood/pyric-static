"""Batch import of CANedge transfer parquet into InfluxDB."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pycyphal.dsdl

from .config import Config, NodeMeta
from .influx import InfluxWriter
from .metrics import RunMetrics
from .transfers import (
    delete_stop_exclusive,
    discover_sessions,
    iter_transfer_batches,
    scan_session_time_range,
    timestamp_to_ns,
    trim_payload,
)

_logger = logging.getLogger(__name__)


@dataclass
class ImportStats:
    sessions: int = 0
    written: int = 0
    skipped_non_message: int = 0
    failed_sessions: int = 0
    metrics: RunMetrics = field(default_factory=RunMetrics)


class ImportRunner:
    def __init__(
        self,
        cfg: Config,
        *,
        roots: list[Path],
        dry_run: bool = False,
    ) -> None:
        self.cfg = cfg
        self.roots = roots
        self.dry_run = dry_run
        self.stats = ImportStats()
        self.writer: InfluxWriter | None = None

    def run(self) -> ImportStats:
        sessions = discover_sessions(self.roots)
        if not sessions:
            _logger.warning("no transfer parquet files found under %s", self.roots)
            return self.stats

        if not self.dry_run:
            self.writer = InfluxWriter.from_import(self.cfg)

        try:
            for (logger, session), files in sessions.items():
                try:
                    self._import_session(logger, session, files)
                    self.stats.sessions += 1
                except Exception:
                    self.stats.failed_sessions += 1
                    _logger.exception("session failed: logger=%s session=%s", logger, session)
                    return self.stats
        finally:
            if self.writer is not None:
                self.writer.close()

        if self.stats.failed_sessions:
            _logger.error("import finished with %d failed session(s)", self.stats.failed_sessions)
        else:
            _logger.info(
                "import done: sessions=%d written=%d skipped_non_message=%d",
                self.stats.sessions,
                self.stats.written,
                self.stats.skipped_non_message,
            )
        return self.stats

    def _import_session(self, logger: str, session: str, files: list[Path]) -> None:
        t_min, t_max = scan_session_time_range(files)
        t_stop = delete_stop_exclusive(t_max)
        session_written = 0
        session_skipped = 0

        if self.dry_run:
            _logger.info(
                "dry-run: logger=%s session=%s files=%d range=%s..%s",
                logger,
                session,
                len(files),
                t_min.isoformat(),
                t_max.isoformat(),
            )
        else:
            assert self.writer is not None
            self.writer.delete_range(logger=logger, session=session, start=t_min, stop=t_stop)

        for path in files:
            for batch in iter_transfer_batches(path):
                for row in batch.to_pylist():
                    written, skipped = self._handle_row(logger, session, row)
                    session_written += written
                    session_skipped += skipped

        self.stats.written += session_written
        self.stats.skipped_non_message += session_skipped

        m = self.stats.metrics
        _logger.info(
            "session complete: logger=%s session=%s written=%d non_msg=%d unresolved=%d deserialize_failed=%d",
            logger,
            session,
            session_written,
            session_skipped,
            sum(m.unresolved_subject.values()),
            sum(m.deserialize_failed.values()),
        )

    def _handle_row(self, logger: str, session: str, row: dict[str, Any]) -> tuple[int, int]:
        if row.get("type") != "Message":
            return 0, 1

        port_id = int(row["id"])
        src_raw = row.get("source")
        src = int(src_raw) if src_raw is not None else None

        if src is not None and src not in self.cfg.nodes:
            self.stats.metrics.note_unlisted_node(src)

        port_spec = self.cfg.resolve(src, port_id)
        if port_spec is None:
            self.stats.metrics.note_unresolved_subject(src, port_id)
            return 0, 0

        payload = trim_payload(row.get("payload"), row.get("length"))
        try:
            message: Any = pycyphal.dsdl.deserialize(port_spec.dtype, payload)
        except Exception:
            self.stats.metrics.note_deserialize_failed(port_spec.type_str)
            _logger.exception("deserialize failed for %s on node %s", port_spec.type_str, src)
            return 0, 0

        if message is None:
            self.stats.metrics.note_deserialize_failed(port_spec.type_str)
            return 0, 0

        if self.dry_run:
            return 1, 0

        assert self.writer is not None
        channel = row.get("channel") or "unknown"
        node_meta: NodeMeta | None = self.cfg.nodes.get(src) if src is not None else None
        self.writer.write_message(
            spec=port_spec,
            source_node_id=src,
            message=message,
            timestamp_ns=timestamp_to_ns(row["timestamp"]),
            node_meta=node_meta,
            import_tags={"logger": logger, "session": session, "iface": str(channel)},
        )
        return 1, 0
