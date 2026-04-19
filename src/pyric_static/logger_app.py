"""The high-level passive logger.

Wires: frame source -> CANTracer -> DSDL resolution -> deserialize ->
flatten -> Influx write.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import pycyphal.dsdl
from pycyphal.transport import MessageDataSpecifier, TransferTrace

from .config import Config
from .influx import InfluxWriter
from .metrics import RunMetrics
from .reassembly import TracerLoop
from .sources import LiveSource, ReplaySource, iter_messages

_logger = logging.getLogger(__name__)


@dataclass
class LoggerStats:
    written: int = 0
    skipped_non_message: int = 0
    last_log_time: float = field(default_factory=time.monotonic)
    metrics: RunMetrics = field(default_factory=RunMetrics)

    def maybe_log(self, tracer_frames: int, tracer_transfers: int, written: int) -> None:
        now = time.monotonic()
        if now - self.last_log_time < 5.0:
            return
        self.last_log_time = now
        m = self.metrics
        n_unres = sum(m.unresolved_subject.values())
        n_unlist = sum(m.unlisted_node.values())
        n_dfail = sum(m.deserialize_failed.values())
        _logger.info(
            "frames=%d transfers=%d written=%d non_msg=%d | "
            "missing_type_mapping=%d unlisted_node_tx=%d deserialize_failed=%d",
            tracer_frames,
            tracer_transfers,
            written,
            self.skipped_non_message,
            n_unres,
            n_unlist,
            n_dfail,
        )


class PassiveLogger:
    def __init__(
        self,
        cfg: Config,
        source: LiveSource | ReplaySource,
        *,
        dry_run: bool = False,
    ) -> None:
        self.cfg = cfg
        self.source = source
        self.dry_run = dry_run
        self.tracer = TracerLoop()
        self.stats = LoggerStats()
        self.writer: InfluxWriter | None = None

    def _ensure_writer(self) -> InfluxWriter:
        if self.writer is None:
            self.writer = InfluxWriter.from_config(self.cfg)
        return self.writer

    def run(self) -> None:
        if self.dry_run:
            _logger.info("dry-run: frames will be decoded but not written to Influx")
        else:
            self._ensure_writer()

        try:
            for msg in iter_messages(self.source):
                trace = self.tracer.feed(msg)
                if isinstance(trace, TransferTrace):
                    self._handle_transfer(trace)
                self.stats.maybe_log(
                    self.tracer.stats.frames,
                    self.tracer.stats.transfers,
                    self.stats.written,
                )
        except KeyboardInterrupt:
            _logger.info("interrupted")
        finally:
            m = self.stats.metrics
            _logger.info(
                "done: frames=%d transfers=%d written=%d non_msg=%d can_err=%s",
                self.tracer.stats.frames,
                self.tracer.stats.transfers,
                self.stats.written,
                self.stats.skipped_non_message,
                self.tracer.stats.errors,
            )
            for line in m.summary_lines():
                _logger.info("metrics: %s", line)
            if self.writer is not None:
                self.writer.close()

    def _handle_transfer(self, trace: TransferTrace) -> None:
        transfer = trace.transfer
        spec = transfer.metadata.session_specifier
        ds = spec.data_specifier
        if not isinstance(ds, MessageDataSpecifier):
            self.stats.skipped_non_message += 1
            return

        port_id = ds.subject_id
        src = spec.source_node_id
        if src is not None and src not in self.cfg.nodes:
            self.stats.metrics.note_unlisted_node(src)

        port_spec = self.cfg.resolve(src, port_id)
        if port_spec is None:
            self.stats.metrics.note_unresolved_subject(src, port_id)
            return

        try:
            message: Any = pycyphal.dsdl.deserialize(port_spec.dtype, transfer.fragmented_payload)
        except Exception:  # noqa: BLE001 - log and count, keep running
            self.stats.metrics.note_deserialize_failed(port_spec.type_str)
            _logger.exception("deserialize failed for %s on node %s", port_spec.type_str, src)
            return

        if message is None:
            self.stats.metrics.note_deserialize_failed(port_spec.type_str)
            return

        if self.dry_run:
            self.stats.written += 1
            return

        assert self.writer is not None
        node_meta = self.cfg.nodes.get(src) if src is not None else None
        try:
            self.writer.write_message(
                spec=port_spec,
                source_node_id=src,
                message=message,
                timestamp_ns=trace.timestamp.system_ns,
                node_meta=node_meta,
            )
            self.stats.written += 1
        except Exception:  # noqa: BLE001
            _logger.exception("Influx write failed for %s", port_spec.type_str)
