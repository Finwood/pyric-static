"""Batch import of CANedge transfer parquet into InfluxDB."""

from __future__ import annotations

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pycyphal.dsdl

from .config import Config, NodeMeta, load
from .influx import InfluxWriter
from .metrics import RunMetrics
from .transfers import (
    discover_sessions,
    filter_session_files,
    iter_transfer_batches,
    scan_session_time_range,
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


@dataclass(frozen=True)
class SessionJob:
    config_path: Path
    logger: str
    session: str
    files: tuple[Path, ...]
    dry_run: bool
    start: datetime | None
    stop: datetime | None


@dataclass
class SessionResult:
    logger: str = ""
    session: str = ""
    written: int = 0
    skipped_non_message: int = 0
    skipped: bool = False
    failed: bool = False
    metrics: RunMetrics = field(default_factory=RunMetrics)


def _handle_row(
    cfg: Config,
    metrics: RunMetrics,
    writer: InfluxWriter | None,
    *,
    logger: str,
    session: str,
    row: dict[str, Any],
    dry_run: bool,
) -> tuple[int, int]:
    if row.get("type") != "Message":
        return 0, 1

    port_id = int(row["id"])
    src_raw = row.get("source")
    src = int(src_raw) if src_raw is not None else None

    if src is not None and src not in cfg.nodes:
        metrics.note_unlisted_node(src)

    port_spec = cfg.resolve(src, port_id)
    if port_spec is None:
        metrics.note_unresolved_subject(src, port_id)
        return 0, 0

    payload = trim_payload(row.get("payload"), row.get("length"))
    try:
        message: Any = pycyphal.dsdl.deserialize(port_spec.dtype, [memoryview(payload)])
    except Exception:
        metrics.note_deserialize_failed(port_spec.type_str)
        _logger.exception("deserialize failed for %s on node %s", port_spec.type_str, src)
        return 0, 0

    if message is None:
        metrics.note_deserialize_failed(port_spec.type_str)
        return 0, 0

    if dry_run:
        return 1, 0

    assert writer is not None
    channel = row.get("channel") or "unknown"
    node_meta: NodeMeta | None = cfg.nodes.get(src) if src is not None else None
    writer.write_message(
        spec=port_spec,
        source_node_id=src,
        message=message,
        timestamp_ns=row["timestamp"] * 1000,
        node_meta=node_meta,
        import_tags={"logger": logger, "session": session, "iface": str(channel)},
    )
    return 1, 0


def _import_session(
    cfg: Config,
    *,
    logger: str,
    session: str,
    files: list[Path],
    dry_run: bool,
    start: datetime | None,
    stop: datetime | None,
    writer: InfluxWriter | None,
) -> SessionResult:
    result = SessionResult(logger=logger, session=session)

    if start is not None and stop is not None:
        files = filter_session_files(files, start, stop)
        if not files:
            _logger.info(
                "skip session (no files overlap window): logger=%s session=%s start=%s stop=%s",
                logger,
                session,
                start.isoformat(),
                stop.isoformat(),
            )
            result.skipped = True
            return result
        t_min, t_max = start, stop
    else:
        t_min, t_max = None, None

    if dry_run:
        if t_min is None or t_max is None:
            t_min, t_max = scan_session_time_range(files)
        _logger.info(
            "dry-run: logger=%s session=%s files=%d range=%s..%s",
            logger,
            session,
            len(files),
            t_min.isoformat(),
            t_max.isoformat(),
        )

    read_kwargs: dict = {}
    if start is not None and stop is not None:
        read_kwargs = {"start": start, "stop": stop}

    for path in files:
        for batch in iter_transfer_batches(path, **read_kwargs):
            batch = batch.set_column(
                batch.schema.get_field_index("timestamp"),
                "timestamp",
                batch.column("timestamp").cast(pa.int64()),
            )
            for row in batch.to_pylist():
                written, skipped = _handle_row(
                    cfg,
                    result.metrics,
                    writer,
                    logger=logger,
                    session=session,
                    row=row,
                    dry_run=dry_run,
                )
                result.written += written
                result.skipped_non_message += skipped

    _logger.info(
        "session complete: logger=%s session=%s written=%d non_msg=%d unresolved=%d deserialize_failed=%d",
        logger,
        session,
        result.written,
        result.skipped_non_message,
        sum(result.metrics.unresolved_subject.values()),
        sum(result.metrics.deserialize_failed.values()),
    )
    return result


def _import_session_worker(job: SessionJob) -> SessionResult:
    cfg = load(job.config_path)
    writer: InfluxWriter | None = None
    try:
        if not job.dry_run:
            writer = InfluxWriter.from_import(cfg)
        return _import_session(
            cfg,
            logger=job.logger,
            session=job.session,
            files=list(job.files),
            dry_run=job.dry_run,
            start=job.start,
            stop=job.stop,
            writer=writer,
        )
    except Exception:
        _logger.exception("session failed: logger=%s session=%s", job.logger, job.session)
        return SessionResult(logger=job.logger, session=job.session, failed=True)
    finally:
        if writer is not None:
            writer.close()


class ImportRunner:
    def __init__(
        self,
        cfg: Config,
        *,
        roots: list[Path],
        config_path: Path,
        dry_run: bool = False,
        start: datetime | None = None,
        stop: datetime | None = None,
        jobs: int = 1,
    ) -> None:
        if (start is None) != (stop is None):
            raise ValueError("start and stop must both be set or both omitted")
        if start is not None and stop is not None and start >= stop:
            raise ValueError("start must be before stop")
        if jobs < 1:
            raise ValueError("jobs must be >= 1")
        self.cfg = cfg
        self.roots = roots
        self.config_path = config_path
        self.dry_run = dry_run
        self.start = start
        self.stop = stop
        self.jobs = jobs
        self.stats = ImportStats()

    def run(self) -> ImportStats:
        sessions = discover_sessions(self.roots)
        if not sessions:
            _logger.warning("no transfer parquet files found under %s", self.roots)
            return self.stats

        jobs = [
            SessionJob(
                config_path=self.config_path,
                logger=logger,
                session=session,
                files=tuple(files),
                dry_run=self.dry_run,
                start=self.start,
                stop=self.stop,
            )
            for (logger, session), files in sessions.items()
        ]

        if self.jobs == 1:
            for job in jobs:
                self._merge_result(_run_session_job_in_process(job))
        else:
            with ProcessPoolExecutor(max_workers=self.jobs) as pool:
                futures = {pool.submit(_import_session_worker, job): job for job in jobs}
                for future in as_completed(futures):
                    self._merge_result(future.result())

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

    def _merge_result(self, result: SessionResult) -> None:
        if result.failed:
            self.stats.failed_sessions += 1
            return
        if result.skipped:
            return
        self.stats.sessions += 1
        self.stats.written += result.written
        self.stats.skipped_non_message += result.skipped_non_message
        self.stats.metrics.merge(result.metrics)


def _run_session_job_in_process(job: SessionJob) -> SessionResult:
    cfg = load(job.config_path)
    writer: InfluxWriter | None = None
    try:
        if not job.dry_run:
            writer = InfluxWriter.from_import(cfg)
        return _import_session(
            cfg,
            logger=job.logger,
            session=job.session,
            files=list(job.files),
            dry_run=job.dry_run,
            start=job.start,
            stop=job.stop,
            writer=writer,
        )
    except Exception:
        _logger.exception("session failed: logger=%s session=%s", job.logger, job.session)
        return SessionResult(logger=job.logger, session=job.session, failed=True)
    finally:
        if writer is not None:
            writer.close()
