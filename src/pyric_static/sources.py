"""Frame producers for live and recorded CAN.

Both modes yield ``can.Message`` so the downstream pipeline has a single path.

Source selection is **not** part of the TOML config; it comes from CLI flags
(``--replay`` or ``--interface`` / ``--channel``).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import can

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveSource:
    interface: str
    channel: str
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReplaySource:
    path: Path


def _iter_replay(path: Path) -> Iterator[can.Message]:
    with can.LogReader(str(path)) as reader:
        for msg in reader:
            yield msg


@contextmanager
def _open_bus(src: LiveSource) -> Iterator[can.BusABC]:
    kwargs: dict[str, Any] = dict(src.kwargs)
    bus = can.Bus(interface=src.interface, channel=src.channel, **kwargs)
    try:
        yield bus
    finally:
        bus.shutdown()


def _iter_live(src: LiveSource) -> Iterator[can.Message]:
    with _open_bus(src) as bus:
        _logger.info("listening on %s:%s", src.interface, src.channel)
        for msg in bus:
            if msg is None:
                continue
            yield msg


def iter_messages(source: LiveSource | ReplaySource) -> Iterator[can.Message]:
    if isinstance(source, ReplaySource):
        _logger.info("replaying %s", source.path)
        yield from _iter_replay(source.path)
    else:
        yield from _iter_live(source)
