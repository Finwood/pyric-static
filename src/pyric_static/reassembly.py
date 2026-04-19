"""Turn ``can.Message`` objects into pycyphal ``CANCapture`` + feed ``CANTracer``.

The tracer reassembles multi-frame Cyphal transfers and yields
``TransferTrace`` / ``CANErrorTrace`` / ``None`` — completely passive, no
transport needed.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Iterable, Iterator

import can
from pycyphal.transport import Timestamp, Trace, TransferTrace
from pycyphal.transport.can import CANCapture, CANErrorTrace, CANTracer
from pycyphal.transport.can._can import CANTransport
from pycyphal.transport.can.media import DataFrame, FrameFormat

_logger = logging.getLogger(__name__)


@dataclass
class ReassemblyStats:
    frames: int = 0
    transfers: int = 0
    errors: dict[str, int] = field(default_factory=dict)

    def note_error(self, err: CANErrorTrace) -> None:
        key = err.error.name
        self.errors[key] = self.errors.get(key, 0) + 1


def message_to_capture(msg: can.Message) -> CANCapture:
    fmt = FrameFormat.EXTENDED if msg.is_extended_id else FrameFormat.BASE
    frame = DataFrame(fmt, msg.arbitration_id, bytearray(msg.data))
    system_ns = int(msg.timestamp * 1_000_000_000) if msg.timestamp else time.time_ns()
    ts = Timestamp(system_ns=system_ns, monotonic_ns=time.monotonic_ns())
    return CANCapture(timestamp=ts, frame=frame, own=False)


class TracerLoop:
    """Feed ``can.Message`` objects and yield ``TransferTrace`` results."""

    def __init__(self) -> None:
        self._tracer: CANTracer = CANTransport.make_tracer()
        self.stats = ReassemblyStats()

    def feed(self, msg: can.Message) -> Trace | None:
        self.stats.frames += 1
        cap = message_to_capture(msg)
        trace = self._tracer.update(cap)
        if isinstance(trace, TransferTrace):
            self.stats.transfers += 1
        elif isinstance(trace, CANErrorTrace):
            self.stats.note_error(trace)
        return trace

    def transfers(self, messages: Iterable[can.Message]) -> Iterator[TransferTrace]:
        for msg in messages:
            tr = self.feed(msg)
            if isinstance(tr, TransferTrace):
                yield tr
