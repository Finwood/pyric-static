from __future__ import annotations

from pathlib import Path

import can
import pytest

from pyric_static.reassembly import TracerLoop, message_to_capture

LOG_PATH = Path("data/can0.2026-04-18.10-27-31.log")


def test_message_to_capture_builds_extended_fd_frame():
    msg = can.Message(
        timestamp=1_000.5,
        arbitration_id=0x107D550E,
        data=bytes.fromhex("02057100000000a1"),
        is_extended_id=True,
        is_fd=True,
    )
    cap = message_to_capture(msg)
    assert cap.frame.identifier == 0x107D550E
    assert cap.own is False
    assert cap.timestamp.system_ns == int(1_000.5 * 1e9)


@pytest.mark.skipif(not LOG_PATH.exists(), reason="log file not available")
def test_tracer_produces_transfers_from_recorded_log():
    loop = TracerLoop()
    count = 0
    with can.CanutilsLogReader(str(LOG_PATH)) as reader:
        for i, msg in enumerate(reader):
            if loop.feed(msg) is not None:
                count += 1
            if i > 2000:
                break
    assert loop.stats.frames > 0
    assert loop.stats.transfers > 0
