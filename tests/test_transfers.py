from __future__ import annotations

from datetime import datetime, timezone

from pyric_static.transfers import discover_sessions, parse_hive_tags, scan_session_time_range
from tests.transfer_fixtures import make_hive_session, make_transfer_row, write_transfer_parquet


def test_parse_hive_tags_from_session_file(tmp_path):
    root = make_hive_session(
        tmp_path,
        "3544BCD3",
        "00000509",
        files={"a.parquet": [make_transfer_row()]},
    )
    path = root / "logger=3544BCD3" / "session=00000509" / "a.parquet"
    assert parse_hive_tags(path) == ("3544BCD3", "00000509")


def test_discover_sessions_full_hive(tmp_path):
    root = make_hive_session(
        tmp_path,
        "AAAA",
        "00000001",
        files={"1.parquet": [make_transfer_row()], "2.parquet": [make_transfer_row()]},
    )
    make_hive_session(
        tmp_path,
        "BBBB",
        "00000002",
        files={"x.parquet": [make_transfer_row()]},
    )
    sessions = discover_sessions([root])
    assert set(sessions.keys()) == {("AAAA", "00000001"), ("BBBB", "00000002")}
    assert len(sessions[("AAAA", "00000001")]) == 2


def test_discover_sessions_session_dir_root(tmp_path):
    session_dir = tmp_path / "logger=3544BCD3" / "session=00000509"
    write_transfer_parquet(session_dir / "only.parquet", [make_transfer_row()])
    sessions = discover_sessions([session_dir])
    assert list(sessions.keys()) == [("3544BCD3", "00000509")]


def test_discover_sessions_dedupes_across_roots(tmp_path):
    root = make_hive_session(tmp_path, "L", "S", files={"a.parquet": [make_transfer_row()]})
    path = root / "logger=L" / "session=S" / "a.parquet"
    sessions = discover_sessions([root, path.parent])
    assert len(sessions[("L", "S")]) == 1


def test_scan_session_time_range(tmp_path):
    root = make_hive_session(
        tmp_path,
        "L",
        "S",
        files={
            "a.parquet": [
                make_transfer_row(timestamp=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
            ],
            "b.parquet": [
                make_transfer_row(timestamp=datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)),
                make_transfer_row(
                    transfer_type="Request",
                    timestamp=datetime(2026, 1, 3, 0, 0, 0, tzinfo=timezone.utc),
                ),
            ],
        },
    )
    files = discover_sessions([root])[("L", "S")]
    t_min, t_max = scan_session_time_range(files)
    assert t_min == datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert t_max == datetime(2026, 1, 3, 0, 0, 0, tzinfo=timezone.utc)
