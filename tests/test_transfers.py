from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pyric_static.transfers import (
    discover_sessions,
    file_overlaps_range,
    iter_transfer_batches,
    parse_hive_tags,
    parse_time_bound,
    scan_session_time_range,
)
from tests.transfer_fixtures import make_hive_session, make_transfer_row, write_transfer_parquet


def test_parse_time_bound_utc_z_suffix():
    assert parse_time_bound("2026-04-18T10:27:31Z") == datetime(2026, 4, 18, 10, 27, 31, tzinfo=timezone.utc)


def test_parse_time_bound_date_only_uses_local_midnight():
    dt = parse_time_bound("2026-04-18")
    assert dt == datetime(2026, 4, 17, 22, 0, 0, tzinfo=timezone.utc)


def test_parse_time_bound_naive_datetime_uses_local_tz():
    dt = parse_time_bound("2026-04-18T08:00:00")
    assert dt == datetime(2026, 4, 18, 6, 0, 0, tzinfo=timezone.utc)


def test_parse_time_bound_invalid_raises():
    with pytest.raises(ValueError, match="invalid time bound"):
        parse_time_bound("not-a-date")


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


def test_file_overlaps_range_true_when_rows_inside_window(tmp_path):
    path = tmp_path / "inside.parquet"
    write_transfer_parquet(
        path,
        [make_transfer_row(timestamp=datetime(2026, 4, 18, 10, 0, 0, tzinfo=timezone.utc))],
    )
    start = datetime(2026, 4, 18, 9, 0, 0, tzinfo=timezone.utc)
    stop = datetime(2026, 4, 18, 11, 0, 0, tzinfo=timezone.utc)
    assert file_overlaps_range(path, start, stop) is True


def test_file_overlaps_range_false_when_stats_prove_no_overlap(tmp_path):
    path = tmp_path / "outside.parquet"
    write_transfer_parquet(
        path,
        [make_transfer_row(timestamp=datetime(2026, 4, 18, 8, 0, 0, tzinfo=timezone.utc))],
    )
    start = datetime(2026, 4, 18, 9, 0, 0, tzinfo=timezone.utc)
    stop = datetime(2026, 4, 18, 11, 0, 0, tzinfo=timezone.utc)
    assert file_overlaps_range(path, start, stop) is False


def test_iter_transfer_batches_filtered_returns_window_rows_only(tmp_path):
    path = tmp_path / "mixed.parquet"
    write_transfer_parquet(
        path,
        [
            make_transfer_row(timestamp=datetime(2026, 4, 18, 8, 0, 0, tzinfo=timezone.utc)),
            make_transfer_row(timestamp=datetime(2026, 4, 18, 10, 0, 0, tzinfo=timezone.utc)),
            make_transfer_row(timestamp=datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)),
        ],
    )
    start = datetime(2026, 4, 18, 9, 0, 0, tzinfo=timezone.utc)
    stop = datetime(2026, 4, 18, 11, 0, 0, tzinfo=timezone.utc)
    rows = []
    for batch in iter_transfer_batches(path, start=start, stop=stop):
        rows.extend(batch.to_pylist())
    assert len(rows) == 1
    assert rows[0]["timestamp"] == datetime(2026, 4, 18, 10, 0, 0, tzinfo=timezone.utc)


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
