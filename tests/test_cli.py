from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from pyric_static.cli import _build_source, import_main, main
from pyric_static.sources import LiveSource, ReplaySource


@pytest.fixture(autouse=True)
def _stub_standard_ports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pyric_static.config.build_standard_ports", lambda: {})


def test_build_source_replay():
    class A:
        replay = Path("a.log")
        bus_interface = None
        bus_channel = None
        bus_arg: list[str] = []

    assert isinstance(_build_source(A()), ReplaySource)


def test_build_source_live():
    class A:
        replay = None
        bus_interface = "socketcan"
        bus_channel = "can0"
        bus_arg = ["bitrate=1000000"]

    s = _build_source(A())
    assert isinstance(s, LiveSource)
    assert s.kwargs == {"bitrate": 1000000}


def test_main_requires_source():
    with pytest.raises(SystemExit):
        main(["--config", "/tmp/pyric-static-missing-source.toml"])


def test_main_replay_and_live_conflict(tmp_path: Path):
    cfg = tmp_path / "c.toml"
    cfg.write_text('[logger]\nname = "t"\niface = "can0"\n')
    with pytest.raises(SystemExit):
        main(
            [
                "--config",
                str(cfg),
                "--replay",
                str(tmp_path / "a.log"),
                "--interface",
                "socketcan",
                "--channel",
                "can0",
            ]
        )


def test_main_live_requires_logger_section(tmp_path: Path):
    cfg = tmp_path / "c.toml"
    cfg.write_text("[[nodes]]\nid = 11\n")
    with pytest.raises(SystemExit, match="\\[logger\\]"):
        main(["--config", str(cfg), "--interface", "socketcan", "--channel", "can0"])


def test_main_import_dispatches(tmp_path: Path, monkeypatch):
    cfg = tmp_path / "c.toml"
    cfg.write_text("[influx]\nbucket = 'pyric'\n")
    hive = tmp_path / "logger=L" / "session=S"
    hive.mkdir(parents=True)
    called: dict = {}

    class FakeRunner:
        def __init__(self, _cfg, *, roots, config_path, dry_run, start=None, stop=None, jobs=1):
            called["roots"] = roots
            called["dry_run"] = dry_run
            called["start"] = start
            called["stop"] = stop
            called["config_path"] = config_path
            called["jobs"] = jobs

        def run(self):
            class R:
                failed_sessions = 0

            return R()

    monkeypatch.setattr("pyric_static.cli.ImportRunner", FakeRunner)
    rc = main(["import", "--config", str(cfg), str(hive), "--dry-run"])
    assert rc == 0
    assert called["dry_run"] is True
    assert called["roots"] == [hive]
    assert called["start"] is None
    assert called["stop"] is None


def test_import_requires_start_and_stop_together(tmp_path: Path):
    cfg = tmp_path / "c.toml"
    cfg.write_text("[influx]\nbucket = 'pyric'\n")
    hive = tmp_path / "logger=L" / "session=S"
    hive.mkdir(parents=True)
    with pytest.raises(SystemExit):
        import_main(["--config", str(cfg), "--start", "2026-04-18", str(hive)])


def test_import_forwards_time_bounds(tmp_path: Path, monkeypatch):
    cfg = tmp_path / "c.toml"
    cfg.write_text("[influx]\nbucket = 'pyric'\n")
    hive = tmp_path / "logger=L" / "session=S"
    hive.mkdir(parents=True)
    called: dict = {}

    class FakeRunner:
        def __init__(self, _cfg, *, roots, config_path, dry_run, start, stop, jobs=1):
            called["roots"] = roots
            called["dry_run"] = dry_run
            called["start"] = start
            called["stop"] = stop
            called["config_path"] = config_path
            called["jobs"] = jobs

        def run(self):
            class R:
                failed_sessions = 0

            return R()

    monkeypatch.setattr("pyric_static.cli.ImportRunner", FakeRunner)
    rc = import_main(
        [
            "--config",
            str(cfg),
            "--start",
            "2026-04-18T08:00:00Z",
            "--stop",
            "2026-04-18T12:00:00Z",
            str(hive),
        ]
    )
    assert rc == 0
    assert called["start"] == datetime(2026, 4, 18, 8, 0, 0, tzinfo=timezone.utc)
    assert called["stop"] == datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)


def test_import_forwards_jobs(tmp_path: Path, monkeypatch):
    cfg = tmp_path / "c.toml"
    cfg.write_text("[influx]\nbucket = 'pyric'\n")
    hive = tmp_path / "logger=L" / "session=S"
    hive.mkdir(parents=True)
    called: dict = {}

    class FakeRunner:
        def __init__(self, _cfg, *, roots, config_path, dry_run, start=None, stop=None, jobs=1):
            called["jobs"] = jobs
            called["config_path"] = config_path

        def run(self):
            class R:
                failed_sessions = 0

            return R()

    monkeypatch.setattr("pyric_static.cli.ImportRunner", FakeRunner)
    rc = import_main(["--config", str(cfg), "--jobs", "4", str(hive)])
    assert rc == 0
    assert called["jobs"] == 4
    assert called["config_path"] == cfg


def test_import_rejects_invalid_jobs(tmp_path: Path):
    cfg = tmp_path / "c.toml"
    cfg.write_text("[influx]\nbucket = 'pyric'\n")
    hive = tmp_path / "logger=L" / "session=S"
    hive.mkdir(parents=True)
    with pytest.raises(SystemExit):
        import_main(["--config", str(cfg), "--jobs", "0", str(hive)])
