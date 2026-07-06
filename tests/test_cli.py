from __future__ import annotations

from pathlib import Path

import pytest

from pyric_static.cli import _build_source, main
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
        def __init__(self, _cfg, *, roots, dry_run):
            called["roots"] = roots
            called["dry_run"] = dry_run

        def run(self):
            class R:
                failed_sessions = 0

            return R()

    monkeypatch.setattr("pyric_static.cli.ImportRunner", FakeRunner)
    rc = main(["import", "--config", str(cfg), str(hive), "--dry-run"])
    assert rc == 0
    assert called["dry_run"] is True
    assert called["roots"] == [hive]
