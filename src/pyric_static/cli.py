"""Command-line entrypoint for ``pyric-static``."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

from .config import load
from .import_app import ImportRunner
from .logger_app import PassiveLogger
from .sources import LiveSource, ReplaySource


def _parse_bus_arg(spec: str) -> tuple[str, Any]:
    if "=" not in spec:
        raise argparse.ArgumentTypeError("expected KEY=VALUE")
    key, raw = spec.split("=", 1)
    key = key.strip()
    if not key:
        raise argparse.ArgumentTypeError("empty key in --bus-arg")
    v = raw.strip()
    if v.lower() in ("true", "false"):
        return key, v.lower() == "true"
    if v.isdigit():
        return key, int(v)
    try:
        return key, int(v, 0)
    except ValueError:
        return key, v


def _build_source(args: argparse.Namespace) -> LiveSource | ReplaySource:
    if args.replay is not None:
        if args.bus_interface is not None or args.bus_channel is not None:
            raise SystemExit("--replay cannot be combined with --interface / --channel")
        if args.bus_arg:
            raise SystemExit("--bus-arg is only valid with --interface / --channel")
        return ReplaySource(path=args.replay)
    assert args.bus_interface is not None and args.bus_channel is not None
    kwargs: dict[str, Any] = {}
    for item in args.bus_arg:
        k, v = _parse_bus_arg(item)
        kwargs[k] = v
    return LiveSource(interface=args.bus_interface, channel=args.bus_channel, kwargs=kwargs)


def _install_logging(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )


def live_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pyric-static")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--config", required=True, help="path to TOML config (logger, influx, nodes)")
    parser.add_argument("--dry-run", action="store_true", help="decode messages but do not write to Influx")
    parser.add_argument(
        "--replay",
        type=Path,
        metavar="FILE",
        help="replay a recorded CAN log (.log, .asc, .blf, …)",
    )
    parser.add_argument(
        "-i",
        "--interface",
        dest="bus_interface",
        metavar="NAME",
        help="python-can interface name for live capture (e.g. socketcan)",
    )
    parser.add_argument(
        "-c",
        "--channel",
        dest="bus_channel",
        metavar="NAME",
        help="python-can channel for live capture (e.g. can0)",
    )
    parser.add_argument(
        "--bus-arg",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="extra keyword argument for can.Bus (repeatable), e.g. --bus-arg bitrate=1000000",
    )
    args = parser.parse_args(argv)
    if args.replay is None and (args.bus_interface is None or args.bus_channel is None):
        parser.error("provide either --replay FILE or both --interface and --channel")
    _install_logging(args.log_level)
    try:
        cfg = load(args.config)
        if cfg.logger is None:
            raise SystemExit(
                "live/replay mode requires a [logger] section in the config (name and iface tags for Influx)"
            )
        source = _build_source(args)
        PassiveLogger(cfg, source, dry_run=args.dry_run).run()
        return 0
    except Exception:  # noqa: BLE001
        logging.getLogger("pyric_static").exception("fatal error")
        return 1


def import_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pyric-static import")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--config", required=True, type=Path, help="path to TOML config (influx, nodes)")
    parser.add_argument("--dry-run", action="store_true", help="decode messages but do not delete or write to Influx")
    parser.add_argument("roots", nargs="+", type=Path, help="hive root(s) containing transfer parquet files")
    args = parser.parse_args(argv)
    _install_logging(args.log_level)
    try:
        cfg = load(args.config)
        stats = ImportRunner(cfg, roots=list(args.roots), dry_run=args.dry_run).run()
        return 1 if stats.failed_sessions else 0
    except Exception:  # noqa: BLE001
        logging.getLogger("pyric_static").exception("fatal error")
        return 1


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "import":
        return import_main(argv[1:])
    return live_main(argv)


if __name__ == "__main__":
    sys.exit(main())
