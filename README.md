# pyric-static

A completely **passive** Cyphal/CAN-to-InfluxDB logger. It produces zero bus
traffic: no Cyphal node, no GetInfo requests, no register access. Instead it
listens to CAN frames (live or recorded), reassembles Cyphal transfers with
`pycyphal.transport.can.CANTracer`, deserializes the payload with compiled
DSDL types, and writes points to InfluxDB using the same measurement, tag,
and field layout as `pyric`'s `NodeExplorer`.

## Pipeline

```text
can.Bus / can.LogReader
        │  can.Message
        ▼
CAN frame → CANCapture → CANTracer → TransferTrace
                                         │
                                         ▼
          resolve PortSpec (explicit (node_id, port_id) → implicit standard port)
                                         │
                                         ▼
                 pycyphal.dsdl.deserialize + flatten(to_builtin(msg))
                                         │
                                         ▼
                 InfluxDB Point (measurement = DSDL type string)
```

## Features

- Two frame sources: live `socketcan` (or any python-can interface) or replay
  from `.log`/`.asc`/`.blf`/... files via `can.LogReader`. **Source is chosen on
  the CLI**, not in the TOML file.
- Node-centric TOML config — each node lists its vendor ports once.
- Standard fixed-subject messages (heartbeat, time sync) are **implicit**: no
  TOML entry required. Per-node explicit entries override them if you ever
  need to.
- Type resolution keyed on `(source_node_id, port_id)` so duplicate/conflicting
  node IDs stay disambiguated.
- Identical Influx schema to `pyric`: measurement = DSDL type string, tags
  `logger`, `iface`, `node_id`, `port_id`, `port_name`, optionally
  `app_name` / `device_uid`; bool fields mapped to `int`.

## Install

```bash
uv sync
```

DSDL namespaces (`uavcan`, `b17`, …) must be importable; point `PYTHONPATH`
at the directory where `pycyphal.dsdl.compile_all()` emitted the Python
packages.

## Influx configuration

`influxdb-client` reads the standard env vars, so a `.env` (loaded via
`direnv` or `dotenv_if_exists`) is enough:

```dotenv
INFLUXDB_V2_URL=http://localhost:8086
INFLUXDB_V2_ORG="B17 Systems"
INFLUXDB_V2_TOKEN=...
INFLUXDB_V2_BUCKET=pyric
```

(`INFLUXDB_V2_BUCKET` is informational; the bucket the logger writes to is
set in the config's `[influx] bucket = "..."` field and defaults to `pyric`.)

## Config schema

The TOML file contains only **`[logger]`**, optional **`[influx]`**, and
**`[[nodes]]`** blocks. Do not put a `[source]` section (it is rejected).

```toml
[logger]
name  = "pyric-static-demo"   # Influx tag "logger"
iface = "can0"                # Influx tag "iface"

[influx]
bucket = "pyric"

[[nodes]]
id   = 11
name = "systems.b17.io-node"
uid  = "41e7cb11e78ab72b65da49714c4bcb3b"

[[nodes.ports]]
id   = 113
name = "analog_inputs"
type = "b17.AnalogInputs.0.1"
```

Lookup order at runtime:

1. Explicit `(source_node_id, port_id)` from the config.
2. Implicit fixed-subject map (heartbeat, time sync).
3. Skip.

## Run

You must pass **either** `--replay FILE` **or** both `--interface` / `-i` and
`--channel` / `-c` for live capture. Optional `--bus-arg KEY=VALUE` (repeatable)
forwards extra keyword arguments to `python-can`'s `Bus` constructor.

Replay a recorded log:

```bash
pyric-static --config pyric-static.toml --replay data/can0.2026-04-18.10-27-31.log
pyric-static --config pyric-static.toml --replay data/can0.log --dry-run
```

Live bus:

```bash
pyric-static --config pyric-static.toml --interface socketcan --channel can0
pyric-static --config pyric-static.toml -i socketcan -c can0 --bus-arg bitrate=1000000
```

Ctrl+C cleanly flushes the Influx batch and shuts down.

## Import CANedge transfer parquet

Batch-upload pre-assembled transfer parquet from the frame-decoding pipeline hive
layout. Omit `[logger]` in the config — `logger`, `session`, and `iface` tags come
from the hive partitions and each transfer's `channel` field.

```bash
pyric-static import --config pyric-static.toml /mnt/data/transfers
pyric-static import --config pyric-static.toml /mnt/a /mnt/b --dry-run
```

Optional time window (both flags required; bounds are `[start, stop)` in UTC after parsing):

```bash
# Single local calendar day
pyric-static import --config pyric-static.toml \
  --start 2026-04-18 --stop 2026-04-19 /mnt/data/transfers

# Sub-day window (ISO 8601; naive values use local timezone)
pyric-static import --config pyric-static.toml \
  --start 2026-04-18T08:00:00 --stop 2026-04-18T12:00:00 /mnt/data/transfers
```

Date-only values map to `00:00:00` local on that date. Filtered import deletes and
re-uploads only within the window (idempotent partial re-import).

Before uploading each `(logger, session)`, existing Influx points with matching tags
in the parquet time range are deleted. Only `Message` transfers are written (same
as live mode).

## Tests

```bash
PYTHONPATH=/path/to/compiled/dsdl uv run pytest
```
