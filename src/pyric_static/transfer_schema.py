"""Vendored Cyphal transfer Arrow schema.

Source: sc-schema/sc_schema/transfers/arrow.py in the canedge monorepo.
Do not add sc-schema as a package dependency.
"""

from __future__ import annotations

import pyarrow as pa

_TRANSFER_FIELDS = [
    pa.field("channel", pa.string()),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("type", pa.string(), nullable=False),
    pa.field("id", pa.int16(), nullable=False),
    pa.field("source", pa.uint8()),
    pa.field("dest", pa.uint8()),
    pa.field("priority", pa.uint8()),
    pa.field("transfer_id", pa.uint8()),
    pa.field("payload", pa.binary()),
    pa.field("length", pa.int32()),
]

TransferSchema = pa.schema(_TRANSFER_FIELDS)
