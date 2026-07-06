import pyarrow as pa

from pyric_static.transfer_schema import TransferSchema


def test_transfer_schema_field_count():
    assert len(TransferSchema) == 10


def test_transfer_schema_timestamp_is_utc_microseconds():
    field = TransferSchema.field("timestamp")
    assert field.type == pa.timestamp("us", tz="UTC")
    assert field.nullable is False
