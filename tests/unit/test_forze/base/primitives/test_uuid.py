from datetime import UTC, datetime

from forze.base.primitives.uuid import datetime_to_uuid7, uuid4, uuid7, uuid7_to_datetime


def test_uuid4_random_and_deterministic_from_value() -> None:
    r1 = uuid4()
    r2 = uuid4()
    assert r1 != r2

    d1 = uuid4("same")
    d2 = uuid4("same")
    assert d1 == d2


def test_uuid7_and_roundtrip_datetime_high_precision() -> None:
    now = datetime.now(tz=UTC)
    u = datetime_to_uuid7(now)
    decoded = uuid7_to_datetime(u, tz=UTC, high_precision=True)
    assert decoded is not None
    # allow small rounding differences
    assert abs(decoded.timestamp() - now.timestamp()) < 0.001


def test_uuid7_zero_timestamp_returns_nil() -> None:
    u = uuid7(timestamp_ms=0)
    assert str(u) == "00000000-0000-0000-0000-000000000000"

