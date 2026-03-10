from datetime import UTC, datetime, timezone
from uuid import UUID

import pytest

from forze.base.primitives.uuid import (
    _hash_from_any,
    _uuid4_from_any,
    datetime_to_uuid7,
    uuid4,
    uuid7,
    uuid7_to_datetime,
)


# ----------------------- #
# uuid7


class TestUuid7:
    def test_generates_valid_uuid(self) -> None:
        u = uuid7()
        assert isinstance(u, UUID)
        assert u.version == 7

    def test_with_timestamp_ms(self) -> None:
        u = uuid7(timestamp_ms=1_000_000)
        assert isinstance(u, UUID)
        assert u.version == 7

    def test_with_timestamp_ns(self) -> None:
        u = uuid7(timestamp_ns=1_000_000_000_000)
        assert isinstance(u, UUID)
        assert u.version == 7

    def test_both_ms_and_ns_raises(self) -> None:
        with pytest.raises(ValueError, match="only one"):
            uuid7(timestamp_ms=100, timestamp_ns=100)

    def test_zero_ms_returns_nil(self) -> None:
        u = uuid7(timestamp_ms=0)
        assert str(u) == "00000000-0000-0000-0000-000000000000"

    def test_zero_ns_returns_nil(self) -> None:
        u = uuid7(timestamp_ns=0)
        assert str(u) == "00000000-0000-0000-0000-000000000000"

    def test_negative_timestamp_raises(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            uuid7(timestamp_ns=-1)

    def test_float_timestamp_ms(self) -> None:
        u = uuid7(timestamp_ms=1_000_000.5)
        assert isinstance(u, UUID)
        assert u.version == 7

    def test_float_timestamp_ns(self) -> None:
        u = uuid7(timestamp_ns=1_000_000_000_000.0)
        assert isinstance(u, UUID)

    def test_uniqueness(self) -> None:
        uuids = {uuid7() for _ in range(100)}
        assert len(uuids) == 100


# ----------------------- #
# uuid7_to_datetime


class TestUuid7ToDatetime:
    def test_extracts_timestamp_utc(self) -> None:
        now = datetime.now(tz=UTC)
        u = uuid7(timestamp_ms=int(now.timestamp() * 1000))
        dt = uuid7_to_datetime(u, tz=UTC)
        assert dt is not None
        assert abs(dt.timestamp() - now.timestamp()) < 1

    def test_high_precision_mode(self) -> None:
        now = datetime.now(tz=UTC)
        u = uuid7(timestamp_ms=int(now.timestamp() * 1000))
        dt = uuid7_to_datetime(u, tz=UTC, high_precision=True)
        assert dt is not None
        assert abs(dt.timestamp() - now.timestamp()) < 0.01

    def test_accepts_string_uuid(self) -> None:
        u = uuid7(timestamp_ms=1_700_000_000_000)
        dt = uuid7_to_datetime(str(u), tz=UTC)
        assert dt is not None

    def test_non_v7_returns_none(self) -> None:
        u4 = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert uuid7_to_datetime(u4) is None

    def test_no_timezone(self) -> None:
        u = uuid7(timestamp_ms=1_700_000_000_000)
        dt = uuid7_to_datetime(u, tz=None)
        assert dt is not None
        assert dt.tzinfo is None

    def test_custom_timezone(self) -> None:
        from zoneinfo import ZoneInfo

        u = uuid7(timestamp_ms=1_700_000_000_000)
        tz = ZoneInfo("America/New_York")
        dt = uuid7_to_datetime(u, tz=tz)
        assert dt is not None
        assert dt.tzinfo is not None


# ----------------------- #
# datetime_to_uuid7


class TestDatetimeToUuid7:
    def test_from_datetime(self) -> None:
        dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        u = datetime_to_uuid7(dt)
        assert isinstance(u, UUID)
        assert u.version == 7

    def test_from_string(self) -> None:
        u = datetime_to_uuid7("2024-06-15T12:00:00Z")
        assert isinstance(u, UUID)
        assert u.version == 7

    def test_roundtrip(self) -> None:
        now = datetime.now(tz=UTC)
        u = datetime_to_uuid7(now)
        decoded = uuid7_to_datetime(u, tz=UTC, high_precision=True)
        assert decoded is not None
        assert abs(decoded.timestamp() - now.timestamp()) < 0.001


# ----------------------- #
# _hash_from_any


class TestHashFromAny:
    def test_dict_deterministic(self) -> None:
        h1 = _hash_from_any({"a": 1, "b": 2})
        h2 = _hash_from_any({"b": 2, "a": 1})
        assert h1 == h2

    def test_string(self) -> None:
        h = _hash_from_any("test")
        assert isinstance(h, str)
        assert len(h) == 32

    def test_number(self) -> None:
        h = _hash_from_any(42)
        assert isinstance(h, str)
        assert len(h) == 32

    def test_different_inputs_different_hashes(self) -> None:
        assert _hash_from_any("a") != _hash_from_any("b")


# ----------------------- #
# _uuid4_from_any


class TestUuid4FromAny:
    def test_deterministic(self) -> None:
        u1 = _uuid4_from_any("value")
        u2 = _uuid4_from_any("value")
        assert u1 == u2
        assert u1.version == 4

    def test_different_inputs(self) -> None:
        assert _uuid4_from_any("a") != _uuid4_from_any("b")


# ----------------------- #
# uuid4


class TestUuid4:
    def test_random_when_no_value(self) -> None:
        r1 = uuid4()
        r2 = uuid4()
        assert r1 != r2

    def test_deterministic_from_value(self) -> None:
        d1 = uuid4("same")
        d2 = uuid4("same")
        assert d1 == d2

    def test_none_generates_random(self) -> None:
        r1 = uuid4(None)
        r2 = uuid4(None)
        assert r1 != r2
