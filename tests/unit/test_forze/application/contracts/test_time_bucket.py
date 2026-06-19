"""Tests for aggregate time-bucket helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from forze.application.contracts.querying.internal.time_bucket import (
    floor_to_time_bucket,
    parse_aggregate_timezone,
    tzinfo_from_resolved,
)
from forze.base.exceptions import CoreException


def test_parse_aggregate_timezone_defaults_to_utc() -> None:
    resolved = parse_aggregate_timezone(None)
    assert resolved.mode == "iana"
    assert resolved.iana == "UTC"


def test_parse_aggregate_timezone_fixed_offset() -> None:
    resolved = parse_aggregate_timezone("+05:30")
    assert resolved.mode == "fixed"
    assert resolved.offset == timedelta(hours=5, minutes=30)


def test_parse_aggregate_timezone_iana() -> None:
    resolved = parse_aggregate_timezone("Europe/Berlin")
    assert resolved.mode == "iana"
    assert resolved.iana == "Europe/Berlin"


def test_parse_aggregate_timezone_invalid_offset_raises() -> None:
    with pytest.raises(CoreException, match="out of range"):
        parse_aggregate_timezone("+99:00")


def test_parse_aggregate_timezone_no_colon_forms() -> None:
    assert parse_aggregate_timezone("+0530").offset == timedelta(hours=5, minutes=30)
    assert parse_aggregate_timezone("+05").offset == timedelta(hours=5)


def test_parse_aggregate_timezone_single_digit_hours() -> None:
    assert parse_aggregate_timezone("+3").offset == timedelta(hours=3)
    assert parse_aggregate_timezone("-5").offset == timedelta(hours=-5)


def test_parse_aggregate_timezone_max_offset_boundary() -> None:
    assert parse_aggregate_timezone("+14:00").offset == timedelta(hours=14)
    # Beyond the real ±14:00 maximum.
    with pytest.raises(CoreException, match="out of range"):
        parse_aggregate_timezone("+14:30")


def test_parse_aggregate_timezone_ambiguous_three_digits_rejected() -> None:
    # ``+123`` must not silently parse as 1h23m; not a valid offset and (not
    # being a zone) it falls through to the unknown-timezone error.
    with pytest.raises(CoreException, match="Unknown timezone"):
        parse_aggregate_timezone("+123")


def test_parse_aggregate_timezone_unknown_iana_raises() -> None:
    with pytest.raises(CoreException, match="Unknown timezone"):
        parse_aggregate_timezone("Not/A_Real_Zone")


def test_tzinfo_from_resolved_fixed_and_iana() -> None:
    fixed = parse_aggregate_timezone("-08:00")
    tz_fixed = tzinfo_from_resolved(fixed)
    assert tz_fixed == timezone(timedelta(hours=-8))

    iana = parse_aggregate_timezone("UTC")
    assert tzinfo_from_resolved(iana) == ZoneInfo("UTC")


def test_floor_to_time_bucket_hour_day_week_month() -> None:
    dt = datetime(2026, 3, 15, 14, 37, 22, tzinfo=timezone.utc)
    tz = ZoneInfo("UTC")

    hour = floor_to_time_bucket(dt, unit="hour", tz=tz)
    assert hour == datetime(2026, 3, 15, 14, 0, 0, tzinfo=tz)

    day = floor_to_time_bucket(dt, unit="day", tz=tz)
    assert day == datetime(2026, 3, 15, 0, 0, 0, tzinfo=tz)

    month = floor_to_time_bucket(dt, unit="month", tz=tz)
    assert month == datetime(2026, 3, 1, 0, 0, 0, tzinfo=tz)

    week = floor_to_time_bucket(dt, unit="week", tz=tz)
    assert week.weekday() == 0


def test_floor_to_time_bucket_naive_input_uses_utc() -> None:
    naive = datetime(2026, 1, 1, 12, 0, 0)
    floored = floor_to_time_bucket(naive, unit="hour", tz=timezone.utc)
    assert floored.tzinfo is not None


def test_floor_to_time_bucket_invalid_unit_raises() -> None:
    dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(CoreException, match="Invalid time bucket unit"):
        floor_to_time_bucket(dt, unit="year", tz=timezone.utc)  # type: ignore[arg-type]
