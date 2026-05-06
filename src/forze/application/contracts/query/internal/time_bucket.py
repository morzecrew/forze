"""Timezone and calendar bucketing helpers for aggregate time windows."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from forze.base.errors import CoreError

# ----------------------- #

_TIMEZONE_OFFSET_RE = re.compile(r"^([+-])(\d{1,2})(?::?(\d{2}))?\Z")

TimeBucketMode = Literal["iana", "fixed"]


@dataclass(frozen=True, slots=True)
class ResolvedTimeBucketTimezone:
    """Normalized timezone for aggregate bucketing."""

    mode: TimeBucketMode
    """``iana`` uses :class:`zoneinfo.ZoneInfo`; ``fixed`` uses a UTC offset."""

    iana: str
    """IANA zone name when ``mode == \"iana\"``; empty when ``mode == \"fixed\"``."""

    offset: timedelta | None
    """Fixed offset from UTC when ``mode == \"fixed\"``; otherwise ``None``."""


# ....................... #


def parse_aggregate_timezone(wire: str | None) -> ResolvedTimeBucketTimezone:
    """Parse wire ``timezone`` for ``$time_bucket`` (IANA name or numeric offset)."""

    if wire is None or not str(wire).strip():
        return ResolvedTimeBucketTimezone(mode="iana", iana="UTC", offset=None)

    s = str(wire).strip()
    up = s.upper()
    if up in ("UTC", "Z", "GMT"):
        return ResolvedTimeBucketTimezone(mode="iana", iana="UTC", offset=None)

    m = _TIMEZONE_OFFSET_RE.match(s.replace(" ", ""))
    if m:
        sign = -1 if m.group(1) == "-" else 1
        h = int(m.group(2))
        mm = int(m.group(3) or 0)
        if h > 14 or mm > 59:
            raise CoreError(f"Timezone offset out of range: {wire!r}")

        total_min = sign * (h * 60 + mm)
        return ResolvedTimeBucketTimezone(
            mode="fixed",
            iana="",
            offset=timedelta(minutes=total_min),
        )

    try:
        ZoneInfo(s)
    except ZoneInfoNotFoundError as e:
        raise CoreError(f"Unknown timezone: {wire!r}") from e

    return ResolvedTimeBucketTimezone(mode="iana", iana=s, offset=None)


def tzinfo_from_resolved(resolved: ResolvedTimeBucketTimezone) -> timezone | ZoneInfo:
    """``tzinfo`` for mock bucketing aligned with Postgres/Mongo semantics."""

    if resolved.mode == "fixed":
        off = resolved.offset or timedelta(0)

        return timezone(off)

    return ZoneInfo(resolved.iana)


def floor_to_time_bucket(
    dt: datetime,
    *,
    unit: Literal["hour", "day", "week", "month"],
    tz: timezone | ZoneInfo,
) -> datetime:
    """Floor *dt* to the start of *unit* in *tz* (weeks start Monday)."""

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    local = dt.astimezone(tz)

    if unit == "hour":
        return local.replace(minute=0, second=0, microsecond=0)

    if unit == "day":
        return local.replace(hour=0, minute=0, second=0, microsecond=0)

    if unit == "month":
        return local.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    if unit == "week":
        d: date = local.date()
        monday = d - timedelta(days=d.weekday())
        return datetime(
            monday.year,
            monday.month,
            monday.day,
            0,
            0,
            0,
            0,
            tzinfo=tz,
        )

    raise CoreError(f"Invalid time bucket unit: {unit!r}")
