"""Timezone and calendar bucketing helpers for aggregate time windows."""

import re
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import attrs

from forze.base.exceptions import exc

# ----------------------- #

# Accepts ``HH:MM`` (group 2/3), compact ``HHMM`` (group 4), or bare ``H``/``HH``
# (group 5). The no-colon minutes form requires four digits, so an ambiguous
# ``+123`` matches none of the alternatives and is rejected (vs. parsing as
# 1h23m); ``+3``, ``+05``, ``+0530`` and ``+05:30`` all parse.
_TIMEZONE_OFFSET_RE = re.compile(r"^([+-])(?:(\d{1,2}):(\d{2})|(\d{4})|(\d{1,2}))\Z")

TimeBucketMode = Literal["iana", "fixed"]

# ....................... #


@attrs.define(frozen=True, slots=True)
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
    """Parse wire ``timezone`` for ``$trunc`` group expressions (IANA or numeric offset)."""

    if wire is None or not str(wire).strip():
        return ResolvedTimeBucketTimezone(mode="iana", iana="UTC", offset=None)

    s = str(wire).strip()
    up = s.upper()
    if up in ("UTC", "Z", "GMT"):
        return ResolvedTimeBucketTimezone(mode="iana", iana="UTC", offset=None)

    m = _TIMEZONE_OFFSET_RE.match(s.replace(" ", ""))
    if m:
        sign = -1 if m.group(1) == "-" else 1
        if m.group(2) is not None:  # HH:MM
            h, mm = int(m.group(2)), int(m.group(3))
        elif m.group(4) is not None:  # compact HHMM
            h, mm = int(m.group(4)[:2]), int(m.group(4)[2:])
        else:  # bare H / HH
            h, mm = int(m.group(5)), 0
        if mm > 59 or h * 60 + mm > 14 * 60:
            # Max real UTC offset is ±14:00; reject ±14:30, ±15:00, ±09:99, …
            raise exc.precondition(f"Timezone offset out of range: {wire!r}")

        total_min = sign * (h * 60 + mm)
        return ResolvedTimeBucketTimezone(
            mode="fixed",
            iana="",
            offset=timedelta(minutes=total_min),
        )

    try:
        ZoneInfo(s)
    except ZoneInfoNotFoundError as e:
        raise exc.precondition(f"Unknown timezone: {wire!r}") from e

    return ResolvedTimeBucketTimezone(mode="iana", iana=s, offset=None)


# ....................... #


def tzinfo_from_resolved(resolved: ResolvedTimeBucketTimezone) -> timezone | ZoneInfo:
    """``tzinfo`` for mock bucketing aligned with Postgres/Mongo semantics."""

    if resolved.mode == "fixed":
        off = resolved.offset or timedelta(0)

        return timezone(off)

    return ZoneInfo(resolved.iana)


# ....................... #


def floor_to_time_bucket(
    dt: datetime,
    *,
    unit: Literal["hour", "day", "week", "month"],
    tz: timezone | ZoneInfo,
) -> datetime:
    """Floor *dt* to the start of *unit* in *tz* (weeks start Monday)."""

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

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

    raise exc.internal(f"Invalid time bucket unit: {unit!r}")
