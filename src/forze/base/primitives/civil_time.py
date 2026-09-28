"""Civil time: a wall clock read in a zone, and the instants it names.

A wall-clock time and a zone name an instant on almost every day of the year. On the two days
a year a zone changes offset they do not: a fall-back local time happens **twice**, and a
spring-forward one **never** happens. Picking an instant silently is how an hour of working time
vanishes or doubles with no signal, so :func:`to_instant` refuses both — the first unless the
caller says which occurrence it meant (``fold``), the second always.

The zone is a value the application injects (:class:`CivilZone`), never a module constant or a
process setting: a constant is invisible at the call site and untestable across zones.

Arithmetic on durations is done on instants only (:func:`elapsed_minutes` refuses a naive
value): subtracting two wall-clock times across a transition is off by the offset change, and
the result is a plausible number.
"""

from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from typing import Annotated, Final, final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import attrs
from pydantic import AfterValidator
from pydantic_core import PydanticCustomError

from forze.base.exceptions import exc

from .period import Period

# ----------------------- #

DST_AMBIGUOUS: Final[str] = "dst_ambiguous"
"""Code on a refused local time that happens twice (fall back)."""

DST_NONEXISTENT: Final[str] = "dst_nonexistent"
"""Code on a refused local time that never happens (spring forward)."""

NAIVE_DATETIME: Final[str] = "naive_datetime"
"""Code on a refused naive datetime where an instant is required."""


@final
@attrs.define(slots=True, frozen=True)
class CivilZone:
    """An IANA time zone, validated once where it is declared and injected where it is used."""

    key: str
    """The IANA name (``"Europe/Berlin"``)."""

    def __attrs_post_init__(self) -> None:
        try:
            ZoneInfo(self.key)

        except (ZoneInfoNotFoundError, ValueError, TypeError):
            raise exc.configuration(
                f"{self.key!r} is not an IANA time zone the zone database knows.",
                code="civil_zone_unknown",
            ) from None

    def zone(self) -> ZoneInfo:
        """The zone (``ZoneInfo`` caches it by key)."""

        return ZoneInfo(self.key)


# ....................... #


def _require_naive(local: datetime) -> None:
    if local.tzinfo is not None:
        raise exc.precondition(
            "A wall-clock time must be naive; an aware datetime is already an instant.",
            code="civil_time_aware",
        )


def _require_aware(*values: datetime) -> None:
    if any(value.tzinfo is None or value.utcoffset() is None for value in values):
        raise exc.precondition(
            "An instant is required, and a naive datetime is a wall-clock time in no zone.",
            code=NAIVE_DATETIME,
        )


def _readings(zone: CivilZone, local: datetime) -> tuple[datetime, datetime, bool, bool]:
    """Both interpretations of *local* as UTC instants, and whether each reads back as *local*."""

    tz = zone.zone()
    early = local.replace(tzinfo=tz, fold=0).astimezone(UTC)
    late = local.replace(tzinfo=tz, fold=1).astimezone(UTC)

    def back(instant: datetime) -> bool:
        return instant.astimezone(tz).replace(tzinfo=None) == local

    return early, late, back(early), back(late)


def to_instant(zone: CivilZone, local: datetime, *, fold: int | None = None) -> datetime:
    """The UTC instant a naive wall-clock *local* time names in *zone*.

    :raises CoreException: ``precondition`` — ``dst_nonexistent`` for a local time the zone
        skips; ``dst_ambiguous`` for one it repeats, unless *fold* (``0`` for the first
        occurrence, ``1`` for the second) says which is meant.
    """

    _require_naive(local)

    if fold not in (None, 0, 1):
        raise exc.precondition("fold is 0 (the first occurrence) or 1 (the second).")

    early, late, early_reads, late_reads = _readings(zone, local)

    if not early_reads and not late_reads:
        raise exc.precondition(
            f"{local.isoformat()} does not occur in {zone.key}: the clock skips it. There is no "
            "instant to return, and moving it is a guess about what was meant.",
            code=DST_NONEXISTENT,
        )

    if early != late:
        if fold is None:
            raise exc.precondition(
                f"{local.isoformat()} occurs twice in {zone.key}; pass fold=0 for the first "
                "occurrence or fold=1 for the second.",
                code=DST_AMBIGUOUS,
            )

        return early if fold == 0 else late

    return early


# ....................... #


def _start_of(zone: CivilZone, day: date) -> datetime:
    """The first instant whose local date is *day* — midnight, or the end of a gap over it."""

    early, late, early_reads, late_reads = _readings(zone, datetime.combine(day, time()))

    if early_reads or late_reads:
        return min(
            instant for instant, reads in ((early, early_reads), (late, late_reads)) if reads
        )

    # Midnight falls in a gap: the day starts at the transition, which neither reading gives in
    # general (a gap need not start at midnight). Local time is monotonic across the gap, so
    # search the whole seconds between the two readings for the first one on *day*.
    tz = zone.zone()
    low, high = sorted((early, late))

    while high - low > timedelta(seconds=1):
        middle = low + (high - low) / 2
        middle = middle.replace(microsecond=0)

        if middle.astimezone(tz).date() >= day:
            high = middle

        else:
            low = middle

    return high


def local_day_bounds(zone: CivilZone, day: date) -> Period[datetime]:
    """The instants of local *day* in *zone*, half-open — 23, 24 or 25 hours, and 23.5 or 24.5
    where a zone shifts by half an hour.

    A day whose local midnight the zone skips starts at the first instant that exists: a day
    always has a first moment even when 00:00 is not it.
    """

    return Period(start=_start_of(zone, day), end=_start_of(zone, day + timedelta(days=1)))


def month_bounds(zone: CivilZone, year: int, month: int) -> Period[datetime]:
    """The instants of a local calendar month in *zone*, half-open."""

    first = date(year, month, 1)
    following = date(year + month // 12, month % 12 + 1, 1)

    return Period(start=_start_of(zone, first), end=_start_of(zone, following))


def spanned_local_days(zone: CivilZone, start: datetime, end: datetime) -> tuple[date, ...]:
    """The local days in *zone* that the instant range ``[start, end)`` touches, in order.

    :raises CoreException: ``precondition`` (``naive_datetime``) for a naive bound, or when
        *end* is before *start*.
    """

    _require_aware(start, end)

    if end < start:
        raise exc.precondition("The range ends before it starts.")

    if end == start:
        return ()

    tz = zone.zone()
    first = start.astimezone(tz).date()
    last = (end - timedelta(microseconds=1)).astimezone(tz).date()

    return tuple(_days(first, last))


def _days(first: date, last: date) -> Iterator[date]:
    day = first

    while day <= last:
        yield day
        day += timedelta(days=1)


def elapsed_minutes(start: datetime, end: datetime) -> int:
    """Whole minutes between two instants, truncated toward zero.

    Instants only: across an offset change the wall-clock difference is off by the change, and
    the wrong number is a plausible one.

    :raises CoreException: ``precondition`` (``naive_datetime``) for a naive argument.
    """

    _require_aware(start, end)

    return int((end - start) / timedelta(minutes=1))


# ....................... #


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise PydanticCustomError(
            NAIVE_DATETIME,
            "a naive datetime has no time zone; send an instant with an offset",
        )

    return value


AwareDatetime = Annotated[datetime, AfterValidator(_aware)]
"""A datetime field for an application's boundary models that refuses a naive value, with the
error code ``naive_datetime`` and the field's name in the location."""
