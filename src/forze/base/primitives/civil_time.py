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

from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from functools import cache, wraps
from typing import Annotated, Final, final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones

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

CIVIL_TIME_OUT_OF_RANGE: Final[str] = "civil_time_out_of_range"
"""Code on a refused value whose answer falls outside years 1 to 9999, the calendar Python
represents."""

_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)


def _in_calendar[**P, R](function: Callable[P, R]) -> Callable[P, R]:
    """Refuse, rather than crash, when an answer falls outside years 1 to 9999.

    A local time in year 1 east of UTC is an instant in year 0, and the day after 31 December 9999
    is in year 10000: Python raises ``OverflowError`` for both, from deep inside a conversion.
    """

    @wraps(function)
    def _wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return function(*args, **kwargs)

        except OverflowError:
            raise exc.precondition(
                "The answer falls outside years 1 to 9999, the calendar Python represents.",
                code=CIVIL_TIME_OUT_OF_RANGE,
            ) from None

    return _wrapped


@final
@attrs.define(slots=True, frozen=True)
class CivilZone:
    """An IANA time zone, validated once where it is declared and injected where it is used."""

    key: str
    """The IANA name (``"Europe/Berlin"``)."""

    def __attrs_post_init__(self) -> None:
        # ZoneInfo opens any file under the zone path, including `right/` zones, which count leap
        # seconds and so disagree with every other clock by tens of seconds; only a listed name
        # is a zone.
        try:
            known = isinstance(self.key, str) and self.key in _zone_names()
            ZoneInfo(self.key)

        except (ZoneInfoNotFoundError, ValueError, TypeError):
            known = False

        if not known:
            raise exc.configuration(
                f"{self.key!r} is not an IANA time zone the zone database knows.",
                code="civil_zone_unknown",
            ) from None

    def zone(self) -> ZoneInfo:
        """The zone (``ZoneInfo`` caches it by key)."""

        return ZoneInfo(self.key)


@cache
def _zone_names() -> frozenset[str]:
    return frozenset(available_timezones())


# ....................... #


def _require_naive(local: datetime) -> None:
    if local.tzinfo is not None:
        raise exc.precondition(
            "A wall-clock time must be naive; an aware datetime is already an instant.",
            code="civil_time_aware",
        )


def _refuse_naive(start: datetime, end: datetime) -> None:
    if any(value.tzinfo is None or value.utcoffset() is None for value in (start, end)):
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


@_in_calendar
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
    """The first instant whose local date is *day* or later: midnight, the end of a gap over it,
    or, for a day the zone skips entirely, the next day's start."""

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


@_in_calendar
def local_day_bounds(zone: CivilZone, day: date) -> Period[datetime]:
    """The instants of local *day* in *zone*, half-open.

    Its length is whatever the zone's shifts make it: 23, 24 or 25 hours where a zone moves by
    an hour, 23.5 or 24.5 where it moves by half of one, 22 or 26 where it moves by two, and
    empty for a day the zone skips entirely (Samoa's 30 December 2011). A day whose local midnight
    the zone skips starts at the first instant that exists.
    """

    return Period(start=_start_of(zone, day), end=_start_of(zone, day + timedelta(days=1)))


@_in_calendar
def month_bounds(zone: CivilZone, year: int, month: int) -> Period[datetime]:
    """The instants of a local calendar month in *zone*, half-open."""

    if not 1 <= month <= 12:
        raise exc.precondition(f"A month is 1 to 12, not {month}.")

    first = date(year, month, 1)
    # Past the first by more than any month and back to its first day: after December 9999 that
    # is year 10000, which raises the OverflowError the decorator refuses.
    following = (first + timedelta(days=32)).replace(day=1)

    return Period(start=_start_of(zone, first), end=_start_of(zone, following))


@_in_calendar
def spanned_local_days(zone: CivilZone, start: datetime, end: datetime) -> tuple[date, ...]:
    """The local days in *zone* whose bounds the instant range ``[start, end)`` meets, in order.

    A day is its :func:`local_day_bounds`, so the days tile and a range's time is counted once.
    Where a zone repeats the hours around a midnight, the repeated stretch after the first
    midnight belongs to the new day, whatever the clock reads there; a day the zone skips is
    empty and never listed.

    :raises CoreException: ``precondition`` (``naive_datetime``) for a naive bound, or when
        *end* is before *start*.
    """

    _refuse_naive(start, end)

    if end - _EPOCH < start - _EPOCH:
        raise exc.precondition("The range ends before it starts.")

    if end - _EPOCH == start - _EPOCH:
        return ()

    one = timedelta(days=1)
    day = start.astimezone(zone.zone()).date()

    # The clock may still read the previous date after the day has begun (a fall-back across
    # midnight); the day holding *start* is the last one to begin at or before it.
    while _start_of(zone, day + one) <= start:
        day += one

    days: list[date] = []
    begins = _start_of(zone, day)

    while begins < end:
        ends = _start_of(zone, day + one)

        if ends > begins:
            days.append(day)

        day, begins = day + one, ends

    return tuple(days)


def elapsed_minutes(start: datetime, end: datetime) -> int:
    """Whole minutes between two instants, truncated toward zero.

    Instants only: across an offset change the wall-clock difference is off by the change, and
    the wrong number is a plausible one.

    :raises CoreException: ``precondition`` (``naive_datetime``) for a naive argument.
    """

    _refuse_naive(start, end)
    # Each as its distance from the UTC epoch: a subtraction across two zones reads instants, and
    # unlike a conversion to UTC it cannot leave the calendar (datetime.max in New York).
    delta = (end - _EPOCH) - (start - _EPOCH)
    whole = abs(delta) // timedelta(minutes=1)

    return whole if delta >= timedelta(0) else -whole


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
