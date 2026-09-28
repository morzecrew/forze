"""Civil time across the spring-forward and fall-back days of three differently shaped zones.

Berlin moves an hour at 02:00; Santiago moves at midnight, so a whole local midnight is skipped;
Lord Howe moves by thirty minutes, so any hour-shaped assumption fails there. The transition
times below were read from the zone database, not recalled.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfo

import pytest
from pydantic import BaseModel, ValidationError

from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import (
    DST_AMBIGUOUS,
    DST_NONEXISTENT,
    NAIVE_DATETIME,
    AwareDatetime,
    CivilZone,
    elapsed_minutes,
    local_day_bounds,
    month_bounds,
    spanned_local_days,
    to_instant,
)

# ----------------------- #

BERLIN: Final = CivilZone("Europe/Berlin")
SANTIAGO: Final = CivilZone("America/Santiago")
LORD_HOWE: Final = CivilZone("Australia/Lord_Howe")

# zone, a skipped local time, a repeated local time, the offset change in minutes
TRANSITIONS: Final = [
    (BERLIN, datetime(2026, 3, 29, 2, 30), datetime(2026, 10, 25, 2, 30), 60),
    (SANTIAGO, datetime(2026, 9, 6, 0, 30), datetime(2026, 4, 4, 23, 30), 60),
    (LORD_HOWE, datetime(2026, 10, 4, 2, 15), datetime(2026, 4, 5, 1, 45), 30),
]
IDS: Final = ["berlin", "santiago", "lord-howe"]


def _refused(code: str) -> pytest.RaisesExc[CoreException]:
    return pytest.raises(
        CoreException, check=lambda e: e.kind is ExceptionKind.PRECONDITION and e.code == code
    )


# ....................... #


class TestToInstant:
    @pytest.mark.parametrize(("zone", "skipped", "repeated", "shift"), TRANSITIONS, ids=IDS)
    def test_a_repeated_local_time_is_refused_without_fold(
        self, zone: CivilZone, skipped: datetime, repeated: datetime, shift: int
    ) -> None:
        with _refused(DST_AMBIGUOUS):
            to_instant(zone, repeated)

    @pytest.mark.parametrize(("zone", "skipped", "repeated", "shift"), TRANSITIONS, ids=IDS)
    def test_fold_names_one_of_two_instants_the_shift_apart(
        self, zone: CivilZone, skipped: datetime, repeated: datetime, shift: int
    ) -> None:
        first = to_instant(zone, repeated, fold=0)
        second = to_instant(zone, repeated, fold=1)

        assert second - first == timedelta(minutes=shift)
        assert first.tzinfo is UTC and second.tzinfo is UTC

    @pytest.mark.parametrize(("zone", "skipped", "repeated", "shift"), TRANSITIONS, ids=IDS)
    @pytest.mark.parametrize("fold", [None, 0, 1])
    def test_a_skipped_local_time_is_always_refused(
        self, zone: CivilZone, skipped: datetime, repeated: datetime, shift: int, fold: int | None
    ) -> None:
        with _refused(DST_NONEXISTENT):
            to_instant(zone, skipped, fold=fold)

    def test_an_ordinary_time_needs_no_fold(self) -> None:
        assert to_instant(BERLIN, datetime(2026, 6, 1, 12, 0)) == datetime(
            2026, 6, 1, 10, 0, tzinfo=UTC
        )

    def test_an_aware_value_is_not_a_wall_clock_time(self) -> None:
        with _refused("civil_time_aware"):
            to_instant(BERLIN, datetime(2026, 6, 1, 12, 0, tzinfo=UTC))

    def test_fold_is_zero_or_one(self) -> None:
        with pytest.raises(CoreException):
            to_instant(BERLIN, datetime(2026, 10, 25, 2, 30), fold=2)


class TestDaysAndMonths:
    @pytest.mark.parametrize(
        ("zone", "day", "hours"),
        [
            (BERLIN, date(2026, 3, 29), 23),
            (BERLIN, date(2026, 10, 25), 25),
            (BERLIN, date(2026, 6, 1), 24),
            (SANTIAGO, date(2026, 9, 6), 23),
            (SANTIAGO, date(2026, 4, 4), 25),
            (LORD_HOWE, date(2026, 10, 4), 23.5),
            (LORD_HOWE, date(2026, 4, 5), 24.5),
        ],
        ids=[
            "berlin-spring",
            "berlin-fall",
            "berlin-plain",
            "santiago-spring",
            "santiago-fall",
            "lord-howe-spring",
            "lord-howe-fall",
        ],
    )
    def test_a_transition_day_has_the_length_it_has(
        self, zone: CivilZone, day: date, hours: float
    ) -> None:
        bounds = local_day_bounds(zone, day)

        assert bounds.bounds == "[)"
        assert bounds.end is not None and bounds.end - bounds.start == timedelta(hours=hours)

    def test_a_skipped_midnight_starts_the_day_at_the_first_instant_that_exists(self) -> None:
        # Santiago skips 00:00-00:59 on 6 Sep: the day starts at 01:00 local, the transition.
        start = local_day_bounds(SANTIAGO, date(2026, 9, 6)).start

        assert start == datetime(2026, 9, 6, 4, 0, tzinfo=UTC)
        assert start.astimezone(SANTIAGO.zone()).hour == 1

    def test_a_gap_straddling_midnight_starts_the_day_where_the_gap_ends(self) -> None:
        # Toronto jumped from 23:30 to 00:30 on 31 Mar 1919: midnight sits thirty minutes inside
        # the gap, so neither reading of 00:00 is the day's first instant — 00:30 local is.
        toronto = CivilZone("America/Toronto")
        start = local_day_bounds(toronto, date(1919, 3, 31)).start

        assert start == datetime(1919, 3, 31, 4, 30, tzinfo=UTC)
        assert start.astimezone(toronto.zone()).strftime("%H:%M") == "00:30"

    def test_a_repeated_midnight_starts_the_day_at_its_first_occurrence(self) -> None:
        # Havana repeats 00:00-00:59 on 1 Nov: the day starts at the first midnight, not the
        # second, and is 25 hours long.
        havana = CivilZone("America/Havana")
        bounds = local_day_bounds(havana, date(2026, 11, 1))

        assert bounds.start == datetime(2026, 11, 1, 4, 0, tzinfo=UTC)
        assert bounds.end is not None and bounds.end - bounds.start == timedelta(hours=25)

    def test_consecutive_days_tile(self) -> None:
        for zone in (BERLIN, SANTIAGO, LORD_HOWE):
            day = date(2026, 3, 25)

            for _ in range(200):
                assert (
                    local_day_bounds(zone, day).end
                    == local_day_bounds(zone, day + timedelta(days=1)).start
                )
                day += timedelta(days=1)

    @pytest.mark.parametrize(
        ("zone", "year", "month", "hours"),
        [
            (BERLIN, 2026, 3, 31 * 24 - 1),
            (BERLIN, 2026, 10, 31 * 24 + 1),
            (BERLIN, 2026, 12, 31 * 24),
            (SANTIAGO, 2026, 9, 30 * 24 - 1),
            (LORD_HOWE, 2026, 4, 30 * 24 + 0.5),
        ],
        ids=[
            "berlin-march",
            "berlin-october",
            "berlin-december",
            "santiago-september",
            "lord-howe-april",
        ],
    )
    def test_a_month_spans_its_local_days(
        self, zone: CivilZone, year: int, month: int, hours: float
    ) -> None:
        bounds = month_bounds(zone, year, month)

        assert bounds.end is not None and bounds.end - bounds.start == timedelta(hours=hours)


class TestSpannedDays:
    @pytest.mark.parametrize("zone", [BERLIN, SANTIAGO, LORD_HOWE], ids=IDS)
    def test_a_range_across_a_transition_has_no_gap_or_repeat(self, zone: CivilZone) -> None:
        start = local_day_bounds(zone, date(2026, 3, 27)).start
        end = local_day_bounds(zone, date(2026, 4, 8)).start

        days = spanned_local_days(zone, start, end)

        assert days == tuple(date(2026, 3, 27) + timedelta(days=n) for n in range(12))

    def test_the_end_is_exclusive_and_an_empty_range_spans_nothing(self) -> None:
        start = local_day_bounds(BERLIN, date(2026, 6, 1)).start

        assert spanned_local_days(BERLIN, start, start + timedelta(days=1)) == (date(2026, 6, 1),)
        assert spanned_local_days(BERLIN, start, start) == ()

    def test_an_empty_range_inside_a_day_spans_nothing(self) -> None:
        noon = to_instant(BERLIN, datetime(2026, 6, 1, 12, 0))

        assert spanned_local_days(BERLIN, noon, noon) == ()

    def test_values_carrying_the_zone_itself_are_ordered_as_instants(self) -> None:
        # The same pair: backwards on the wall clock, forwards in fact.
        tz = BERLIN.zone()
        first = datetime(2026, 10, 25, 2, 30, fold=0, tzinfo=tz)
        second = datetime(2026, 10, 25, 2, 15, fold=1, tzinfo=tz)

        assert spanned_local_days(BERLIN, first, second) == (date(2026, 10, 25),)

    def test_a_backwards_or_naive_range_is_refused(self) -> None:
        start = datetime(2026, 6, 1, tzinfo=UTC)

        with pytest.raises(CoreException):
            spanned_local_days(BERLIN, start, start - timedelta(hours=1))

        with _refused(NAIVE_DATETIME):
            spanned_local_days(BERLIN, datetime(2026, 6, 1), start)


class TestElapsed:
    @pytest.mark.parametrize(("zone", "skipped", "repeated", "shift"), TRANSITIONS, ids=IDS)
    def test_across_a_transition_it_is_the_true_elapsed_time(
        self, zone: CivilZone, skipped: datetime, repeated: datetime, shift: int
    ) -> None:
        # Two hours either side of a repeated hour: four hours on the clock, four plus the shift
        # in fact — the arithmetic a wall-clock subtraction silently gets wrong.
        before, after = repeated - timedelta(hours=2), repeated + timedelta(hours=2)

        assert after - before == timedelta(hours=4)
        assert elapsed_minutes(to_instant(zone, before), to_instant(zone, after)) == 4 * 60 + shift

    def test_a_naive_argument_is_refused(self) -> None:
        with _refused(NAIVE_DATETIME):
            elapsed_minutes(datetime(2026, 6, 1), datetime(2026, 6, 2, tzinfo=UTC))

    def test_values_carrying_the_zone_itself_are_measured_as_instants(self) -> None:
        # Python subtracts two datetimes sharing a tzinfo by their wall clocks. Across Berlin's
        # repeated hour, the first 02:30 to the second 02:15 reads as -15 minutes on the clock.
        # It is 45 minutes in fact.
        tz = BERLIN.zone()
        first = datetime(2026, 10, 25, 2, 30, fold=0, tzinfo=tz)
        second = datetime(2026, 10, 25, 2, 15, fold=1, tzinfo=tz)

        assert elapsed_minutes(first, second) == 45

    def test_it_truncates_toward_zero(self) -> None:
        start = datetime(2026, 6, 1, tzinfo=UTC)

        assert elapsed_minutes(start, start + timedelta(seconds=119)) == 1
        assert elapsed_minutes(start, start - timedelta(seconds=119)) == -1


class TestTheZoneAndTheBoundary:
    @pytest.mark.parametrize("key", ["Not/AZone", "", "+03:00"])
    def test_an_unknown_zone_is_refused_where_it_is_declared(self, key: str) -> None:
        with pytest.raises(CoreException) as caught:
            CivilZone(key)

        assert caught.value.code == "civil_zone_unknown"

    def test_a_naive_value_at_the_boundary_is_refused_by_name(self) -> None:
        class Shift(BaseModel):
            starts_at: AwareDatetime

        with pytest.raises(ValidationError) as caught:
            Shift(starts_at=datetime(2026, 6, 1, 9, 0))

        [error] = caught.value.errors()
        assert (error["type"], error["loc"]) == (NAIVE_DATETIME, ("starts_at",))
        assert Shift(starts_at=datetime(2026, 6, 1, 9, 0, tzinfo=UTC)).starts_at.tzinfo is UTC


class TestTheZoneDatabaseEdges:
    APIA = CivilZone("Pacific/Apia")

    def test_a_day_the_zone_skips_is_empty_and_not_spanned(self) -> None:
        # Samoa skipped 30 Dec 2011 entirely, moving across the date line.
        skipped = local_day_bounds(self.APIA, date(2011, 12, 30))
        start = local_day_bounds(self.APIA, date(2011, 12, 31)).start

        assert skipped.end == skipped.start
        assert spanned_local_days(
            self.APIA, start - timedelta(hours=1), start + timedelta(hours=1)
        ) == (date(2011, 12, 29), date(2011, 12, 31))

    @pytest.mark.parametrize(
        ("key", "start"),
        [
            # A fall-back across midnight: at 00:01 the clock returned to 23:01 of the day before.
            ("America/St_Johns", datetime(2010, 11, 7, 2, 30, 30, tzinfo=UTC)),
            ("America/St_Johns", datetime(2010, 11, 7, 2, 40, tzinfo=UTC)),
            # Sitka took the American date in Oct 1867 and read the 18th a second time.
            ("America/Sitka", datetime(1867, 10, 19, 0, 0, tzinfo=UTC)),
            ("America/Sitka", datetime(1867, 10, 19, 2, 0, tzinfo=UTC)),
            # Samoa skipped 30 Dec 2011.
            ("Pacific/Apia", datetime(2011, 12, 30, 9, 0, tzinfo=UTC)),
        ],
        ids=["st-johns-first-midnight", "st-johns-repeat", "sitka-before", "sitka-after", "apia"],
    )
    @pytest.mark.parametrize("hours", [0.5, 3, 30])
    def test_the_listed_days_cover_the_range_once(
        self, key: str, start: datetime, hours: float
    ) -> None:
        # Summed over its days, a range's time is all there and counted once: the property a
        # timesheet split by day rests on, and the one a day read off the clock breaks where the
        # clock repeats a midnight.
        zone = CivilZone(key)
        end = start + timedelta(hours=hours)
        covered = timedelta(0)

        for day in spanned_local_days(zone, start, end):
            bounds = local_day_bounds(zone, day)
            assert bounds.end is not None and bounds.end > start and bounds.start < end, day
            covered += min(end, bounds.end) - max(start, bounds.start)

        assert covered == end - start

    def test_a_two_hour_shift_gives_22_and_26_hour_days(self) -> None:
        troll = CivilZone("Antarctica/Troll")

        for day, hours in ((date(2026, 3, 29), 22), (date(2026, 10, 25), 26)):
            bounds = local_day_bounds(troll, day)
            assert bounds.end is not None and bounds.end - bounds.start == timedelta(hours=hours)

    @pytest.mark.parametrize("key", ["right/Europe/Berlin", "posix/Europe/Berlin", "posixrules"])
    def test_a_file_that_is_not_a_zone_name_is_refused(self, key: str) -> None:
        # ZoneInfo opens these, and the right/ ones count leap seconds: a result off by 27 s.
        with pytest.raises(CoreException) as caught:
            CivilZone(key)

        assert caught.value.code == "civil_zone_unknown"

    def test_elapsed_minutes_truncates_exactly_over_long_spans(self) -> None:
        start = datetime(1000, 1, 1, tzinfo=UTC)
        minutes = 2**29

        assert elapsed_minutes(start, start + timedelta(minutes=minutes, microseconds=-1)) == (
            minutes - 1
        )
        assert elapsed_minutes(start + timedelta(minutes=minutes, microseconds=-1), start) == -(
            minutes - 1
        )

    def test_elapsed_minutes_reaches_the_end_of_the_calendar(self) -> None:
        # Converted to UTC, datetime.max in New York is past year 9999.
        forever = datetime.max.replace(tzinfo=ZoneInfo("America/New_York"))

        assert elapsed_minutes(datetime(9999, 12, 31, tzinfo=UTC), forever) == 24 * 60 + 5 * 60 - 1

    @pytest.mark.parametrize("month", [0, 13])
    def test_a_month_outside_the_calendar_is_refused(self, month: int) -> None:
        with pytest.raises(CoreException):
            month_bounds(BERLIN, 2026, month)
