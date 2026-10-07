"""A described schedule's calendars read back as the cron expressions they were compiled from."""

from __future__ import annotations

from datetime import timedelta

import pytest

pytest.importorskip("temporalio")

from temporalio.client import (
    ScheduleCalendarSpec,
    ScheduleIntervalSpec,
    ScheduleRange,
    ScheduleSpec,
)

from forze.base.exceptions import CoreException
from forze_temporal.kernel.client.schedule_mapping import (
    calendar_to_cron,
    schedule_spec_to_timing,
)

# ----------------------- #


def _r(start: int, end: int = 0, step: int = 0) -> ScheduleRange:
    return ScheduleRange(start=start, end=end, step=step)


def _calendar(**fields: tuple[ScheduleRange, ...]) -> ScheduleCalendarSpec:
    base: dict[str, tuple[ScheduleRange, ...]] = {
        "second": (_r(0),),
        "minute": (_r(0),),
        "hour": (_r(0),),
        "day_of_month": (_r(1, 31),),
        "month": (_r(1, 12),),
        "day_of_week": (_r(0, 6),),
    }
    return ScheduleCalendarSpec(**{**base, **fields})  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("calendar", "cron"),
    [
        # What the server compiles "0 9 * * 1-5" into.
        (_calendar(hour=(_r(9),), day_of_week=(_r(1, 5),)), "0 9 * * 1-5"),
        # What it compiles "*/15 2 1 */2 *" into.
        (
            _calendar(
                minute=(_r(0, 59, 15),),
                hour=(_r(2),),
                day_of_month=(_r(1),),
                month=(_r(1, 12, 2),),
            ),
            "*/15 2 1 */2 *",
        ),
        (_calendar(minute=(_r(5), _r(10, 20), _r(30, 50, 10))), "5,10-20,30-50/10 0 * * *"),
        (_calendar(year=(_r(2030),)), "0 0 * * * 2030"),
        (_calendar(second=(_r(30),)), "30 0 0 * * * *"),
        (_calendar(second=(_r(0, 59, 20),), year=(_r(2030, 2032),)), "*/20 0 0 * * * 2030-2032"),
    ],
)
def test_a_calendar_reads_back_as_its_cron(calendar: ScheduleCalendarSpec, cron: str) -> None:
    assert calendar_to_cron(calendar) == cron


def test_a_described_cron_schedule_has_a_timing() -> None:
    """The server returns no cron expressions, only calendars; the timing still has both."""

    spec = ScheduleSpec(calendars=[_calendar(hour=(_r(9),), day_of_week=(_r(1, 5),))])

    assert schedule_spec_to_timing(spec).cron_expressions == ("0 9 * * 1-5",)


def test_a_calendar_that_never_fires_has_no_cron() -> None:
    """Only a hand-built calendar has a field matching nothing; it is dropped, not refused."""

    assert calendar_to_cron(_calendar(hour=())) is None

    spec = ScheduleSpec(calendars=[_calendar(hour=()), _calendar(hour=(_r(9),))])
    assert schedule_spec_to_timing(spec).cron_expressions == ("0 9 * * *",)


def test_a_calendar_comment_follows_its_cron() -> None:
    assert calendar_to_cron(ScheduleCalendarSpec(comment="note")) == "0 0 * * * # note"


@pytest.mark.parametrize(
    "spec",
    [
        ScheduleSpec(
            cron_expressions=["0 9 * * *"],
            skip=[_calendar(day_of_week=(_r(0),))],
        ),
        ScheduleSpec(
            intervals=[ScheduleIntervalSpec(every=timedelta(hours=1), offset=timedelta(minutes=5))]
        ),
        ScheduleSpec(
            intervals=[
                ScheduleIntervalSpec(every=timedelta(hours=1)),
                ScheduleIntervalSpec(every=timedelta(hours=3)),
            ]
        ),
    ],
    ids=["skip", "interval-offset", "two-intervals"],
)
def test_a_timing_the_description_would_misstate_is_unsupported(spec: ScheduleSpec) -> None:
    """Excluded periods, an interval offset or a second interval have no forze form; reporting
    the rest would describe a schedule that fires at other times than it does."""

    with pytest.raises(CoreException) as caught:
        schedule_spec_to_timing(spec)

    assert caught.value.code == "core.temporal.schedule_timing_unsupported"


def test_an_interval_without_an_offset_is_supported() -> None:
    spec = ScheduleSpec(
        intervals=[ScheduleIntervalSpec(every=timedelta(hours=1), offset=timedelta(0))]
    )

    assert schedule_spec_to_timing(spec).interval == timedelta(hours=1)
