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

from forze.application.contracts.durable.workflow import DurableWorkflowScheduleTiming
from forze.base.exceptions import CoreException
from forze_temporal.kernel.client.schedule_mapping import (
    calendar_to_cron,
    schedule_spec_to_timing,
    timing_to_schedule_spec,
)

# ----------------------- #


def _r(start: int, end: int = 0, step: int = 0) -> ScheduleRange:
    return ScheduleRange(start=start, end=end, step=step)


def _calendar(
    comment: str | None = None, **fields: tuple[ScheduleRange, ...]
) -> ScheduleCalendarSpec:
    base: dict[str, tuple[ScheduleRange, ...]] = {
        "second": (_r(0),),
        "minute": (_r(0),),
        "hour": (_r(0),),
        "day_of_month": (_r(1, 31),),
        "month": (_r(1, 12),),
        "day_of_week": (_r(0, 6),),
    }
    return ScheduleCalendarSpec(**{**base, **fields}, comment=comment)  # type: ignore[arg-type]


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


# ....................... #


@pytest.mark.parametrize(
    ("expression", "sent"),
    [
        ("0 9 * * MON-FRI", "0 9 * * MON-FRI # forze:cron=0 9 * * MON-FRI"),
        ("@daily", "@daily # forze:cron=@daily"),
        # Its own comment would be cut short, and the server moves a timezone prefix to the
        # schedule: both are sent as they are and read back rebuilt.
        ("0 3 * * * # nightly", "0 3 * * * # nightly"),
        ("CRON_TZ=Europe/Berlin 0 3 * * *", "CRON_TZ=Europe/Berlin 0 3 * * *"),
        ("TZ=UTC 0 3 * * *", "TZ=UTC 0 3 * * *"),
    ],
)
def test_an_expression_is_sent_with_its_text_as_the_calendar_comment(
    expression: str, sent: str
) -> None:
    timing = DurableWorkflowScheduleTiming(cron_expressions=(expression,))

    assert timing_to_schedule_spec(timing).cron_expressions == [sent]


def test_a_described_schedule_reports_its_expressions_as_written() -> None:
    """The server compiles ``0 9 * * MON-FRI`` into the calendar of ``0 9 * * 1-5``; the
    comment it keeps on that calendar is what is reported."""

    spec = ScheduleSpec(
        calendars=[
            _calendar(
                hour=(_r(9),),
                day_of_week=(_r(1, 5),),
                comment="forze:cron=0 9 * * MON-FRI",
            ),
            # A calendar without the comment: an older forze's, or one written outside forze.
            _calendar(hour=(_r(3),)),
            _calendar(hour=(_r(4),), comment="nightly"),
        ]
    )

    assert schedule_spec_to_timing(spec).cron_expressions == (
        "0 9 * * MON-FRI",
        "0 3 * * *",
        "0 4 * * * # nightly",
    )
