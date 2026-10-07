"""Map Forze workflow schedule models to Temporal schedule types."""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Final

from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleCalendarSpec,
    ScheduleIntervalSpec,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
)

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleDescription,
    DurableWorkflowScheduleTiming,
)
from forze.base.exceptions import CoreException, exc

from .._logger import logger

# ----------------------- #

_CRON_FIELDS: Final = (
    ("second", 0, 59),
    ("minute", 0, 59),
    ("hour", 0, 23),
    ("day_of_month", 1, 31),
    ("month", 1, 12),
    ("day_of_week", 0, 6),
)
"""Calendar fields in cron order, with the bounds a ``*`` stands for."""

# ....................... #


def _cron_field(ranges: Sequence[ScheduleRange], bounds: tuple[int, int] | None) -> str:
    parts: list[str] = []

    for r in ranges:
        if r.start == r.end:
            parts.append(str(r.start))

        elif (r.start, r.end) == bounds:
            parts.append("*" if r.step == 1 else f"*/{r.step}")

        else:
            parts.append(f"{r.start}-{r.end}" + (f"/{r.step}" if r.step > 1 else ""))

    return ",".join(parts)


def calendar_to_cron(calendar: ScheduleCalendarSpec) -> str | None:
    """The cron expression a structured calendar matches the same times as.

    The server compiles each cron expression into a calendar and keeps only the calendar,
    so this is how a described or listed schedule reports its timing. The layouts are the
    server's own: five fields while the second is ``0`` and every year matches, six with a
    year, seven with a second (and ``*`` for any year); a calendar's comment follows as
    ``# comment``. ``None`` for a calendar with a field that matches nothing, which never
    fires (only a hand-built calendar has one).

    :param calendar: A calendar as the server describes it.
    :returns: The equivalent cron expression, or ``None`` for a calendar that never fires.
    """

    fields: dict[str, str] = {}

    for name, low, high in _CRON_FIELDS:
        if not (ranges := getattr(calendar, name)):
            return None

        fields[name] = _cron_field(ranges, (low, high))

    body = [fields[name] for name in ("minute", "hour", "day_of_month", "month", "day_of_week")]
    year = _cron_field(calendar.year, None)

    if fields["second"] != "0":
        cron = " ".join([fields["second"], *body, year or "*"])

    else:
        cron = " ".join([*body, year] if year else body)

    return f"{cron} # {calendar.comment}" if calendar.comment else cron


# ....................... #


def timing_to_schedule_spec(timing: DurableWorkflowScheduleTiming) -> ScheduleSpec:
    """Convert a :class:`WorkflowScheduleTiming` to Temporal ``ScheduleSpec``."""

    intervals: list[ScheduleIntervalSpec] = []
    if timing.interval is not None:
        intervals.append(ScheduleIntervalSpec(every=timing.interval))

    return ScheduleSpec(
        cron_expressions=list(timing.cron_expressions),
        intervals=intervals,
        start_at=timing.start_at,
        end_at=timing.end_at,
        jitter=timing.jitter,
        time_zone_name=timing.timezone,
    )


# ....................... #


def schedule_spec_to_timing(spec: ScheduleSpec) -> DurableWorkflowScheduleTiming:
    """Convert Temporal ``ScheduleSpec`` to :class:`WorkflowScheduleTiming`."""

    # The timing has one interval with no offset and nothing excluded; anything else would
    # describe a schedule that fires at other times than it does.
    if spec.skip or len(spec.intervals) > 1 or any(i.offset for i in spec.intervals):
        raise exc.precondition(
            "Schedule timing has no forze form: it skips calendar periods, offsets its "
            "interval, or runs on more than one interval.",
            code="core.temporal.schedule_timing_unsupported",
        )

    interval = spec.intervals[0].every if spec.intervals else None
    # A described schedule carries no cron expressions, only the calendars the server
    # compiled them into.
    cron = tuple(spec.cron_expressions) or tuple(
        expression for c in spec.calendars if (expression := calendar_to_cron(c)) is not None
    )

    if not cron and interval is None:
        raise exc.precondition(
            "Schedule timing has no forze form: it fires on no cron expression or interval "
            "(a trigger-only schedule, or calendars that never match).",
            code="core.temporal.schedule_timing_unsupported",
        )

    return DurableWorkflowScheduleTiming(
        cron_expressions=cron,
        interval=interval,
        start_at=spec.start_at,
        end_at=spec.end_at,
        jitter=spec.jitter,
        timezone=spec.time_zone_name,
    )


# ....................... #


def build_start_workflow_action(
    *,
    workflow_name: str,
    queue: str,
    arg: object,
    workflow_id: str,
) -> ScheduleActionStartWorkflow:
    """Build a Temporal schedule action that starts a workflow run."""

    return ScheduleActionStartWorkflow(
        workflow_name,
        arg,
        id=workflow_id,
        task_queue=queue,
    )


# ....................... #


def build_schedule(
    *,
    workflow_name: str,
    queue: str,
    arg: object,
    workflow_id: str,
    timing: DurableWorkflowScheduleTiming,
    note: str | None = None,
) -> Schedule:
    """Build a Temporal :class:`Schedule` from Forze inputs."""

    return Schedule(
        action=build_start_workflow_action(
            workflow_name=workflow_name,
            queue=queue,
            arg=arg,
            workflow_id=workflow_id,
        ),
        spec=timing_to_schedule_spec(timing),
        state=ScheduleState(note=note or ""),
    )


# ....................... #


def resolve_scheduled_workflow_id(
    schedule_id: str,
    *,
    workflow_id_base: str | None,
) -> str:
    """Resolve the workflow id used for each scheduled workflow start."""

    if workflow_id_base is not None:
        return workflow_id_base

    return f"{schedule_id}-scheduled"


# ....................... #


def description_from_temporal(
    desc: object,
    *,
    workflow_name: str,
) -> DurableWorkflowScheduleDescription:
    """Convert a Temporal :class:`ScheduleDescription` to Forze form."""

    from temporalio.client import ScheduleDescription

    if not isinstance(desc, ScheduleDescription):
        raise TypeError("expected ScheduleDescription")

    action = desc.schedule.action

    if not isinstance(action, ScheduleActionStartWorkflow):
        msg = "schedule action is not ScheduleActionStartWorkflow"
        raise TypeError(msg)

    timing = schedule_spec_to_timing(desc.schedule.spec)
    next_times = tuple(
        t.replace(tzinfo=UTC) if t.tzinfo is None else t for t in desc.info.next_action_times
    )

    return DurableWorkflowScheduleDescription(
        schedule_id=desc.id,
        workflow_name=workflow_name or action.workflow,
        paused=desc.schedule.state.paused,
        timing=timing,
        note=desc.schedule.state.note or None,
        next_run_times=next_times,
    )


# ....................... #


def description_from_list_entry(
    entry: object,
) -> DurableWorkflowScheduleDescription | None:
    """Convert a Temporal :class:`ScheduleListDescription` to Forze form."""

    from temporalio.client import (
        ScheduleListActionStartWorkflow,
        ScheduleListDescription,
    )

    if not isinstance(entry, ScheduleListDescription):
        raise TypeError("expected ScheduleListDescription")

    if entry.schedule is None:
        return None

    action = entry.schedule.action

    if not isinstance(action, ScheduleListActionStartWorkflow):
        return None

    try:
        timing = schedule_spec_to_timing(entry.schedule.spec)

    except CoreException:
        # A schedule built outside forze (a trigger-only spec, calendars that never fire) has
        # no timing forze can express; one such entry must not fail the whole listing.
        logger.warning(
            "Skipping a listed Temporal schedule whose timing has no forze form",
            schedule_id=entry.id,
        )
        return None

    next_times: tuple[datetime, ...] = ()

    if entry.info is not None:
        next_times = tuple(
            t.replace(tzinfo=UTC) if t.tzinfo is None else t for t in entry.info.next_action_times
        )

    return DurableWorkflowScheduleDescription(
        schedule_id=entry.id,
        workflow_name=action.workflow,
        paused=entry.schedule.state.paused,
        timing=timing,
        note=entry.schedule.state.note or None,
        next_run_times=next_times,
    )
