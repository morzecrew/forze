"""Map Forze workflow schedule models to Temporal schedule types."""

from datetime import datetime, timezone

from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
    ScheduleState,
)

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleDescription,
    DurableWorkflowScheduleTiming,
)

# ----------------------- #


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

    interval = spec.intervals[0].every if spec.intervals else None

    return DurableWorkflowScheduleTiming(
        cron_expressions=tuple(spec.cron_expressions),
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
        t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t
        for t in desc.info.next_action_times
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

    timing = schedule_spec_to_timing(entry.schedule.spec)
    next_times: tuple[datetime, ...] = ()
    if entry.info is not None:
        next_times = tuple(
            t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t
            for t in entry.info.next_action_times
        )

    return DurableWorkflowScheduleDescription(
        schedule_id=entry.id,
        workflow_name=action.workflow,
        paused=entry.schedule.state.paused,
        timing=timing,
        note=entry.schedule.state.note or None,
        next_run_times=next_times,
    )
