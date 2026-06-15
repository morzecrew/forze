from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Generic, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import BaseSpec
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowInvokeSpec(Generic[In, Out]):
    """Specification for abstract invocation within a workflow."""

    args_type: type[In]
    """The type of the arguments for the abstract invocation."""

    return_type: type[Out] | None = attrs.field(default=None)
    """The type of the return value for the abstract invocation."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowSignalSpec(DurableWorkflowInvokeSpec[In, Any], Generic[In]):
    """Specification for a signal invocation within a workflow."""

    name: StrKey
    """The name of the signal."""

    return_type: None = attrs.field(default=None, init=False)
    """Signal operations don't return a value."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowQuerySpec(DurableWorkflowInvokeSpec[In, Out], Generic[In, Out]):
    """Specification for a query invocation within a workflow."""

    name: StrKey
    """The name of the query."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowUpdateSpec(DurableWorkflowInvokeSpec[In, Out], Generic[In, Out]):
    """Specification for an update invocation within a workflow."""

    name: StrKey
    """The name of the update."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowSpec(Generic[In, Out], BaseSpec):
    """Specification for a workflow."""

    run: DurableWorkflowInvokeSpec[In, Out]
    """The main invocation of the workflow."""

    signals: dict[StrKey, DurableWorkflowSignalSpec[Any]] = attrs.field(factory=dict)
    """Signal invocations within the workflow."""

    queries: dict[StrKey, DurableWorkflowQuerySpec[Any, Any]] = attrs.field(
        factory=dict
    )
    """Query invocations within the workflow."""

    updates: dict[StrKey, DurableWorkflowUpdateSpec[Any, Any]] = attrs.field(
        factory=dict
    )
    """Update invocations within the workflow."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowHandle:
    """Handle for a workflow run."""

    workflow_id: str
    """The id of the workflow."""

    run_id: str | None = attrs.field(default=None)
    """The id of the run."""


# ....................... #


class DurableWorkflowRunStatus(StrEnum):
    """Coarse lifecycle status of a workflow run."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TERMINATED = "terminated"
    CONTINUED_AS_NEW = "continued_as_new"
    TIMED_OUT = "timed_out"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowRunDescription:
    """Coarse description of a workflow run returned by query ports."""

    workflow_id: str
    """Workflow execution identifier."""

    run_id: str
    """Run identifier for this execution."""

    workflow_name: StrKey
    """Logical workflow name (``DurableWorkflowSpec.name``)."""

    status: DurableWorkflowRunStatus
    """Coarse execution status."""

    started_at: datetime | None = None
    """When the run started (UTC when the provider supplies a timezone)."""

    closed_at: datetime | None = None
    """When the run closed, if applicable."""

    failure_message: str | None = None
    """Human-readable failure detail when ``status`` is terminal failure."""

    failure_type: str | None = None
    """Failure type name when the provider exposes it."""

    # ....................... #

    @property
    def is_terminal(self) -> bool:
        """Whether the run has reached a terminal coarse status."""

        return self.status in (
            DurableWorkflowRunStatus.COMPLETED,
            DurableWorkflowRunStatus.FAILED,
            DurableWorkflowRunStatus.CANCELLED,
            DurableWorkflowRunStatus.TERMINATED,
            DurableWorkflowRunStatus.TIMED_OUT,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowScheduleHandle:
    """Handle for a workflow schedule resource."""

    schedule_id: str
    """Stable schedule identifier (may be tenant-prefixed when tenant-aware)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowScheduleTiming:
    """Provider-agnostic schedule timing specification."""

    cron_expressions: tuple[str, ...] = attrs.field(factory=tuple)
    """Cron expressions (provider interprets timezone; Temporal uses UTC by default)."""

    interval: timedelta | None = None
    """Fixed interval between runs."""

    start_at: datetime | None = None
    """Earliest time the schedule may fire."""

    end_at: datetime | None = None
    """Latest time the schedule may fire."""

    jitter: timedelta | None = None
    """Random delay bound applied to each scheduled fire."""

    timezone: str | None = None
    """Reserved for providers that support named timezones."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.cron_expressions and self.interval is None:
            raise exc.validation(
                "DurableWorkflowScheduleTiming requires cron_expressions or interval",
            )

        if self.interval is not None and self.interval.total_seconds() <= 0:
            raise exc.configuration("Interval must be positive")

        if self.jitter is not None and self.jitter.total_seconds() <= 0:
            raise exc.configuration("Jitter must be positive")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowScheduleDescription:
    """Description of a workflow schedule returned by query ports."""

    schedule_id: str
    """Schedule identifier."""

    workflow_name: StrKey
    """Logical workflow name (``DurableWorkflowSpec.name``)."""

    paused: bool
    """Whether the schedule is paused."""

    timing: DurableWorkflowScheduleTiming
    """Normalized timing specification."""

    note: str | None = None
    """Optional operator note."""

    next_run_times: tuple[datetime, ...] = attrs.field(factory=tuple)
    """Upcoming fire times when the provider exposes them."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableWorkflowScheduleBootstrap(Generic[In]):
    """Declarative schedule registered at application startup."""

    workflow_name: StrKey
    """Route key matching ``DurableWorkflowSpec.name``."""

    schedule_id: str
    """Stable schedule identifier."""

    default_args: In
    """Default workflow run arguments."""

    timing: DurableWorkflowScheduleTiming
    """When the schedule fires."""

    workflow_id_base: str | None = None
    """Optional base workflow id for each fired run.

    Passed through to the backend verbatim — Forze performs no placeholder
    interpolation. Backends derive per-run uniqueness themselves (e.g. Temporal
    appends the fire timestamp to the id of every scheduled run). When unset,
    backends fall back to a ``<schedule_id>``-derived id.
    """

    trigger_immediately: bool = False
    """Whether to fire once immediately after create/upsert."""

    note: str | None = None
    """Optional operator note."""
