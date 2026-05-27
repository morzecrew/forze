from datetime import datetime, timedelta
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

    cron_expressions: tuple[str, ...] = ()
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

    next_run_times: tuple[datetime, ...] = ()
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

    workflow_id_template: str | None = None  #! very questionable thing
    """Optional workflow id template for each fired run."""

    trigger_immediately: bool = False
    """Whether to fire once immediately after create/upsert."""

    note: str | None = None
    """Optional operator note."""
