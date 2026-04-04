from typing import Any, Generic, TypeVar, final

import attrs
from pydantic import BaseModel

from ..base import BaseSpec

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowInvokeSpec(Generic[In, Out]):
    """Specification for abstract invocation within a workflow."""

    args_type: type[In]
    """The type of the arguments for the abstract invocation."""

    return_type: type[Out] | None = None
    """The type of the return value for the abstract invocation."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowSignalSpec(WorkflowInvokeSpec[In, Any], Generic[In]):
    """Specification for a signal invocation within a workflow."""

    name: str
    """The name of the signal."""

    return_type: None = attrs.field(default=None, init=False)
    """Signal operations don't return a value."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowQuerySpec(WorkflowInvokeSpec[In, Out], Generic[In, Out]):
    """Specification for a query invocation within a workflow."""

    name: str
    """The name of the query."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowUpdateSpec(WorkflowInvokeSpec[In, Out], Generic[In, Out]):
    """Specification for an update invocation within a workflow."""

    name: str
    """The name of the update."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowSpec(Generic[In, Out], BaseSpec):
    """Specification for a workflow."""

    run: WorkflowInvokeSpec[In, Out]
    """The main invocation of the workflow."""

    signals: dict[str, WorkflowSignalSpec[Any]] = attrs.field(factory=dict)
    """Signal invocations within the workflow."""

    queries: dict[str, WorkflowQuerySpec[Any, Any]] = attrs.field(factory=dict)
    """Query invocations within the workflow."""

    updates: dict[str, WorkflowUpdateSpec[Any, Any]] = attrs.field(factory=dict)
    """Update invocations within the workflow."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowHandle:
    """Handle for a workflow run."""

    workflow_id: str
    """The id of the workflow."""

    run_id: str | None = None
    """The id of the run."""
