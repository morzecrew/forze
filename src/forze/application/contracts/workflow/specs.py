from typing import Any, Generic, TypeVar, final

import attrs
from pydantic import BaseModel

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowRunSpec(Generic[In, Out]):
    input_type: type[In]
    output_type: type[Out]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowSignalSpec(Generic[In]):
    name: str
    input_type: type[In]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowQuerySpec(Generic[In, Out]):
    name: str
    input_type: type[In]
    output_type: type[Out]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowUpdateSpec(Generic[In, Out]):
    name: str
    input_type: type[In]
    output_type: type[Out]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class WorkflowSpec(Generic[In, Out]):
    name: str
    run: WorkflowRunSpec[In, Out]
    signals: dict[str, WorkflowSignalSpec[Any]] = attrs.field(factory=dict)
    queries: dict[str, WorkflowQuerySpec[Any, Any]] = attrs.field(factory=dict)
    updates: dict[str, WorkflowUpdateSpec[Any, Any]] = attrs.field(factory=dict)
