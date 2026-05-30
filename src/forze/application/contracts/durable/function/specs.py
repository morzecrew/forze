from typing import Any, Generic, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import BaseSpec
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze.base.serialization import RecordMappingCodec

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableFunctionEventSpec[M](BaseSpec):
    """Specification binding a logical event name to its payload record codec."""

    codec: RecordMappingCodec[M, Any]
    """Payload record codec for events with this name."""

    # ....................... #

    @property
    def model_type(self) -> type[M]:
        """Payload model type carried by :attr:`codec`."""

        return self.codec.model_type


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableFunctionInvokeSpec(Generic[In, Out]):
    """Specification for the main durable function invocation."""

    args_type: type[In]
    """The type of the arguments for the function run."""

    return_type: type[Out] | None = attrs.field(default=None)
    """The type of the return value for the function run."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableFunctionEventTrigger:
    """Trigger that starts a function when an event is received."""

    event: StrKey
    """Logical event name (typically matches a :class:`DurableFunctionEventSpec` name)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableFunctionCronTrigger:
    """Trigger that starts a function on a cron schedule."""

    expression: str
    """Cron expression interpreted by the provider."""


# ....................... #

DurableFunctionTrigger = DurableFunctionEventTrigger | DurableFunctionCronTrigger
"""Union of supported durable function triggers."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableFunctionSpec(Generic[In, Out], BaseSpec):
    """Specification for an event-driven durable function.

    When :attr:`operation` is set, runtimes resolve and execute that operation
    from a frozen :class:`~forze.application.execution.operations.registry.FrozenOperationRegistry`
    (full operation plan). Otherwise integrations use a custom handler factory.
    """

    run: DurableFunctionInvokeSpec[In, Out]
    """The main invocation of the function."""

    triggers: tuple[DurableFunctionTrigger, ...]
    """How the function may be started (events and/or cron)."""

    operation: StrKey | None = None
    """When set, run this operation key from a frozen registry at invoke time."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.triggers:
            raise exc.validation(
                "DurableFunctionSpec requires at least one trigger",
            )
