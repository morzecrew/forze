from typing import Any, Generic, TypeVar, final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import attrs
from pydantic import BaseModel, ValidationError

from forze.application.contracts.base import BaseSpec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, StrKey
from forze.base.serialization import ModelCodec

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableFunctionEventSpec[M](BaseSpec):
    """Specification binding a logical event name to its payload record codec."""

    codec: ModelCodec[M, Any]
    """Payload record codec for events with this name."""

    encrypt: bool = False
    """When ``True`` and a keyring is wired, the event payload is sealed before it leaves
    for the durable backend and the function handler receives it decrypted (the ``_forze``
    context envelope stays plaintext for routing/context binding). Default ``False``."""

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

    tz: str | None = None
    """IANA timezone the expression is evaluated in (``None`` = UTC); an unknown zone fails
    the declaration."""

    # Kept out of the hash: a dict (or an unfrozen model) does not hash.
    input: JsonDict | BaseModel | None = attrs.field(default=None, hash=False)
    """Input every fire hands the run: a JSON dict or an instance of the spec's args type.

    Checked against :attr:`DurableFunctionInvokeSpec.args_type` when the spec is declared, so
    a payload no run could accept fails at import rather than on the first fire."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.tz is None:
            return

        try:
            ZoneInfo(self.tz)

        except (ZoneInfoNotFoundError, ValueError) as error:
            raise exc.configuration(f"Cron trigger timezone {self.tz!r} is unknown.") from error

    # ....................... #

    def encode_input(self, args_type: type[BaseModel]) -> JsonDict | None:
        """Return :attr:`input` validated against *args_type*, as the run store holds it.

        :raises CoreException: ``configuration`` when the input is not an *args_type*.
        """

        if self.input is None:
            return None

        if isinstance(self.input, BaseModel):
            if not isinstance(self.input, args_type):
                raise exc.configuration(
                    f"Cron trigger input is a {type(self.input).__name__}, "
                    f"not the spec's args type {args_type.__name__}.",
                )

            return self.input.model_dump(mode="json")

        try:
            return args_type.model_validate(self.input).model_dump(mode="json")

        except ValidationError as error:
            raise exc.configuration(
                f"Cron trigger input does not validate as {args_type.__name__}.",
            ) from error


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

        for trigger in self.triggers:
            if isinstance(trigger, DurableFunctionCronTrigger):
                trigger.encode_input(self.run.args_type)
