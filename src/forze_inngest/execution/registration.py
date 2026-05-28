"""Register Forze durable functions with the Inngest SDK."""

from forze_inngest._compat import require_inngest

require_inngest()

# ....................... #

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Generic, TypeVar, final

import attrs
import inngest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionEventTrigger,
    DurableFunctionSpec,
    DurableFunctionTrigger,
)
from forze.application.contracts.execution import Handler
from forze.application.execution.context import (
    ExecutionContext,
    ExecutionContextFactory,
)

from ..adapters.context import (
    InngestDecodedContext,
    parse_function_args,
    split_envelope,
)
from ..adapters.step import bind_inngest_step, reset_inngest_step
from ..kernel.platform import InngestClientPort

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InngestFunctionBinding(Generic[In, Out]):
    """Binds a :class:`DurableFunctionSpec` to a Forze handler factory."""

    spec: DurableFunctionSpec[In, Out]
    """Function specification (triggers and argument types)."""

    handler_factory: Callable[[ExecutionContext], Handler[In, Out]]
    """Factory that builds the handler given an :class:`ExecutionContext`."""


# ....................... #


def _map_trigger(
    trigger: DurableFunctionTrigger,
) -> inngest.TriggerEvent | inngest.TriggerCron:
    if isinstance(trigger, DurableFunctionEventTrigger):
        return inngest.TriggerEvent(event=str(trigger.event))

    if isinstance(
        trigger, DurableFunctionCronTrigger
    ):  # pyright: ignore[reportUnnecessaryIsInstance]
        return inngest.TriggerCron(cron=trigger.expression)

    raise TypeError(f"unsupported durable function trigger: {type(trigger)!r}")


# ....................... #


@contextmanager
def _bind_invocation(
    ctx: ExecutionContext,
    envelope: InngestDecodedContext,
) -> Iterator[None]:
    if envelope.metadata is not None:
        with ctx.inv.bind(
            metadata=envelope.metadata,
            authn=envelope.authn,
            tenant=envelope.tenant,
        ):
            yield

    else:
        with ctx.inv.bind_identity(
            authn=envelope.authn,
            tenant=envelope.tenant,
        ):
            yield


# ....................... #


def _register_one(
    sdk: inngest.Inngest,
    binding: InngestFunctionBinding[Any, Any],
    *,
    ctx_factory: ExecutionContextFactory,
) -> inngest.Function[Any]:
    spec = binding.spec
    triggers = [_map_trigger(t) for t in spec.triggers]

    trigger: (
        inngest.TriggerEvent
        | inngest.TriggerCron
        | list[inngest.TriggerEvent | inngest.TriggerCron]
    )

    if len(triggers) == 1:
        trigger = triggers[0]

    else:
        trigger = triggers

    @sdk.create_function(
        fn_id=str(spec.name),
        trigger=trigger,
    )
    async def _handler(ctx: inngest.Context) -> Any:
        raw_data: dict[str, Any] = dict(ctx.event.data) if ctx.event else {}

        envelope, _ = split_envelope(raw_data)
        args = parse_function_args(raw_data, args_type=binding.spec.run.args_type)

        execution_ctx = ctx_factory()
        step_token = bind_inngest_step(ctx.step)

        try:
            with _bind_invocation(execution_ctx, envelope):
                handler = binding.handler_factory(execution_ctx)
                return await handler(args)

        finally:
            reset_inngest_step(step_token)

    return _handler


# ....................... #


def register_functions(
    client: InngestClientPort,
    bindings: Sequence[InngestFunctionBinding[Any, Any]],
    *,
    ctx_factory: ExecutionContextFactory,
) -> list[inngest.Function[Any]]:
    """Build Inngest SDK functions from Forze bindings."""

    sdk = client.native

    return [
        _register_one(sdk, binding, ctx_factory=ctx_factory) for binding in bindings
    ]
