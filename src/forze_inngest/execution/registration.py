"""Register Forze durable functions with the Inngest SDK."""

from forze_inngest._compat import require_inngest

require_inngest()

# ....................... #

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Generic, Self, TypeVar, final

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
from forze.application.execution.running import handler_for_registry_operation
from forze.application.execution.registry import FrozenOperationRegistry
from forze.base.exceptions import exc

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
    """Binds a :class:`DurableFunctionSpec` to a handler or frozen registry."""

    spec: DurableFunctionSpec[In, Out]
    """Function specification (triggers and argument types)."""

    handler_factory: Callable[[ExecutionContext], Handler[In, Out]] | None = None
    """Custom handler factory when :attr:`~DurableFunctionSpec.operation` is unset."""

    registry: FrozenOperationRegistry | None = None
    """Frozen registry for :attr:`~DurableFunctionSpec.operation` (optional if passed to :func:`register_functions`)."""

    def __attrs_post_init__(self) -> None:
        if self.spec.operation is not None and self.handler_factory is not None:
            raise exc.configuration(
                "InngestFunctionBinding cannot set both spec.operation and handler_factory",
            )

        if self.spec.operation is None and self.handler_factory is None:
            raise exc.configuration(
                "InngestFunctionBinding requires handler_factory when spec.operation is unset",
            )

    @classmethod
    def for_registry_operation(
        cls,
        spec: DurableFunctionSpec[In, Out],
        registry: FrozenOperationRegistry,
    ) -> Self:
        """Bind *spec* with :attr:`~DurableFunctionSpec.operation` to *registry*."""

        return cls(spec=spec, registry=registry)


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


def _resolve_handler_factory(
    binding: InngestFunctionBinding[Any, Any],
    *,
    registry: FrozenOperationRegistry | None,
) -> Callable[[ExecutionContext], Handler[Any, Any]]:
    spec = binding.spec

    if spec.operation is not None:
        reg = binding.registry or registry

        if reg is None:
            raise exc.configuration(
                "register_functions requires registry= when spec.operation is set "
                "and InngestFunctionBinding.registry is unset",
            )

        return handler_for_registry_operation(reg, spec.operation)

    if binding.handler_factory is None:
        raise exc.configuration(
            "InngestFunctionBinding.handler_factory is required when spec.operation is unset",
        )

    return binding.handler_factory


# ....................... #


def _register_one(
    sdk: inngest.Inngest,
    binding: InngestFunctionBinding[Any, Any],
    *,
    ctx_factory: ExecutionContextFactory,
    registry: FrozenOperationRegistry | None,
) -> inngest.Function[Any]:
    spec = binding.spec
    handler_factory = _resolve_handler_factory(binding, registry=registry)
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
                handler = handler_factory(execution_ctx)
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
    registry: FrozenOperationRegistry | None = None,
) -> list[inngest.Function[Any]]:
    """Build Inngest SDK functions from Forze bindings."""

    sdk = client.native

    return [
        _register_one(
            sdk,
            binding,
            ctx_factory=ctx_factory,
            registry=registry,
        )
        for binding in bindings
    ]
