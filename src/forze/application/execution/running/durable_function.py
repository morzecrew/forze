"""Run durable functions via frozen operation registry keys."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar, cast

from pydantic import BaseModel

from forze.application.contracts.durable.function import DurableFunctionSpec
from forze.application.contracts.execution import Handler
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .operation import run_operation

if TYPE_CHECKING:
    from ..context import ExecutionContext
    from ..registry import FrozenOperationRegistry

SpecIn = TypeVar("SpecIn", bound=BaseModel)
SpecOut = TypeVar("SpecOut", bound=BaseModel)

# ----------------------- #


def handler_for_registry_operation(
    registry: "FrozenOperationRegistry",
    operation: StrKey,
) -> Callable[["ExecutionContext"], Handler[Any, Any]]:
    """Return a factory that yields a resolved operation (full plan) for *operation*."""

    def factory(ctx: "ExecutionContext") -> Handler[Any, Any]:
        return registry.resolve(operation, ctx)

    return factory


# ....................... #


async def run_durable_function(
    spec: DurableFunctionSpec[Any, Any],
    registry: "FrozenOperationRegistry",
    ctx: "ExecutionContext",
    args: Any,
) -> Any:
    """Run a durable function backed by :attr:`DurableFunctionSpec.operation`."""

    if spec.operation is None:
        raise exc.configuration(
            "DurableFunctionSpec.operation is required for registry-backed runs",
        )

    return await run_operation(registry, spec.operation, args, ctx)


# ....................... #


async def run_durable_function_typed(
    spec: DurableFunctionSpec[SpecIn, SpecOut],
    registry: "FrozenOperationRegistry",
    ctx: "ExecutionContext",
    args: SpecIn,
) -> SpecOut:
    """Typed wrapper around :func:`run_durable_function`."""

    return cast(SpecOut, await run_durable_function(spec, registry, ctx, args))
