from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any, Callable, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.function import DurableFunctionSpec
from forze.application.contracts.execution import Handler, OnSuccess
from forze.application.contracts.transaction import AfterCommitPort
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..planning.plans import OperationKind, ResolvedOperationPlan
from .plan import run_resolved_operation_plan

if TYPE_CHECKING:
    from ...context import ExecutionContext
    from ...context.invocation import InvocationContext
    from ..registry import FrozenOperationRegistry

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ResolvedOperation[Args, R](Handler[Args, R]):
    """Resolved operation."""

    op: StrKey
    """Operation key."""

    handler: Handler[Args, R]
    """Handler."""

    plan: ResolvedOperationPlan
    """Resolved operation plan."""

    tx_runner: Callable[[StrKey], AbstractAsyncContextManager[None]]
    """Callable that returns an async context manager that scopes a transaction."""

    defer_after_commit: AfterCommitPort
    """Defer work until after a successful root transaction commit."""

    inv_ctx: InvocationContext
    """Invocation context — used to bind the read-only flag for a QUERY operation."""

    # ....................... #

    async def _run(self, args: Args) -> R:
        return await run_resolved_operation_plan(
            self.plan,
            self.handler,
            args,
            tx_runner=self.tx_runner,
            defer_after_commit=self.defer_after_commit,
        )

    # ....................... #

    async def __call__(self, args: Args) -> R:
        """Call the operation.

        A QUERY operation runs under a read-only flag, so a command (write) port cannot be
        acquired for its duration (enforced in ``ConvenientDeps._resolve_command``).
        """

        if self.plan.kind is OperationKind.QUERY:
            with self.inv_ctx.bind_read_only():
                return await self._run(args)

        return await self._run(args)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DispatchedOperation[Args, R](OnSuccess[Args, R]):
    """Resolved operation dispatcher."""

    resolved: ResolvedOperation[Any, Any]
    """Resolved operation."""

    mapper: Callable[[Args, R], Any]
    """Mapper function to transform the result of the target operation."""

    # ....................... #

    async def __call__(self, args: Args, result: R) -> None:
        """Call the operation dispatcher."""

        op_args = self.mapper(args, result)

        return await self.resolved(op_args)


# ....................... #


async def run_operation(
    registry: FrozenOperationRegistry,
    op: StrKey,
    args: Any,
    ctx: ExecutionContext,
) -> Any:
    """Run an operation from a frozen registry (resolve + full plan)."""

    resolved = registry.resolve(op, ctx)

    return await resolved(args)


# ....................... #


def handler_for_registry_operation(
    registry: FrozenOperationRegistry,
    operation: StrKey,
) -> Callable[[ExecutionContext], Handler[Any, Any]]:
    """Return a factory that yields a resolved operation (full plan) for *operation*."""

    def factory(ctx: ExecutionContext) -> Handler[Any, Any]:
        return registry.resolve(operation, ctx)

    return factory


# ....................... #


async def run_durable_function(
    spec: DurableFunctionSpec[Any, Any],
    registry: FrozenOperationRegistry,
    ctx: ExecutionContext,
    args: Any,
) -> Any:
    """Run a durable function backed by :attr:`DurableFunctionSpec.operation`."""

    if spec.operation is None:
        raise exc.configuration(
            "DurableFunctionSpec.operation is required for registry-backed runs",
        )

    return await run_operation(registry, spec.operation, args, ctx)


# ....................... #


async def run_durable_function_typed[SpecIn: BaseModel, SpecOut: BaseModel](
    spec: DurableFunctionSpec[SpecIn, SpecOut],
    registry: FrozenOperationRegistry,
    ctx: ExecutionContext,
    args: SpecIn,
) -> SpecOut:
    """Typed wrapper around :func:`run_durable_function`."""

    return cast(SpecOut, await run_durable_function(spec, registry, ctx, args))
