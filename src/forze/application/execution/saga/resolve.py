"""Resolve the saga executor with an in-process default."""

from typing import TYPE_CHECKING

from forze.application.contracts.saga import (
    SagaDefinition,
    SagaExecutorDepKey,
    SagaExecutorPort,
)

from .executor import InProcessSagaExecutor

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #

_DEFAULT_SAGA_EXECUTOR: SagaExecutorPort = InProcessSagaExecutor()
"""Process-wide in-process executor used when no app executor is registered."""

# ....................... #


def default_saga_executor() -> SagaExecutorPort:
    """Return the shared in-process saga executor."""

    return _DEFAULT_SAGA_EXECUTOR


# ....................... #


def resolve_saga_executor(ctx: ExecutionContext) -> SagaExecutorPort:
    """Return the app-registered saga executor, or the in-process default."""

    if ctx.deps.exists(SagaExecutorDepKey):
        return ctx.deps.provide(SagaExecutorDepKey)

    return _DEFAULT_SAGA_EXECUTOR


# ....................... #


async def run_saga[Ctx](
    ctx: ExecutionContext,
    definition: SagaDefinition[Ctx],
    initial: Ctx,
) -> Ctx:
    """Run *definition* from *initial* via the resolved saga executor."""

    return await resolve_saga_executor(ctx).run(ctx, definition, initial)
