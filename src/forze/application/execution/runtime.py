"""Execution runtime for scoped dependency and lifecycle management."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator, final

import attrs

from forze.application._logger import logger
from forze.base.primitives import RuntimeVar

from .context import ExecutionContext
from .deps import FrozenDepsRegistry
from .lifecycle import FrozenLifecyclePlan

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ExecutionRuntime:
    """Runnable scope combining deps registry, lifecycle plan, and execution context.

    Use :meth:`scope` as an async context manager to create a context, run
    startup hooks, yield, then run shutdown hooks and reset. The context is
    stored in a :class:`RuntimeVar` for per-request or per-task access.
    """

    deps: FrozenDepsRegistry = attrs.field(factory=FrozenDepsRegistry)
    """Registry for building and freezing dependency providers."""

    lifecycle: FrozenLifecyclePlan = attrs.field(factory=FrozenLifecyclePlan)
    """Plan for startup and shutdown hooks."""

    cache_resolved_operations: bool = attrs.field(default=True)
    """Memoize resolved operations per scope (build once per op, then reuse).

    Safe by default: the scope's :class:`ExecutionContext` is created once and is
    immutable, and operation hook/handler factories defer every per-request read
    (identity/tenant/tx) to execution time, so a resolved operation is a pure
    function of its key within a scope. Disable only if you wire a *stateful*
    handler or hook factory that must rebuild on every invocation.
    """

    # Non initable fields
    __ctx: RuntimeVar[ExecutionContext] = attrs.field(
        factory=lambda: RuntimeVar("execution_context"),
        repr=False,
        init=False,
    )
    """Per-scope execution context."""

    # ....................... #

    def get_context(self) -> ExecutionContext:
        """Return the current execution context.

        :returns: Context for the active scope.
        :raises RuntimeError: If no context has been created (e.g. outside scope).
        """

        return self.__ctx.get()

    # ....................... #

    def create_context(self) -> None:
        """Create and set the execution context from the deps registry.

        Freezes the registry when needed, resolves per-scope deps, and stores
        the context. Idempotent within a scope; raises if context already exists.
        """

        logger.info("Creating execution context")

        resolved_deps = self.deps.resolve()

        ctx = ExecutionContext(
            deps=resolved_deps,
            cache_operations=self.cache_resolved_operations,
        )
        self.__ctx.set_once(ctx)

        logger.info("Execution context created")

    # ....................... #

    async def startup(self) -> None:
        """Run lifecycle startup hooks with the current context."""

        logger.info("Starting execution runtime")

        ctx = self.__ctx.get()
        await self.lifecycle.startup(ctx)

        logger.info("Execution runtime startup completed")

    # ....................... #

    async def shutdown(self) -> None:
        """Run lifecycle shutdown hooks and reset the context.

        Shutdown runs in reverse wave order. Context is reset in a
        ``finally`` block so it is cleared even if shutdown raises.
        """

        logger.info("Shutting down execution runtime")

        try:
            ctx = self.__ctx.get()
            await self.lifecycle.shutdown(ctx)

        finally:
            self.__ctx.reset()

        logger.info("Execution runtime shutdown completed")

    # ....................... #

    @asynccontextmanager
    async def scope(self) -> AsyncGenerator[None]:
        """Enter an execution scope: create context, startup, yield, shutdown.

        Use as an async context manager. On entry: create context, run startup.
        On exit: run shutdown, reset context.
        """

        logger.info("Entering execution runtime scope")
        self.create_context()

        try:
            await self.startup()

            yield

        finally:
            logger.info("Leaving execution runtime scope")
            await self.shutdown()

        logger.info("Execution runtime scope exited")
