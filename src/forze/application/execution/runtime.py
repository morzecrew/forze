"""Execution runtime for scoped dependency and lifecycle management."""

from contextlib import asynccontextmanager
from typing import AsyncIterator, final

import attrs

from forze.application._logger import logger
from forze.base.primitives import RuntimeVar

from .context import ExecutionContext
from .deps import DepsPlan
from .lifecycle import LifecyclePlan

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ExecutionRuntime:
    """Runnable scope combining deps plan, lifecycle plan, and execution context.

    Use :meth:`scope` as an async context manager to create a context, run
    startup hooks, yield, then run shutdown hooks and reset. The context is
    stored in a :class:`RuntimeVar` for per-request or per-task access.
    """

    deps: DepsPlan = attrs.field(factory=DepsPlan)
    """Plan for building the dependency container."""

    lifecycle: LifecyclePlan = attrs.field(factory=LifecyclePlan)
    """Plan for startup and shutdown hooks."""

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
        """Create and set the execution context from the deps plan.

        Builds deps via :meth:`DepsPlan.build` and stores the context.
        Idempotent within a scope; raises if context already exists.
        """

        logger.info("Creating execution context")

        deps = self.deps.build()

        ctx = ExecutionContext(deps=deps)
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

        Shutdown runs in reverse order of startup. Context is reset in a
        ``finally`` block so it is cleared even if shutdown raises.
        """

        logger.info("Shutting down execution runtime")

        try:
            await self.lifecycle.shutdown(self.__ctx.get())

        finally:
            self.__ctx.reset()

        logger.info("Execution runtime shutdown completed")

    # ....................... #

    @asynccontextmanager
    async def scope(self) -> AsyncIterator[None]:
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
