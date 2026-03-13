"""Execution runtime for scoped dependency and lifecycle management."""

from contextlib import asynccontextmanager
from typing import AsyncIterator, final

import attrs

from forze.base.logging import getLogger, log_section
from forze.base.primitives import RuntimeVar

from .context import ExecutionContext
from .deps import DepsPlan
from .lifecycle import LifecyclePlan

# ----------------------- #

logger = getLogger(__name__)

# ....................... #


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

        logger.debug("Creating execution context")

        with log_section():
            deps = self.deps.build()

            ctx = ExecutionContext(deps=deps)
            self.__ctx.set_once(ctx)

    # ....................... #

    async def startup(self) -> None:
        """Run lifecycle startup hooks with the current context."""

        logger.debug("Starting execution runtime")

        with log_section():
            ctx = self.__ctx.get()
            await self.lifecycle.startup(ctx)

    # ....................... #

    async def shutdown(self) -> None:
        """Run lifecycle shutdown hooks and reset the context.

        Shutdown runs in reverse order of startup. Context is reset in a
        ``finally`` block so it is cleared even if shutdown raises.
        """

        logger.debug("Shutting down execution runtime")

        with log_section():
            try:
                await self.lifecycle.shutdown(self.__ctx.get())
                logger.debug("Execution runtime lifecycle shutdown completed")

            finally:
                self.__ctx.reset()
                logger.debug("Execution context reset")

    # ....................... #

    @asynccontextmanager
    async def scope(self) -> AsyncIterator[None]:
        """Enter an execution scope: create context, startup, yield, shutdown.

        Use as an async context manager. On entry: create context, run startup.
        On exit: run shutdown, reset context.
        """

        logger.debug("Entering execution runtime scope")
        self.create_context()

        try:
            await self.startup()
            logger.debug("Execution runtime scope entered")

            yield

        finally:
            logger.debug("Leaving execution runtime scope")
            await self.shutdown()
            logger.debug("Execution runtime scope left")
