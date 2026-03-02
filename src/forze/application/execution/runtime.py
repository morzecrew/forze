"""Execution runtime for scoped dependency and lifecycle management."""

from contextlib import asynccontextmanager
from typing import AsyncIterator, final

import attrs

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
        ctx = ExecutionContext(deps=self.deps.build())
        self.__ctx.set_once(ctx)

    # ....................... #

    async def startup(self) -> None:
        """Run lifecycle startup hooks with the current context."""
        await self.lifecycle.startup(self.__ctx.get())

    # ....................... #

    async def shutdown(self) -> None:
        """Run lifecycle shutdown hooks and reset the context.

        Shutdown runs in reverse order of startup. Context is reset in a
        ``finally`` block so it is cleared even if shutdown raises.
        """
        try:
            await self.lifecycle.shutdown(self.__ctx.get())

        finally:
            self.__ctx.reset()

    # ....................... #

    @asynccontextmanager
    async def scope(self) -> AsyncIterator[None]:
        """Enter an execution scope: create context, startup, yield, shutdown.

        Use as an async context manager. On entry: create context, run startup.
        On exit: run shutdown, reset context.
        """
        self.create_context()

        try:
            await self.startup()
            yield

        finally:
            await self.shutdown()
