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

    drain_timeout: float = attrs.field(default=10.0)
    """Bounded wait (seconds) for in-flight operations during :meth:`shutdown`.

    Shutdown first flips the scope's drain gate — new top-level invocations
    fail with a retryable ``THROTTLED`` (``code="draining"``) — then waits up
    to this long for in-flight operations to finish before running lifecycle
    teardown (which closes the clients they depend on). ``0.0`` skips the
    wait (still rejects new work); expiry proceeds with a logged warning, it
    never blocks shutdown indefinitely. No in-flight work exits immediately.
    """

    cache_resolved_ports: bool = attrs.field(default=True)
    """Memoize resolved configurable ports (document/search/cache/storage/... adapters)
    per scope, so each ``ctx.<x>.query(spec)`` reuses one gateway/adapter (and its codecs,
    filter renderers, key codecs) instead of rebuilding it on every call.

    Safe by default for the same reason as :attr:`cache_resolved_operations`: port
    factories are synchronous, scope-stable builders that capture only scope-stable
    deps (clients/config) and defer every per-request read (tenant via the bound
    ``inv_ctx.get_tenant``, the DB connection via the client at call time) to execution
    time. Keyed by ``(dep key, route)`` and validated against the presented spec, so a
    different spec on the same route rebuilds. Bypassed automatically while resolution
    tracing is enabled (to keep per-task resolution traces complete). Disable only for a
    *stateful* port factory that must rebuild per call.
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
            cache_ports=self.cache_resolved_ports,
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
        """Drain in-flight operations, run lifecycle shutdown hooks, reset the context.

        Draining comes first (see :attr:`drain_timeout`): the scope stops
        admitting new top-level invocations and gives in-flight operations a
        bounded window to finish before lifecycle teardown closes the clients
        they depend on. A drain-timeout expiry is logged and shutdown proceeds.

        Shutdown runs in reverse wave order. Context is reset in a
        ``finally`` block so it is cleared even if shutdown raises.

        Only steps whose startup completed and that were not already shut down
        (e.g. rolled back after a partial startup failure) are shut down, so each
        step's shutdown runs at most once per successful startup.
        """

        logger.info("Shutting down execution runtime")

        try:
            ctx = self.__ctx.get()

            if not await ctx.drain_gate.drain(self.drain_timeout):
                logger.warning(
                    "Drain timeout (%.1fs) expired with %d operation(s) still "
                    "in flight; proceeding with lifecycle shutdown",
                    self.drain_timeout,
                    ctx.drain_gate.in_flight,
                )

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
