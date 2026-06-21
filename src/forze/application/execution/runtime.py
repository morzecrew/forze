"""Execution runtime for scoped dependency and lifecycle management."""

from contextlib import asynccontextmanager, nullcontext
from datetime import timedelta
from enum import StrEnum
from typing import AsyncGenerator, final

import attrs

from forze.application._logger import logger
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import CpuExecutor, RuntimeVar, bind_cpu_executor

from .context import ExecutionContext
from .deps import FrozenDepsRegistry
from .lifecycle import FrozenLifecyclePlan

# ----------------------- #


class DeploymentProfile(StrEnum):
    """Declared deployment posture, consulted by assembly-time validation.

    ``FLEET`` states the app runs as N replicas behind a load balancer: a
    lifecycle step declared ``mutates_shared_state`` must be
    ``singleton_guarded`` (e.g. wrapped in ``forze_kits``'
    ``singleton_lifecycle_step``), or runtime assembly fails — N replicas
    stampeding a migration is a deploy-time mistake, caught at composition.

    ``SERVERLESS`` states the app runs as a function that freezes between
    invocations (Lambda, Cloud Functions, …): a lifecycle step declared
    ``requires_long_running`` fails runtime assembly — a frozen host cannot keep
    a background poller/relay/scheduler alive. It also drops the graceful-drain
    default to zero (see :func:`~forze.application.execution.assemble.build_runtime`),
    since there is no drain window between a response and a freeze. For warm
    containers, hold one ``scope()`` open across invocations (module-level, like
    the FastAPI/MCP ``runtime_lifespan``) so the per-scope caches stay warm.

    Advisory by design: both markers are declared by the step author, not
    detected structurally.
    """

    SINGLE_PROCESS = "single_process"
    FLEET = "fleet"
    SERVERLESS = "serverless"


# ....................... #


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

    cache_resolved_operations: bool = True
    """Memoize resolved operations per scope (build once per op, then reuse).

    Safe by default: the scope's :class:`ExecutionContext` is created once and is
    immutable, and operation hook/handler factories defer every per-request read
    (identity/tenant/tx) to execution time, so a resolved operation is a pure
    function of its key within a scope. Disable only if you wire a *stateful*
    handler or hook factory that must rebuild on every invocation.
    """

    deployment: DeploymentProfile = DeploymentProfile.SINGLE_PROCESS
    """Declared deployment posture (see :class:`DeploymentProfile`).

    ``FLEET`` enables assembly-time validation: a lifecycle step declared
    ``mutates_shared_state`` must be ``singleton_guarded`` or construction fails.
    """

    drain_timeout: timedelta = timedelta(seconds=10)
    """Bounded wait for in-flight operations during :meth:`shutdown`.

    Shutdown first flips the scope's drain gate — new top-level invocations
    fail with a retryable ``THROTTLED`` (``code="draining"``) — then waits up
    to this long for in-flight operations to finish before running lifecycle
    teardown (which closes the clients they depend on). ``0.0`` skips the
    wait (still rejects new work); expiry proceeds with a logged warning, it
    never blocks shutdown indefinitely. No in-flight work exits immediately.
    """

    cache_resolved_ports: bool = True
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

    cpu_executor: CpuExecutor | None = None
    """Optional CPU-offload executor (see :func:`~forze.base.primitives.run_cpu`).

    ``None`` (default) uses the process-wide default pool — zero config, cleaned up
    at interpreter exit. Pass a ``ThreadPoolCpuExecutor(max_workers=…)`` to size the
    pool ``run_cpu`` uses within this scope: it is bound as the ambient executor for
    the scope's startup, body, and shutdown. The runtime does **not** close it — the
    caller owns its lifecycle (close it yourself, or let it clean up at process exit;
    register a shutdown hook to drain it on teardown).
    """

    # ....................... #

    __ctx: RuntimeVar[ExecutionContext] = attrs.field(
        factory=lambda: RuntimeVar("execution_context"),
        repr=False,
        init=False,
    )
    """Per-scope execution context."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.deployment is DeploymentProfile.FLEET:
            if offending := sorted(
                str(step.id)
                for step in self.lifecycle.graph.steps.values()
                if step.mutates_shared_state and not step.singleton_guarded
            ):
                raise exc.configuration(
                    "FLEET deployment forbids unguarded shared-state-mutating "
                    "lifecycle steps (N replicas would stampede them at startup): "
                    + ", ".join(offending)
                    + ". Wrap each in a singleton guard (e.g. forze_kits "
                    "singleton_lifecycle_step) or run it as a deploy step instead.",
                )

        elif self.deployment is DeploymentProfile.SERVERLESS:
            if offending := sorted(
                str(step.id)
                for step in self.lifecycle.graph.steps.values()
                if step.requires_long_running
            ):
                raise exc.configuration(
                    "SERVERLESS deployment forbids lifecycle steps that require a "
                    "long-running host (background pollers/relays/schedulers cannot "
                    "survive a function freeze between invocations): "
                    + ", ".join(offending)
                    + ". Run each as a separate long-running worker instead.",
                )

    # ....................... #

    @property
    def draining(self) -> bool:
        """Whether the scope is draining (rejecting new invocations).

        ``False`` outside a scope. Flip your readiness probe on this so the
        load balancer stops routing before the drain window starts.
        """

        try:
            ctx = self.__ctx.get()

        except CoreException:
            return False

        return ctx.drain_gate.draining

    # ....................... #

    @property
    def ready(self) -> bool:
        """Readiness probe payload: a scope is active and not draining."""

        try:
            ctx = self.__ctx.get()

        except CoreException:
            return False

        return not ctx.drain_gate.draining

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

            if not await ctx.drain_gate.drain(self.drain_timeout.total_seconds()):
                logger.warning(
                    "Drain timeout (%.1fs) expired with %d operation(s) still "
                    "in flight; proceeding with lifecycle shutdown",
                    self.drain_timeout.total_seconds(),
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

        # Bind the scope's CPU-offload executor (if any) for startup, body, and
        # shutdown. The caller owns its lifecycle — the runtime never closes it.
        bind = (
            bind_cpu_executor(self.cpu_executor)
            if self.cpu_executor is not None
            else nullcontext()
        )

        with bind:
            try:
                await self.startup()

                yield

            finally:
                logger.info("Leaving execution runtime scope")
                await self.shutdown()

        logger.info("Execution runtime scope exited")
