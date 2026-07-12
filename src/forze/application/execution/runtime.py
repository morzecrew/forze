"""Execution runtime for scoped dependency and lifecycle management."""

from contextlib import AbstractContextManager, asynccontextmanager, nullcontext
from datetime import timedelta
from enum import StrEnum
from typing import AsyncGenerator, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.querying import (
    CursorTokenCipher,
    CursorTokenSigner,
    bind_cursor_cipher,
    bind_cursor_signer,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import (
    CpuExecutor,
    RuntimeVar,
    ThreadPoolCpuExecutor,
    bind_cpu_executor,
    cpu_executor_bound,
)

from .context import ExecutionContext
from .context.transaction import AfterCommitErrorHandler
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


def _positive_cpu_workers(_instance: object, _attribute: object, value: int | None) -> None:
    """Reject a non-positive ``cpu_workers`` at construction, not later on first offload."""

    if value is not None and value < 1:
        raise exc.configuration(
            f"cpu_workers must be a positive integer when set (got {value!r}); "
            "leave it None for default sizing.",
            code="core.runtime.cpu_workers_invalid",
        )


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

    shutdown_step_timeout: timedelta = timedelta(seconds=10)
    """Bounded wait for each lifecycle shutdown hook during :meth:`shutdown`.

    The drain window bounds in-flight *work*; this bounds the *teardown* that
    follows it. Each shutdown hook (client ``close()``, broker flush, poller
    stop) gets this long to finish; a hook that exceeds it is abandoned with a
    logged error and teardown moves on to the next step — so one wedged hook
    (a connection that will not drain, a flush that never returns) can never
    hang process exit. Raise it for hooks with legitimately long flushes; a
    very large value effectively restores unbounded teardown.
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
    """CPU-offload executor to bind for this scope (see :func:`~forze.base.primitives.run_cpu`).

    ``None`` (default): unless an executor is already bound in the surrounding context,
    the runtime creates a scope-lifetime :class:`ThreadPoolCpuExecutor`, binds it for
    startup/body/shutdown, and **closes it on scope exit** — a bounded pool this runtime
    owns and releases with the rest of its resources, not a shared process-global one
    (size it with :attr:`cpu_workers`). Pass an executor to inject your own instead; the
    runtime then binds but never closes it (you own its lifecycle). An executor already
    bound around the scope (e.g. a simulation's) is always respected, never overridden;
    with no runtime scope active at all, ``run_cpu`` runs inline.
    """

    cpu_workers: int | None = attrs.field(default=None, validator=_positive_cpu_workers)
    """Worker count for the runtime-owned CPU pool (ignored when :attr:`cpu_executor` is set).

    ``None`` uses the default sizing (``min(32, os.cpu_count() + 4)``), letting you size the
    pool without constructing and owning a :class:`ThreadPoolCpuExecutor` yourself. Must be a
    positive integer when set (validated at construction).
    """

    cursor_token_signer: CursorTokenSigner | None = None
    """Optional HMAC signer for keyset cursor tokens. When set, the runtime binds it per scope
    (context-scoped, auto-restored on scope exit — so two runtimes in one process sign with
    their own signer), so every keyset cursor token is signed and verification rejects any
    unsigned or tampered token — opt-in, hard cutover. ``None`` (default) leaves tokens
    unsigned. See :func:`~forze.application.contracts.querying.bind_cursor_signer`."""

    cursor_token_cipher: CursorTokenCipher | None = None
    """Optional AEAD cipher for keyset cursor tokens. When set, the runtime binds it per scope,
    so every keyset cursor token is encrypted (payload hidden, tag-authenticated) and
    verification rejects any unencrypted or tampered token — opt-in, hard cutover. A cipher
    **supersedes** :attr:`cursor_token_signer` (AEAD already authenticates). ``None`` (default)
    leaves confidentiality off. See
    :func:`~forze.application.contracts.querying.configure_cursor_cipher`."""

    after_commit_error_handler: AfterCommitErrorHandler | None = None
    """Optional out-of-band handler notified when a non-fatal post-commit callback fails
    on an already-committed transaction (an idempotency-record write, an eager dispatch).

    Passed to the scope's :class:`ExecutionContext` (see
    :attr:`ExecutionContext.after_commit_error_handler`), so every operation in the scope
    reports through it. The operation still returns its committed result; the handler only
    surfaces the failed effect (an
    :class:`~forze.application.execution.context.transaction.AfterCommitError`) for
    alerting/reconciliation. Must not raise. ``None`` (default) = log only."""

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
            after_commit_error_handler=self.after_commit_error_handler,
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

        Shutdown runs in reverse wave order, each hook bounded by
        :attr:`shutdown_step_timeout` so a wedged hook cannot hang process
        exit. Context is reset in a ``finally`` block so it is cleared even if
        shutdown raises.

        Only steps whose startup completed and that were not already shut down
        (e.g. rolled back after a partial startup failure) are shut down, so each
        step's shutdown runs at most once per successful startup.
        """

        logger.info("Shutting down execution runtime")

        try:
            ctx = self.__ctx.get()

            if not await ctx.drain_gate.drain(self.drain_timeout.total_seconds()):
                # Drain expired with work still running. Cancel the stragglers and let them
                # unwind (rollback, release connections) *before* lifecycle teardown closes
                # the clients they hold — otherwise they run on against closing resources.
                cancelled = await ctx.drain_gate.cancel_in_flight(
                    grace=self.shutdown_step_timeout.total_seconds()
                )
                logger.warning(
                    "Drain timeout (%.1fs) expired with %d operation(s) still in flight; "
                    "cancelled them before lifecycle shutdown",
                    self.drain_timeout.total_seconds(),
                    cancelled,
                )

            # Cancel detached background work (e.g. document-cache early refreshes) before
            # teardown closes the clients it uses — a straggler would otherwise run on
            # against a closing cache/gateway. Always runs, drain-timeout or not.
            await ctx.background_owners.close(
                grace=self.shutdown_step_timeout.total_seconds()
            )

            await self.lifecycle.shutdown(
                ctx, step_timeout=self.shutdown_step_timeout.total_seconds()
            )

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

        # Bind a CPU-offload executor for startup, body, and shutdown, by priority:
        #   1. an injected executor     -> bound, caller-owned (never closed here);
        #   2. one already bound upstack -> deferred to, never overridden (e.g. a
        #      simulation's inline executor — overriding it would break determinism);
        #   3. nothing bound            -> a scope-lifetime pool this runtime owns,
        #      created now and closed on exit (no shared process-global pool).
        owned_pool: ThreadPoolCpuExecutor | None = None
        bind: AbstractContextManager[None]

        if self.cpu_executor is not None:
            bind = bind_cpu_executor(self.cpu_executor)
        elif cpu_executor_bound():
            bind = nullcontext()
        else:
            owned_pool = (
                ThreadPoolCpuExecutor(max_workers=self.cpu_workers)
                if self.cpu_workers is not None
                else ThreadPoolCpuExecutor()
            )
            bind = bind_cpu_executor(owned_pool)

        # Opt-in cursor-token signing, scoped to this runtime's scope (context-local, so two
        # runtimes in one process each mint/verify with their own signer). ``None`` = unsigned.
        sign_bind: AbstractContextManager[None] = (
            bind_cursor_signer(self.cursor_token_signer)
            if self.cursor_token_signer is not None
            else nullcontext()
        )

        # Opt-in cursor-token encryption (confidentiality), same scoping. A cipher supersedes
        # the signer — AEAD authenticates too — so both binding is harmless. ``None`` = plaintext.
        cipher_bind: AbstractContextManager[None] = (
            bind_cursor_cipher(self.cursor_token_cipher)
            if self.cursor_token_cipher is not None
            else nullcontext()
        )

        try:
            with bind, sign_bind, cipher_bind:
                try:
                    await self.startup()

                    yield

                finally:
                    logger.info("Leaving execution runtime scope")
                    await self.shutdown()

        finally:
            if owned_pool is not None:
                owned_pool.close()

        logger.info("Execution runtime scope exited")
