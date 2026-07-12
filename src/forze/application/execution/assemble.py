"""One-call assembly of an :class:`ExecutionRuntime` from modules, deps, and steps."""

from typing import Iterable
from datetime import timedelta

from forze.base.primitives import CpuExecutor

from forze.application.contracts.deps import Deps, DepsModule
from forze.application.contracts.execution import LifecycleModule, LifecycleStep

from .context.transaction import AfterCommitErrorHandler
from .deps import DepsRegistry
from .lifecycle import LifecyclePlan
from .runtime import DeploymentProfile, ExecutionRuntime

# ----------------------- #


def build_runtime(
    *modules: DepsModule,
    deps: Iterable[Deps] = (),
    lifecycle_modules: Iterable[LifecycleModule] = (),
    lifecycle_steps: Iterable[LifecycleStep] = (),
    concurrent_lifecycle: bool = False,
    cache_resolved_operations: bool = True,
    cache_resolved_ports: bool = True,
    drain_timeout: timedelta | None = None,
    deployment: DeploymentProfile = DeploymentProfile.SINGLE_PROCESS,
    cpu_executor: CpuExecutor | None = None,
    cpu_workers: int | None = None,
    after_commit_error_handler: AfterCommitErrorHandler | None = None,
) -> ExecutionRuntime:
    """Assemble an :class:`ExecutionRuntime` in one call.

    Thin assembler over the standard composition — build a
    :class:`DepsRegistry` from ``modules`` and/or raw ``deps`` blobs, build a
    :class:`LifecyclePlan` from ``lifecycle_modules`` and/or
    ``lifecycle_steps``, freeze both, and construct the runtime. No new
    semantics: validation still happens at freeze time, exactly as when
    composing the parts by hand.

    Equivalent to::

        ExecutionRuntime(
            deps=DepsRegistry.from_modules(*modules).with_deps(*deps).freeze(),
            lifecycle=LifecyclePlan.from_modules(*lifecycle_modules)
            .with_steps(*lifecycle_steps)
            .with_concurrent(concurrent_lifecycle)
            .freeze(),
        )

    Enter :meth:`ExecutionRuntime.scope` yourself (or hand the runtime to
    ``forze_fastapi.runtime_lifespan`` for FastAPI apps).

    :param modules: Deps modules to merge into the registry.
    :param deps: Raw registration deps blobs to merge after the modules.
    :param lifecycle_modules: Lifecycle modules contributing startup/shutdown steps.
    :param lifecycle_steps: Lifecycle steps appended after the modules' steps.
    :param concurrent_lifecycle: When ``True``, run lifecycle steps within the
        same wave concurrently.
    :param cache_resolved_operations: Passed through to
        :attr:`ExecutionRuntime.cache_resolved_operations`.
    :param cache_resolved_ports: Passed through to
        :attr:`ExecutionRuntime.cache_resolved_ports`.
    :param drain_timeout: Passed through to
        :attr:`ExecutionRuntime.drain_timeout` (bounded wait for in-flight
        operations before lifecycle shutdown). ``None`` (default) resolves to
        ``0`` under a ``SERVERLESS`` deployment — a frozen function has no drain
        window — and ``10s`` otherwise; an explicit value is always honored.
    :param deployment: Passed through to :attr:`ExecutionRuntime.deployment`
        (``FLEET`` validates that shared-state-mutating lifecycle steps are
        singleton-guarded; ``SERVERLESS`` forbids ``requires_long_running`` steps).
    :param cpu_executor: CPU-offload executor to inject for the scope (see
        :attr:`ExecutionRuntime.cpu_executor`); caller-owned — the runtime binds but
        does not close it. ``None`` (default) lets the runtime own a scope-lifetime pool
        it closes on exit, unless one is already bound in the surrounding context.
    :param cpu_workers: Size of the runtime-owned CPU pool when ``cpu_executor`` is
        ``None`` (see :attr:`ExecutionRuntime.cpu_workers`); ``None`` uses default sizing.
    :param after_commit_error_handler: Passed through to
        :attr:`ExecutionRuntime.after_commit_error_handler` — an out-of-band handler
        notified when a non-fatal post-commit callback fails on an already-committed
        transaction (the operation still returns its committed result). Must not raise.
        ``None`` (default) logs only.
    :returns: Runtime ready for :meth:`ExecutionRuntime.scope`.
    """

    if drain_timeout is None:
        drain_timeout = (
            timedelta(0)
            if deployment is DeploymentProfile.SERVERLESS
            else timedelta(seconds=10)
        )

    registry = DepsRegistry(modules=tuple(modules), deps=tuple(deps))
    plan = LifecyclePlan(
        modules=tuple(lifecycle_modules),
        steps=tuple(lifecycle_steps),
        concurrent=concurrent_lifecycle,
    )

    return ExecutionRuntime(
        deps=registry.freeze(),
        lifecycle=plan.freeze(),
        cache_resolved_operations=cache_resolved_operations,
        cache_resolved_ports=cache_resolved_ports,
        drain_timeout=drain_timeout,
        deployment=deployment,
        cpu_executor=cpu_executor,
        cpu_workers=cpu_workers,
        after_commit_error_handler=after_commit_error_handler,
    )
