"""One-call assembly of an :class:`ExecutionRuntime` from modules, deps, and steps."""

from typing import Iterable

from .deps import Deps, DepsModule, DepsRegistry
from .lifecycle import LifecycleModule, LifecyclePlan, LifecycleStep
from .runtime import ExecutionRuntime

# ----------------------- #


def build_runtime(
    *modules: DepsModule,
    deps: Iterable[Deps] = (),
    lifecycle_modules: Iterable[LifecycleModule] = (),
    lifecycle_steps: Iterable[LifecycleStep] = (),
    concurrent_lifecycle: bool = False,
    cache_resolved_operations: bool = True,
    cache_resolved_ports: bool = True,
    drain_timeout: float = 10.0,
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
        operations before lifecycle shutdown).
    :returns: Runtime ready for :meth:`ExecutionRuntime.scope`.
    """

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
    )
