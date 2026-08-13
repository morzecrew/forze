"""One-call assembly of an :class:`ExecutionRuntime` from modules, deps, and steps."""

from collections.abc import Iterable
from datetime import timedelta
from typing import Any

from forze.application.contracts.deps import Deps, DepsModule
from forze.application.contracts.execution import LifecycleModule, LifecycleStep
from forze.application.contracts.inventory import FrozenSpecRegistry, SpecRegistry
from forze.application.contracts.querying import CursorTokenCipher, CursorTokenSigner
from forze.base.exceptions import exc
from forze.base.primitives import CpuExecutor

from .context.transaction import AfterCommitErrorHandler
from .deps import DepsRegistry
from .lifecycle import LifecyclePlan
from .runtime import DeploymentProfile, ExecutionRuntime

# ----------------------- #


def _many[T](value: T | Iterable[T]) -> tuple[T, ...]:
    """Read a one-or-many argument as a tuple.

    Every collection this module takes is far more often given one item than several, and
    ``lifecycle_steps=[step]`` is a list written to satisfy a signature rather than to say
    anything. The two shapes cannot be confused for one another: deps and lifecycle modules
    are callables, deps blobs and steps are attrs values, and none of them is iterable.
    """

    return tuple(value) if isinstance(value, Iterable) else (value,)  # pyright: ignore[reportUnknownArgumentType]


# ....................... #


def _frozen_specs(
    specs: SpecRegistry | FrozenSpecRegistry | Iterable[SpecRegistry] | None,
) -> FrozenSpecRegistry | None:
    """Fold an application's spec inventory into one frozen registry.

    Several registries are the normal case, not the exception: the author's own specs, one
    per ``AggregateKit``, one for the identity plane. They merge into a *fresh* registry
    rather than into the first one given, because :meth:`SpecRegistry.merge` mutates its
    receiver — folding in place would quietly leave the caller's own registry carrying
    every kit's specs as well.
    """

    if specs is None or isinstance(specs, FrozenSpecRegistry):
        return specs

    if isinstance(specs, SpecRegistry):
        return specs.freeze()

    merged = SpecRegistry()

    for one in specs:
        if isinstance(one, FrozenSpecRegistry):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise exc.configuration(
                "A frozen registry cannot be merged with others. Pass the unfrozen "
                "SpecRegistry contributions and let assembly freeze the result.",
            )

        merged.merge(one)

    return merged.freeze()


# ----------------------- #


def build_runtime(
    deps_modules: DepsModule | Iterable[DepsModule] = (),
    *,
    deps: Deps | Iterable[Deps] = (),
    lifecycle_modules: LifecycleModule | Iterable[LifecycleModule] = (),
    lifecycle_steps: LifecycleStep | Iterable[LifecycleStep] = (),
    lifecycle_concurrent: bool = False,
    cache_resolved_operations: bool = True,
    cache_resolved_ports: bool = True,
    drain_timeout: timedelta | None = None,
    shutdown_step_timeout: timedelta | None = None,
    deployment: DeploymentProfile = DeploymentProfile.SINGLE_PROCESS,
    cpu_executor: CpuExecutor | None = None,
    cpu_workers: int | None = None,
    after_commit_error_handler: AfterCommitErrorHandler | None = None,
    cursor_token_signer: CursorTokenSigner | None = None,
    cursor_token_cipher: CursorTokenCipher | None = None,
    specs: SpecRegistry | FrozenSpecRegistry | Iterable[SpecRegistry] | None = None,
    allow_unregistered: bool = False,
) -> ExecutionRuntime:
    """Assemble an :class:`ExecutionRuntime` in one call.

    Thin assembler over the standard composition — build a
    :class:`DepsRegistry` from ``deps_modules`` and/or raw ``deps`` blobs, build a
    :class:`LifecyclePlan` from ``lifecycle_modules`` and/or
    ``lifecycle_steps``, freeze both, and construct the runtime. No new
    semantics: validation still happens at freeze time, exactly as when
    composing the parts by hand.

    Equivalent to the composition it saves you, with every argument read as a sequence
    (a lone module or step being a sequence of one)::

        ExecutionRuntime(
            deps=DepsRegistry.from_modules(*deps_modules).with_deps(*deps).freeze(),
            lifecycle=LifecyclePlan.from_modules(*lifecycle_modules)
            .with_steps(*lifecycle_steps)
            .with_concurrent(lifecycle_concurrent)
            .freeze(),
        )

    Every collection here takes one item or many, so the common case says what it means::

        build_runtime(PostgresDepsModule(client=pg), lifecycle_steps=step)
        build_runtime(deps_modules=[postgres, redis], specs=[mine, TASKS.spec_contributions()])

    Enter :meth:`ExecutionRuntime.scope` yourself (or hand the runtime to
    ``forze_fastapi.runtime_lifespan`` for FastAPI apps).

    :param deps_modules: Deps module, or modules, to merge into the registry.
    :param deps: Raw registration deps blob(s) to merge after the modules.
    :param lifecycle_modules: Lifecycle module(s) contributing startup/shutdown steps.
    :param lifecycle_steps: Lifecycle step(s) appended after the modules' steps.
    :param lifecycle_concurrent: When ``True``, run lifecycle steps within the
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
    :param shutdown_step_timeout: Passed through to
        :attr:`ExecutionRuntime.shutdown_step_timeout` — the per-step ceiling on lifecycle
        shutdown, shared by every loop a step still has running. A component with its own
        drain window has to fit inside it. ``None`` (default) keeps the runtime's.
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
    :param cursor_token_signer: Passed through to
        :attr:`ExecutionRuntime.cursor_token_signer` — when set, every keyset cursor token
        is HMAC-signed and an unsigned or tampered one is rejected. ``None`` (default)
        leaves tokens unsigned.
    :param cursor_token_cipher: Passed through to
        :attr:`ExecutionRuntime.cursor_token_cipher` — AEAD for cursor tokens, superseding
        the signer. ``None`` (default) leaves confidentiality off.
    :param specs: This application's spec inventory — one registry, or every contribution
        to merge: the author's own specs, each ``AggregateKit.spec_contributions()``, and
        ``forze_identity.spec_contributions()`` if the identity plane is wired. Merging
        happens into a fresh registry, so none of the arguments is modified, and the result
        is frozen for you. When given, it is reconciled against the wired dependencies at
        construction: a spec catalogued but never bound, or a route bound but never
        catalogued, fails assembly. ``None`` (default) skips the check. Already-frozen
        registries are accepted alone but cannot be merged with others.
    :param allow_unregistered: Downgrade "bound but not catalogued" to a logged warning, for
        adopting the inventory incrementally.
    :returns: Runtime ready for :meth:`ExecutionRuntime.scope`.
    """

    if drain_timeout is None:
        drain_timeout = (
            timedelta(0) if deployment is DeploymentProfile.SERVERLESS else timedelta(seconds=10)
        )

    registry = DepsRegistry(modules=_many(deps_modules), deps=_many(deps))
    plan = LifecyclePlan(
        modules=_many(lifecycle_modules),
        steps=_many(lifecycle_steps),
        concurrent=lifecycle_concurrent,
    )

    # Left out of the call below rather than defaulted here: the runtime owns the value,
    # and repeating it would leave two places to change it.
    optional: dict[str, Any] = {}

    if shutdown_step_timeout is not None:
        optional["shutdown_step_timeout"] = shutdown_step_timeout

    return ExecutionRuntime(
        deps=registry.freeze(),
        lifecycle=plan.freeze(),
        spec_registry=_frozen_specs(specs),
        allow_unregistered=allow_unregistered,
        cache_resolved_operations=cache_resolved_operations,
        cache_resolved_ports=cache_resolved_ports,
        drain_timeout=drain_timeout,
        deployment=deployment,
        cpu_executor=cpu_executor,
        cpu_workers=cpu_workers,
        after_commit_error_handler=after_commit_error_handler,
        cursor_token_signer=cursor_token_signer,
        cursor_token_cipher=cursor_token_cipher,
        **optional,
    )
