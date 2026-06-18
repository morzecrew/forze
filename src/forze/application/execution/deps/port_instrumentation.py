"""Runtime tracing and resilience-policy hooks for resolved dependency ports."""

from typing import TYPE_CHECKING, Any

from forze.application.contracts.base import BaseSpec
from forze.application.contracts.deps import DepKey
from forze.application.contracts.resilience import ResiliencePortPoliciesDepKey
from forze.base.primitives import StrKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

    from .frozen import FrozenDeps

# ----------------------- #


def maybe_wrap_interceptors(
    deps: "FrozenDeps",
    ctx: "ExecutionContext",
    key: DepKey[Any],
    spec: BaseSpec,
    route: StrKey | None,
    result: Any,
) -> Any:
    """Wrap a configurable port in the port-interceptor chain when any interceptor applies.

    The effective chain is the deps-scoped interceptors plus the ambient (run-scoped) chain;
    when both are empty the port is returned bare (zero cost in production). Applied
    **innermost** — inside the runtime-tracing and resilience wraps.

    Contract: a *wrapped* port reads the ambient chain per call, so ambient interceptors bound
    later still apply to it; but the wrap *decision* is made once per resolve (and ports are
    cached per scope), so ambient interceptors must be bound **before** the ports they should
    wrap are first resolved — a port resolved while no interceptor exists stays bare and won't
    retroactively pick up a later ambient binding. ``run_simulation`` honors this (it binds the
    cooperative interceptor before the scenario resolves any port).
    """

    from ..interception import current_interceptors, wrap_intercepted

    if not deps.interceptors and not current_interceptors():
        return result

    from ..tracing.metadata import infer_port_metadata

    _domain, surface, route_name, _phase = infer_port_metadata(key, spec, route=route)

    return wrap_intercepted(
        result,
        interceptors=deps.interceptors,
        surface=surface,
        route=route_name,
    )


# ....................... #


def maybe_wrap_configurable(
    deps: "FrozenDeps",
    ctx: "ExecutionContext",
    key: DepKey[Any],
    spec: BaseSpec,
    route: StrKey | None,
    result: Any,
) -> Any:
    """Wrap a configurable port for runtime tracing when enabled."""

    if not deps.runtime_tracer.enabled:
        return result

    from ..tracing.metadata import infer_port_metadata
    from ..tracing.port_proxy import wrap_port

    domain, surface, route_name, phase = infer_port_metadata(
        key,
        spec,
        route=route,
    )
    return wrap_port(
        result,
        deps=deps,
        domain=domain,
        surface=surface,
        route=route_name,
        phase=phase,
        tx_depth_getter=ctx.tx_ctx.depth,
    )


# ....................... #


def maybe_wrap_port_policy(
    deps: "FrozenDeps",
    ctx: "ExecutionContext",
    key: DepKey[Any],
    route: StrKey | None,
    result: Any,
) -> Any:
    """Wrap a configurable port under a declared resilience port policy.

    Applied **outside** the tracing wrap from :func:`maybe_wrap_configurable`,
    so trace events record the real call: each retry attempt inside the policy
    re-invokes the traced method, and a rejected (throttled / bulkhead-full /
    breaker-open) call never records a phantom port event — rejections surface
    as the executor's own ``domain="resilience"`` events instead.
    """

    table = deps.store.plain_deps.get(ResiliencePortPoliciesDepKey)

    if not table:
        return result

    port_policy = table.get(key)

    if port_policy is None:
        return result

    from ..resilience.port_policy import wrap_port_policy

    return wrap_port_policy(
        result,
        ctx=ctx,
        port_policy=port_policy,
        resolved_route=route,
    )


# ....................... #


def record_simple_resolve(
    deps: "FrozenDeps",
    ctx: "ExecutionContext",
    key: DepKey[Any],
    route: StrKey | None,
) -> None:
    """Record a simple dependency resolve event when runtime tracing is enabled."""

    if not deps.runtime_tracer.enabled:
        return

    from ..tracing.metadata import infer_port_metadata

    domain, surface, route_name, phase = infer_port_metadata(
        key,
        object(),
        route=route,
    )
    deps.record_runtime_event(
        domain=domain,
        op="resolve",
        surface=surface,
        route=route_name,
        phase=phase,
        tx_depth=ctx.tx_ctx.depth(),
    )
