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

    A *wrapped* port reads the ambient chain per call, so ambient interceptors bound later
    still apply to it. A port resolved and cached while no interceptor existed would stay bare
    — so ``resolve_configurable`` bypasses the port cache whenever an ambient chain is bound
    (the same as under resolution tracing), re-resolving and rewrapping each call against the
    current chain. A binding established at any time is therefore picked up.
    """

    from ..interception import current_interceptors, wrap_intercepted

    if not deps.interceptors and not current_interceptors():
        return result

    from ..tracing.port_proxy import infer_port_metadata

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

    from ..tracing.port_proxy import infer_port_metadata
    from ..tracing.port_proxy import wrap_port

    domain, surface, route_name, phase = infer_port_metadata(
        key,
        spec,
        route=route,
    )
    capture = deps.runtime_tracer.capture_values
    return wrap_port(
        result,
        deps=deps,
        domain=domain,
        surface=surface,
        route=route_name,
        phase=phase,
        tx_depth_getter=ctx.tx_ctx.depth,
        capture=capture,
        redact=_sensitive_fields(spec) if capture else frozenset(),
    )


# ....................... #


def _sensitive_fields(spec: BaseSpec) -> frozenset[str]:
    """The spec's declared-sensitive field names — what value capture redacts.

    Reuses the encryption signal (``spec.encryption.encrypted`` ∪ ``.searchable``); a spec with no
    declared encryption contributes nothing (captured values stay unmasked — safe, sim data only).
    """

    encryption = getattr(spec, "encryption", None)
    if encryption is None:
        return frozenset()

    encrypted = getattr(encryption, "encrypted", ()) or ()
    searchable = getattr(encryption, "searchable", ()) or ()
    return frozenset(encrypted) | frozenset(searchable)


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

    from ..tracing.port_proxy import infer_port_metadata

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
