"""Runtime tracing hooks for resolved dependency ports."""

from typing import TYPE_CHECKING, Any

from forze.application.contracts.base import BaseSpec
from forze.application.contracts.deps import DepKey
from forze.base.primitives import StrKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

    from .container import Deps

# ----------------------- #


def maybe_wrap_configurable(
    deps: "Deps[Any]",
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


def record_simple_resolve(
    deps: "Deps[Any]",
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
