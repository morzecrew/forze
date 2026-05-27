"""Record runtime tracing events on the active dependency container."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .session import active_runtime_tracer

if TYPE_CHECKING:
    from ..deps.container import Deps
    from ..deps.runtime_tracer import RuntimeTracer

# ----------------------- #


def init_runtime_tracing(deps: Deps[Any]) -> None:
    """Ensure an empty runtime trace buffer exists when tracing is enabled."""

    if deps.runtime_tracer.enabled:
        deps.runtime_tracer.init_task()


# ....................... #


def record(
    *,
    domain: str,
    op: str,
    surface: str | None = None,
    route: str | None = None,
    phase: str | None = None,
    tx_depth: int = 0,
    tx_route: str | None = None,
    deps: Deps[Any] | None = None,
) -> None:
    """Append a runtime event when tracing is enabled on the active or given *deps*."""

    tracer: RuntimeTracer | None

    if deps is not None:
        tracer = deps.runtime_tracer if deps.runtime_tracer.enabled else None
    else:
        tracer = active_runtime_tracer()

    if tracer is None or not tracer.enabled:
        return

    tracer.record(
        domain=domain,
        op=op,
        surface=surface,
        route=route,
        phase=phase,
        tx_depth=tx_depth,
        tx_route=tx_route,
    )
