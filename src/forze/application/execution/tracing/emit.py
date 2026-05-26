"""Record runtime tracing events on the active dependency container."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .session import active_deps

if TYPE_CHECKING:
    from ..deps.container import Deps

# ----------------------- #


def init_runtime_tracing(deps: Deps[Any]) -> None:
    """Ensure an empty :class:`~forze.application.execution.tracing.buffer.RuntimeTrace` exists when tracing is enabled."""

    if deps.trace_runtime:
        deps._runtime_trace_get_or_create()  # pyright: ignore[reportPrivateUsage]


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

    container = deps if deps is not None else active_deps()

    if container is None or not container.trace_runtime:
        return

    container.record_runtime_event(
        domain=domain,
        op=op,
        surface=surface,
        route=route,
        phase=phase,
        tx_depth=tx_depth,
        tx_route=tx_route,
    )
