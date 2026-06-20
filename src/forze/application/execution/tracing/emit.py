"""Runtime tracing recording pipeline: the per-task active deps and the record site.

``bind_active_deps`` binds the resolver for the current task; ``record`` reads the active
runtime tracer (or one passed explicitly) and appends an event when tracing is enabled.
"""

from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from ..deps.frozen import FrozenDeps

    from .tracers import RuntimeTracer

# ----------------------- #

_active_deps: ContextVar["FrozenDeps | None"] = ContextVar(
    "forze_active_deps",
    default=None,
)


# ....................... #


def active_deps() -> "FrozenDeps | None":
    """Return the :class:`~forze.application.execution.deps.container.Deps` bound for the current task."""

    return _active_deps.get()


# ....................... #


def active_runtime_tracer() -> "RuntimeTracer | None":
    """Return the runtime tracer from the active deps when recording is enabled."""

    deps = _active_deps.get()

    if deps is None or not deps.runtime_tracer.enabled:
        return None

    return deps.runtime_tracer


# ....................... #


def bind_active_deps(deps: "FrozenDeps | None") -> Token["FrozenDeps | None"]:
    """Bind *deps* as the active container for runtime tracing in the current task."""

    return _active_deps.set(deps)


# ....................... #


def init_runtime_tracing(deps: "FrozenDeps") -> None:
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
    key: str | None = None,
    outcome: str | None = None,
    error: str | None = None,
    corr: int | None = None,
    nested: bool = False,
    payload: "Mapping[str, Any] | None" = None,
    result: "Mapping[str, Any] | None" = None,
    deps: "FrozenDeps | None" = None,
) -> int | None:
    """Append a runtime event when tracing is enabled; return its ``seq`` (``None`` if disabled).

    The returned ``seq`` lets an operation boundary correlate its terminal back to its invoke:
    pass the invoke's ``seq`` as ``corr`` on the matching ``complete``/``error`` record.
    """

    tracer: "RuntimeTracer | None"

    if deps is not None:
        tracer = deps.runtime_tracer if deps.runtime_tracer.enabled else None

    else:
        tracer = active_runtime_tracer()

    if tracer is None or not tracer.enabled:
        return None

    return tracer.record(
        domain=domain,
        op=op,
        surface=surface,
        route=route,
        phase=phase,
        tx_depth=tx_depth,
        tx_route=tx_route,
        key=key,
        outcome=outcome,
        error=error,
        corr=corr,
        nested=nested,
        payload=payload,
        result=result,
    )
