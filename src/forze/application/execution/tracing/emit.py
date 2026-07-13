"""Runtime tracing recording pipeline: the per-task active deps and the record site.

``bind_active_deps`` binds the resolver for the current task; ``record`` reads the active
runtime tracer (or one passed explicitly) and appends an event when tracing is enabled.
"""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..deps.frozen import FrozenDeps
    from .tracers import RuntimeTracer

# ----------------------- #

_active_deps: ContextVar["FrozenDeps | None"] = ContextVar(
    "forze_active_deps",
    default=None,
)


# A run-global transaction-id counter, bound once around a whole simulation run (not per task) so
# every transaction across every execution context sharing the run gets a unique, replay-stable id.
# Unbound in production (``None``) → ``tx_id`` is never stamped, so the trace stays id-only and the
# seam costs nothing. A mutable single-element box: tasks copy the ContextVar's reference at spawn,
# so they share and advance the one counter (deterministic, since root-enter order is deterministic
# under the scheduler).
_tx_sequence: ContextVar[list[int] | None] = ContextVar(
    "forze_tx_sequence",
    default=None,
)


def next_tx_id() -> int | None:
    """Mint the next run-global transaction id, or ``None`` when no run counter is bound."""

    box = _tx_sequence.get()

    if box is None:
        return None

    box[0] += 1
    return box[0]


@contextmanager
def bind_tx_sequence() -> Iterator[None]:
    """Bind a fresh run-global transaction-id counter for the duration of the block.

    Bind once around a whole simulation run (around the gather of all tasks), not per task, so the
    counter is shared and transaction ids are unique across the run.
    """

    token = _tx_sequence.set([0])

    try:
        yield

    finally:
        _tx_sequence.reset(token)


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
    tx_id: int | None = None,
    key: str | None = None,
    outcome: str | None = None,
    error: str | None = None,
    corr: int | None = None,
    nested: bool = False,
    payload: "Mapping[str, Any] | None" = None,
    result: "Mapping[str, Any] | None" = None,
    result_native: "Mapping[str, Any] | None" = None,
    deps: "FrozenDeps | None" = None,
) -> int | None:
    """Append a runtime event when tracing is enabled; return its ``seq`` (``None`` if disabled).

    The returned ``seq`` lets an operation boundary correlate its terminal back to its invoke:
    pass the invoke's ``seq`` as ``corr`` on the matching ``complete``/``error`` record.
    """

    tracer: RuntimeTracer | None

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
        tx_id=tx_id,
        key=key,
        outcome=outcome,
        error=error,
        corr=corr,
        nested=nested,
        payload=payload,
        result=result,
        result_native=result_native,
    )
