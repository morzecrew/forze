"""Per-task active dependency container for runtime tracing emit sites."""

from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..deps.frozen import FrozenDeps

    from .runtime_tracer import RuntimeTracer

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
