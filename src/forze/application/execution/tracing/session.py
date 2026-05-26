"""Per-task active dependency container for runtime tracing emit sites."""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..deps.container import Deps

# ----------------------- #

_active_deps: ContextVar[Deps[Any] | None] = ContextVar(
    "forze_active_deps",
    default=None,
)

# ....................... #


def active_deps() -> Deps[Any] | None:
    """Return the :class:`~forze.application.execution.deps.container.Deps` bound for the current task."""

    return _active_deps.get()


# ....................... #


def bind_active_deps(deps: Deps[Any] | None) -> Token[Deps[Any] | None]:
    """Bind *deps* as the active container for runtime tracing in the current task."""

    return _active_deps.set(deps)
