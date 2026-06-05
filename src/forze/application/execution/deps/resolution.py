"""Resolution frame types and cycle error formatting."""

from functools import lru_cache
from typing import final

import attrs

from forze.application.contracts.deps import DepKey
from forze.base.primitives import StrKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class ResolutionFrame:
    """One step in an active dependency resolution chain."""

    key_name: str
    """Registered dependency key name."""

    route: str | None
    """Optional route label (``None`` for plain deps)."""

    # ....................... #

    def label(self) -> str:
        """Human-readable frame label for error messages."""

        if self.route is None:
            return self.key_name

        return f"{self.key_name}@{self.route}"


# ....................... #


@lru_cache(maxsize=None)
def frame_for[T](key: DepKey[T], route: StrKey | None) -> ResolutionFrame:
    """Build a resolution frame from a dep key and optional route.

    Memoized: ``(key, route)`` is a finite, hashable space (registered keys ×
    routes), and :class:`ResolutionFrame` is immutable, so frames are reused
    across resolutions instead of reallocated per call. Cycle detection compares
    frames by value, so sharing instances does not change behavior.
    """

    return ResolutionFrame(
        key_name=key.name,
        route=str(route) if route is not None else None,
    )


# ....................... #


def format_cycle_error(
    stack: tuple[ResolutionFrame, ...],
    frame: ResolutionFrame,
) -> str:
    """Format a cyclic dependency error message."""

    chain = " -> ".join(f.label() for f in stack)

    return f"Cyclic dependency resolution: {chain} -> {frame.label()}"
