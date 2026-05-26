"""Tracing wrapper for configurable dependency ports."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

import attrs

from .emit import record

if TYPE_CHECKING:
    from ..deps.container import Deps

# ----------------------- #


def _default_tx_depth() -> int:
    return 0


# ....................... #


@attrs.define(slots=True)
class TracingPortProxy:
    """Wrap a port and record async method calls before ``await``."""

    inner: Any
    deps: Deps[Any]
    domain: str
    surface: str
    route: str | None
    phase: str | None
    tx_depth_getter: Callable[[], int] = attrs.field(default=_default_tx_depth)

    # ....................... #

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self.inner, name)

        if not callable(attr):
            return attr

        async def traced(*args: Any, **kwargs: Any) -> Any:
            record(
                domain=self.domain,
                op=name,
                surface=self.surface,
                route=self.route,
                phase=self.phase,
                tx_depth=self.tx_depth_getter(),
                deps=self.deps,
            )
            return await attr(*args, **kwargs)  # type: ignore[return-value]

        return traced


# ....................... #


def wrap_port[T](
    inner: T,
    *,
    deps: Deps[Any],
    domain: str,
    surface: str,
    route: str | None,
    phase: str | None,
    tx_depth_getter: Callable[[], int] | None = None,
) -> T:
    """Return *inner* wrapped for runtime tracing."""

    return cast(
        T,
        TracingPortProxy(
            inner=inner,
            deps=deps,
            domain=domain,
            surface=surface,
            route=route,
            phase=phase,
            tx_depth_getter=tx_depth_getter or _default_tx_depth,
        ),
    )
