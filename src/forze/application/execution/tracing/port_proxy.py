"""Tracing wrapper for configurable dependency ports."""

import inspect
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, cast

import attrs

from .emit import record

if TYPE_CHECKING:
    from ..deps.frozen import FrozenDeps

# ----------------------- #


def _default_tx_depth() -> int:
    return 0


# ....................... #


@attrs.define(slots=True)
class TracingPortProxy:
    """Wrap a port and record sync and async method calls."""

    inner: Any
    deps: "FrozenDeps"
    domain: str
    surface: str
    route: str | None
    phase: str | None
    tx_depth_getter: Callable[[], int] = attrs.field(default=_default_tx_depth)

    # ....................... #

    def _record_call(self, name: str) -> None:
        record(
            domain=self.domain,
            op=name,
            surface=self.surface,
            route=self.route,
            phase=self.phase,
            tx_depth=self.tx_depth_getter(),
            deps=self.deps,
        )

    # ....................... #

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self.inner, name)

        if not callable(attr):
            return attr

        if inspect.isasyncgenfunction(attr):

            @wraps(attr)
            async def traced_async_gen(*args: Any, **kwargs: Any) -> Any:
                self._record_call(name)
                async for item in attr(*args, **kwargs):
                    yield item

            return traced_async_gen

        if inspect.iscoroutinefunction(attr):

            @wraps(attr)
            async def traced_async(*args: Any, **kwargs: Any) -> Any:
                self._record_call(name)
                return await attr(*args, **kwargs)

            return traced_async

        @wraps(attr)
        def traced_sync(*args: Any, **kwargs: Any) -> Any:
            self._record_call(name)
            return attr(*args, **kwargs)

        return traced_sync


# ....................... #


def wrap_port[T](
    inner: T,
    *,
    deps: "FrozenDeps",
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
