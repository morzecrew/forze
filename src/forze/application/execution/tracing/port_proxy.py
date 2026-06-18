"""Tracing wrapper for configurable dependency ports."""

import inspect
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, cast
from uuid import UUID

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

    @staticmethod
    def _key_of(args: tuple[Any, ...]) -> str | None:
        """The targeted entity key, when the first positional arg is an id.

        Captured id-only (``UUID`` / ``int`` — e.g. a document primary key) so the trace
        carries a per-entity key for assertions without recording free-form (possibly PII)
        values. ``None`` when the call is not keyed by a leading id (creates, queries)."""

        if args and isinstance(args[0], (UUID, int)):
            return str(args[0])

        return None

    def _record_call(self, name: str, args: tuple[Any, ...] = ()) -> None:
        record(
            domain=self.domain,
            op=name,
            surface=self.surface,
            route=self.route,
            phase=self.phase,
            tx_depth=self.tx_depth_getter(),
            key=self._key_of(args),
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
                self._record_call(name, args)
                async for item in attr(*args, **kwargs):
                    yield item

            return traced_async_gen

        if inspect.iscoroutinefunction(attr):

            @wraps(attr)
            async def traced_async(*args: Any, **kwargs: Any) -> Any:
                self._record_call(name, args)
                return await attr(*args, **kwargs)

            return traced_async

        @wraps(attr)
        def traced_sync(*args: Any, **kwargs: Any) -> Any:
            self._record_call(name, args)
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
