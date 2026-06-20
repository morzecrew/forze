"""Tracing wrapper for configurable dependency ports."""

import inspect
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Mapping, cast
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
    capture: bool = False
    """Capture redaction-applied call values (payload/result) onto the trace — DST only; off in
    production so the trace stays id-only and PII-free."""
    redact: frozenset[str] = frozenset()
    """Field names to mask to ``"<redacted>"`` when capturing — the spec's declared-sensitive
    fields (``encryption.encrypted`` ∪ ``encryption.searchable``)."""

    # ....................... #

    @staticmethod
    def _key_of(args: tuple[Any, ...]) -> str | None:
        """The targeted entity key, when the first positional arg is an id.

        Captured id-only (``UUID`` / ``int`` — e.g. a document primary key) so the trace
        carries a per-entity key for assertions without recording free-form (possibly PII)
        values. ``None`` when the call is not keyed by a leading id (creates, queries).
        """

        return str(args[0]) if args and isinstance(args[0], (UUID, int)) else None

    # ....................... #

    @staticmethod
    def _dump(value: Any) -> dict[str, Any] | None:
        """A structural ``dict`` view of *value* (a write DTO / read model), or ``None`` for a
        scalar / id / unstructured value — so capture only records the meaningful payloads.
        """

        if value is None or isinstance(value, (str, bytes, int, float, bool, UUID)):
            return None

        dump = getattr(value, "model_dump", None)

        if callable(dump):
            data = dump()

        elif attrs.has(type(value)):  # pyright: ignore[reportUnknownArgumentType]
            data = attrs.asdict(value)

        elif isinstance(value, Mapping):
            data = dict(  # pyright: ignore[reportUnknownVariableType]
                value  # pyright: ignore[reportUnknownArgumentType]
            )

        else:
            return None

        return (
            data if isinstance(data, dict) else None
        )  # pyright: ignore[reportUnknownVariableType]

    # ....................... #

    def _redact(self, data: dict[str, Any]) -> dict[str, Any]:
        """Mask the spec's declared-sensitive fields to ``"<redacted>"`` (shallow, top-level)."""

        if not self.redact:
            return data

        return {
            key: ("<redacted>" if key in self.redact else value)
            for key, value in data.items()
        }

    # ....................... #

    def _payload_of(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Mapping[str, Any] | None:
        """The first structured argument as a redacted value map — the write payload."""

        for value in (*args, *kwargs.values()):
            data = self._dump(value)
            if data is not None:
                return self._redact(data)

        return None

    # ....................... #

    def _record_call(
        self,
        name: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        record(
            domain=self.domain,
            op=name,
            surface=self.surface,
            route=self.route,
            phase=self.phase,
            tx_depth=self.tx_depth_getter(),
            key=self._key_of(args),
            payload=(self._payload_of(args, kwargs or {}) if self.capture else None),
            deps=self.deps,
        )

    # ....................... #

    def _record_return(self, name: str, args: tuple[Any, ...], result: Any) -> None:
        """Record a read's returned value on a return event (capture mode only)."""

        if not self.capture:
            return

        data = self._dump(result)
        if data is None:
            return

        record(
            domain=self.domain,
            op=name,
            surface=self.surface,
            route=self.route,
            phase=self.phase,
            tx_depth=self.tx_depth_getter(),
            key=self._key_of(args),
            result=self._redact(data),
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
                self._record_call(name, args, kwargs)
                async for item in attr(*args, **kwargs):
                    yield item

            return traced_async_gen

        if inspect.iscoroutinefunction(attr):

            @wraps(attr)
            async def traced_async(*args: Any, **kwargs: Any) -> Any:
                self._record_call(name, args, kwargs)
                result = await attr(*args, **kwargs)
                self._record_return(name, args, result)
                return result

            return traced_async

        @wraps(attr)
        def traced_sync(*args: Any, **kwargs: Any) -> Any:
            self._record_call(name, args, kwargs)
            result = attr(*args, **kwargs)
            self._record_return(name, args, result)
            return result

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
    capture: bool = False,
    redact: frozenset[str] = frozenset(),
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
            capture=capture,
            redact=redact,
        ),
    )
