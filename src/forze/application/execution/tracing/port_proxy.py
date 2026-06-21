"""Tracing wrapper for configurable dependency ports, with port-metadata inference."""

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Mapping, cast
from uuid import UUID

import attrs

from forze.application.contracts.deps import DepKey
from forze.base.primitives import StrKey

from ..port_proxy_base import PortProxy
from .emit import record

if TYPE_CHECKING:
    from ..deps.frozen import FrozenDeps


# ....................... #


def infer_port_metadata(
    key: DepKey[object],
    spec: object,
    *,
    route: StrKey | None,
) -> tuple[str, str, str | None, str | None]:
    """Return ``(domain, surface, route, phase)`` for a configurable port resolution."""

    surface = key.name
    phase: str | None = None

    if surface.endswith("_query"):
        phase = "query"
    elif surface.endswith("_command"):
        phase = "command"

    domain = surface.split("_", 1)[0] if "_" in surface else surface
    route_name = getattr(spec, "name", None)

    if route_name is None and route is not None:
        route_name = str(getattr(route, "value", route))

    return domain, surface, route_name, phase

# ----------------------- #


def _default_tx_depth() -> int:
    return 0


# ....................... #


@attrs.define(slots=True)
class TracingPortProxy(PortProxy):
    """Wrap a port and record sync and async method calls."""

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
            # ``mode="json"`` keeps the captured value JSON-native (UUID → str, datetime →
            # ISO-8601), so the trace / timeline / bundle stay portable and deterministic.
            try:
                data = dump(mode="json")
            except TypeError:  # a model_dump without the mode kwarg
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

    def _wrap_async_gen(self, name: str, attr: Any) -> Any:
        @wraps(attr)
        async def traced_async_gen(*args: Any, **kwargs: Any) -> Any:
            self._record_call(name, args, kwargs)
            async for item in attr(*args, **kwargs):
                yield item

        return traced_async_gen

    # ....................... #

    def _wrap_async(self, name: str, attr: Any) -> Any:
        @wraps(attr)
        async def traced_async(*args: Any, **kwargs: Any) -> Any:
            self._record_call(name, args, kwargs)
            result = await attr(*args, **kwargs)
            self._record_return(name, args, result)
            return result

        return traced_async

    # ....................... #

    def _wrap_sync(self, name: str, attr: Any) -> Any:
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
