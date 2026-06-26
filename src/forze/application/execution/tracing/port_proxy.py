"""Tracing wrapper for configurable dependency ports, with port-metadata inference."""

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Mapping, cast
from uuid import UUID

import attrs

from forze.application.contracts.base.value_objects import CountlessPage, CursorPage
from forze.application.contracts.deps import DepKey
from forze.base.primitives import JsonDict, StrKey

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

REDACTED = "<redacted>"
"""The mask a captured value's declared-sensitive fields are replaced with. A single source so a
trace consumer (e.g. the isolation oracle's predicate matcher) can detect a redacted value and
refuse to reason over it rather than treat the mask as a real value."""


def _default_tx_depth() -> int:
    return 0


def _default_tx_id() -> int | None:
    return None


# ....................... #


@attrs.define(slots=True)
class TracingPortProxy(PortProxy):
    """Wrap a port and record sync and async method calls."""

    deps: "FrozenDeps"
    """The frozen dependencies."""

    domain: str
    """The domain of the port."""

    surface: str
    """The surface of the port."""

    route: str | None
    """The route of the port."""

    phase: str | None
    """The phase of the port."""

    tx_depth_getter: Callable[[], int] = attrs.field(default=_default_tx_depth)
    """Returns the active transaction nesting depth."""

    tx_id_getter: Callable[[], int | None] = attrs.field(default=_default_tx_id)
    """Returns the active root transaction's run-global id (``None`` outside a tx / in production) —
    lets the oracle group a port call into the transaction that issued it."""

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
    def _dump(value: Any, *, mode: str = "json") -> JsonDict | None:
        """A structural ``dict`` view of *value* (a write DTO / read model), or ``None`` for a
        scalar / id / unstructured value — so capture only records the meaningful payloads.

        ``mode="json"`` (default) keeps values portable (UUID → str, datetime → ISO-8601) for the
        timeline/bundle; ``mode="python"`` keeps them native (the form the backend's in-memory scan
        matches a filter against), which the isolation oracle needs so its predicate evaluation agrees
        with the live scan rather than comparing a JSON string to a native value.
        """

        if value is None or isinstance(value, (str, bytes, int, float, bool, UUID)):
            return None

        dump = getattr(value, "model_dump", None)

        if callable(dump):
            try:
                data = dump(mode=mode)

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

    def _redact(self, data: JsonDict) -> JsonDict:
        """Mask the spec's declared-sensitive fields to ``"<redacted>"`` (shallow, top-level)."""

        if not self.redact:
            return data

        return {
            key: (REDACTED if key in self.redact else value)
            for key, value in data.items()
        }

    # ....................... #

    def _payload_of(
        self,
        args: tuple[Any, ...],
        kwargs: JsonDict,
    ) -> Mapping[str, Any] | None:
        """The first structured argument as a redacted value map — the write payload."""

        for value in (*args, *kwargs.values()):
            data = self._dump(value)
            if data is not None:
                return self._redact(data)

        return None

    # ....................... #

    def _captured_in(
        self,
        args: tuple[Any, ...],
        kwargs: JsonDict,
    ) -> Mapping[str, Any] | None:
        """The captured input for a call — the scan predicate for a query, else the write payload.

        For a query the predicate is the ``filters`` argument specifically — the explicit ``filters``
        kwarg when present, else the leading positional (filters is first on every filter-led scan:
        ``find_many``/``count``/…). Preferring the named kwarg keeps the right argument even for ops
        whose first positional is not the filter (``aggregate_many(aggregates, filters=…)``), and a
        *following* positional — pagination, sorts — is never mistaken for it; a match-all scan or a
        point ``get`` captures ``None``. This precision matters downstream: the isolation oracle
        re-evaluates the captured filter against concurrent writes, so a wrong argument standing in for
        the filter would be a wrong predicate. A command keeps the first-structured-argument rule.
        """

        if self.phase == "query":
            candidate = (
                kwargs["filters"]
                if "filters" in kwargs
                else (args[0] if args else None)
            )
            data = self._dump(candidate)
            return self._redact(data) if data is not None else None

        return self._payload_of(args, kwargs)

    # ....................... #

    def _record_call(
        self,
        name: str,
        args: tuple[Any, ...] = (),
        kwargs: JsonDict | None = None,
    ) -> None:
        record(
            domain=self.domain,
            op=name,
            surface=self.surface,
            route=self.route,
            phase=self.phase,
            tx_depth=self.tx_depth_getter(),
            tx_id=self.tx_id_getter(),
            key=self._key_of(args),
            payload=(self._captured_in(args, kwargs or {}) if self.capture else None),
            deps=self.deps,
        )

    # ....................... #

    def _record_return(self, name: str, args: tuple[Any, ...], result: Any) -> None:
        """Record a call's returned value(s) on return event(s) (capture mode only).

        A batch operation returns a ``list`` of entities (``create_many``/``update_many``); each is
        recorded as its own return event, so the value-trace — and the per-commit cross-aggregate
        oracle, which reconstructs state from these results — sees every written entity, not just
        single-entity writes. A scan **page** (``find_many``/``find_page``/``find_cursor``) is unwrapped
        the same way — each hit becomes its own return event — so the isolation oracle sees the
        individual rows a predicate read returned (``_dump`` would otherwise ``attrs.asdict`` the page
        wrapper and leave the nested pydantic hits un-dumped). A non-list, non-page (a single read
        model, or a ``(model, diff)`` tuple that ``_dump`` treats as unstructured) is recorded as one
        event as before.
        """

        if not self.capture:
            return

        if isinstance(result, list):
            items = cast("list[Any]", result)  # type: ignore[redundant-cast]
        elif isinstance(result, (CountlessPage, CursorPage)):
            items = cast("list[Any]", result.hits)  # type: ignore[redundant-cast]
        else:
            items = [result]

        for item in items:
            data = self._dump(item)

            if data is None:
                continue

            # A write result also keeps a NATIVE-typed copy (``result_native``): the isolation oracle
            # matches a captured scan predicate against it, and the backend scans the native row — so a
            # JSON copy (UUID/IP/Decimal/datetime → str) would make the oracle's match diverge from the
            # live scan and manufacture a false phantom edge. Reads keep only the JSON ``result``.
            native = (
                self._redact(self._dump(item, mode="python") or data)
                if self.phase == "command"
                else None
            )

            record(
                domain=self.domain,
                op=name,
                surface=self.surface,
                route=self.route,
                phase=self.phase,
                tx_depth=self.tx_depth_getter(),
                tx_id=self.tx_id_getter(),
                key=self._key_of(args),
                result=self._redact(data),
                result_native=native,
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
    tx_id_getter: Callable[[], int | None] | None = None,
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
            tx_id_getter=tx_id_getter or _default_tx_id,
            capture=capture,
            redact=redact,
        ),
    )
