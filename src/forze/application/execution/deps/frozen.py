"""Frozen dependency registry and per-scope resolver."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator, final

import attrs

from forze.application.contracts.base import BaseSpec
from forze.application.contracts.deps import DepKey
from forze.application.execution.tracing import RuntimeTrace
from forze.base.primitives import StrKey

from .port_instrumentation import maybe_wrap_configurable, record_simple_resolve
from .resolution import ResolutionFrame, frame_for
from .resolution_context import ResolutionContext
from .resolution_tracer import NOOP_RESOLUTION_TRACER, ResolutionTracer
from .runtime_tracer import NOOP_RUNTIME_TRACER, RuntimeTracer
from .store import PlainDepsMap, ProviderStore, RoutedDeps
from .trace import DepsResolutionTrace

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenDepsRegistry:
    """Frozen dependency registry with merged providers and tracer policy."""

    store: ProviderStore = attrs.field(factory=ProviderStore)
    """Merged provider store."""

    resolution_tracer: ResolutionTracer = attrs.field(default=NOOP_RESOLUTION_TRACER)
    """Resolution tracer applied when resolving."""

    runtime_tracer: RuntimeTracer = attrs.field(default=NOOP_RUNTIME_TRACER)
    """Runtime tracer applied when resolving."""

    # ....................... #

    def resolve(self) -> FrozenDeps:
        """Create a per-scope resolver with a fresh resolution context."""

        return FrozenDeps(
            store=self.store,
            resolution_tracer=self.resolution_tracer,
            runtime_tracer=self.runtime_tracer,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenDeps:
    """Per-scope dependency resolver (registration is frozen)."""

    store: ProviderStore = attrs.field(factory=ProviderStore)
    """Merged provider store (shared across scopes)."""

    resolution_tracer: ResolutionTracer = attrs.field(default=NOOP_RESOLUTION_TRACER)
    """Optional recorder for observed resolution edges."""

    runtime_tracer: RuntimeTracer = attrs.field(default=NOOP_RUNTIME_TRACER)
    """Optional recorder for runtime port and transaction events."""

    _resolution: ResolutionContext = attrs.field(
        default=attrs.Factory(
            lambda self: ResolutionContext(self.resolution_tracer),
            takes_self=True,
        ),
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )

    # ....................... #

    @property
    def plain_deps(self) -> PlainDepsMap:
        """Registered plain dependencies (read-only view)."""

        return self.store.plain_deps

    @property
    def routed_deps(self) -> RoutedDeps:
        """Registered routed dependencies (read-only view)."""

        return self.store.routed_deps

    @property
    def trace_resolution(self) -> bool:
        """Whether resolution edge recording is enabled (compat shim)."""

        return self.resolution_tracer.enabled

    @property
    def trace_runtime(self) -> bool:
        """Whether runtime event recording is enabled (compat shim)."""

        return self.runtime_tracer.enabled

    # ....................... #

    def record_runtime_event(
        self,
        *,
        domain: str,
        op: str,
        surface: str | None = None,
        route: str | None = None,
        phase: str | None = None,
        tx_depth: int = 0,
        tx_route: str | None = None,
    ) -> None:
        """Append a runtime tracing event when :attr:`runtime_tracer` is enabled."""

        self.runtime_tracer.record(
            domain=domain,
            op=op,
            surface=surface,
            route=route,
            phase=phase,
            tx_depth=tx_depth,
            tx_route=tx_route,
        )

    # ....................... #

    def provide[T](
        self,
        key: DepKey[T],
        *,
        route: StrKey | None = None,
        fallback_to_plain: bool = True,
    ) -> T:
        """Return a dependency value for the given key."""

        frame = frame_for(key, route)
        self._resolution.assert_not_active(frame)
        self._resolution.record_provide_edge(frame)

        return self.store.get_provider(
            key,
            route=route,
            fallback_to_plain=fallback_to_plain,
        )

    # ....................... #

    @contextmanager
    def resolution_scope(
        self,
        key: DepKey[Any],
        *,
        route: StrKey | None = None,
    ) -> Iterator[None]:
        """Enter a resolution scope for ``key`` (and optional ``route``)."""

        frame = frame_for(key, route)
        token = self._resolution.push(frame)

        try:
            yield

        finally:
            self._resolution.pop(token)

    # ....................... #

    def resolve_configurable(
        self,
        ctx: ExecutionContext,
        key: DepKey[Any],
        spec: BaseSpec,
        *,
        route: StrKey | None = None,
    ) -> Any:
        """Resolve a configurable dependency: lookup factory and invoke with ``spec``.

        Resolved ports are memoized per scope on ``ctx`` (keyed by ``(key, route)`` and
        validated against ``spec`` by value equality, so per-call-constructed but
        structurally equal specs reuse the cached port) when port caching is enabled.
        Caching is bypassed while resolution tracing is active so per-task resolution
        traces stay complete.
        """

        cache_key = (key, route)
        use_cache = not self.resolution_tracer.enabled

        if use_cache:
            cached = ctx.cached_port(cache_key, spec)

            if cached is not None:
                return cached

        frame = frame_for(key, route)
        token = self._resolution.push(frame)

        try:
            factory = self.store.get_provider(key, route=route)
            result = factory(ctx, spec)
            port = maybe_wrap_configurable(self, ctx, key, spec, route, result)

        finally:
            self._resolution.pop(token)

        if use_cache:
            ctx.store_port(cache_key, spec, port)

        return port

    # ....................... #

    def resolve_simple(
        self,
        ctx: ExecutionContext,
        key: DepKey[Any],
        *,
        route: StrKey | None = None,
    ) -> Any:
        """Resolve a simple dependency: lookup factory and invoke with ``ctx`` only."""

        frame = frame_for(key, route)
        token = self._resolution.push(frame)

        try:
            factory = self.store.get_provider(key, route=route)
            result = factory(ctx)
            record_simple_resolve(self, ctx, key, route)
            return result

        finally:
            self._resolution.pop(token)

    # ....................... #

    def resolution_trace(self) -> DepsResolutionTrace | None:
        """Return the observed resolution trace for the current task, if any."""

        return self.resolution_tracer.snapshot()

    # ....................... #

    def runtime_trace(self) -> RuntimeTrace | None:
        """Return the observed runtime trace for the current task, if any."""

        return self.runtime_tracer.snapshot()

    # ....................... #

    def registered_frames(self) -> frozenset[ResolutionFrame]:
        """Return all registered dependency frames (static inventory)."""

        return self.store.registered_frames()

    # ....................... #

    def exists[T](self, key: DepKey[T], *, route: StrKey | None = None) -> bool:
        """Return ``True`` if the dependency is registered."""

        return self.store.exists(key, route=route)
