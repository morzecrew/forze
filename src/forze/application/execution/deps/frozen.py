"""Frozen dependency registry and per-scope resolver."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator, final

import attrs

from forze.application.contracts.base import BaseSpec
from forze.application.contracts.deps import DepKey
from forze.application.execution.tracing import (
    NOOP_RUNTIME_TRACER,
    RuntimeTrace,
    RuntimeTracer,
)
from forze.base.primitives import StrKey

from ..interception import PortInterceptorChain
from .port_instrumentation import (
    maybe_wrap_configurable,
    maybe_wrap_interceptors,
    maybe_wrap_otel_spans,
    maybe_wrap_port_policy,
    record_simple_resolve,
)
from .resolution import (
    NOOP_RESOLUTION_TRACER,
    DepsResolutionTrace,
    ResolutionContext,
    ResolutionFrame,
    ResolutionTracer,
    frame_for,
)
from forze.application.contracts.deps import PlainDepsMap, ProviderStore, RoutedDeps

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenDepsRegistry:
    """Frozen dependency registry with merged providers and tracer policy."""

    store: ProviderStore = attrs.field(factory=ProviderStore)
    """Merged provider store."""

    resolution_tracer: ResolutionTracer = NOOP_RESOLUTION_TRACER
    """Resolution tracer applied when resolving."""

    runtime_tracer: RuntimeTracer = NOOP_RUNTIME_TRACER
    """Runtime tracer applied when resolving."""

    interceptors: PortInterceptorChain = attrs.field(factory=tuple)
    """Deps-scoped port interceptors applied to every resolved configurable port."""

    otel_port_tracer: Any = None
    """When set, every resolved configurable port emits a per-call OpenTelemetry client span through
    this tracer (an OTel ``Tracer``; production observability, opt-in via
    ``DepsRegistry.with_otel_port_spans``). ``None`` leaves ports bare (zero cost)."""

    # ....................... #

    def resolve(self) -> FrozenDeps:
        """Create a per-scope resolver with a fresh resolution context."""

        return FrozenDeps(
            store=self.store,
            resolution_tracer=self.resolution_tracer,
            runtime_tracer=self.runtime_tracer,
            interceptors=self.interceptors,
            otel_port_tracer=self.otel_port_tracer,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenDeps:
    """Per-scope dependency resolver (registration is frozen)."""

    store: ProviderStore = attrs.field(factory=ProviderStore)
    """Merged provider store (shared across scopes)."""

    resolution_tracer: ResolutionTracer = NOOP_RESOLUTION_TRACER
    """Optional recorder for observed resolution edges."""

    runtime_tracer: RuntimeTracer = NOOP_RUNTIME_TRACER
    """Optional recorder for runtime port and transaction events."""

    interceptors: PortInterceptorChain = attrs.field(factory=tuple)
    """Deps-scoped port interceptors applied to every resolved configurable port."""

    otel_port_tracer: Any = None
    """When set, each resolved configurable port emits a per-call OpenTelemetry client span through
    this tracer (an OTel ``Tracer``); ``None`` leaves ports bare (zero cost)."""

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
        key: str | None = None,
        outcome: str | None = None,
        error: str | None = None,
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
            key=key,
            outcome=outcome,
            error=error,
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
        ctx: "ExecutionContext",
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

        Wrapping order: the resilience port-policy proxy (when a
        :class:`~forze.application.contracts.resilience.PortPolicy` targets ``key``)
        wraps **outside** the runtime-tracing proxy, and the fully wrapped instance
        is what gets cached.
        """

        from ..interception import current_interceptors

        cache_key = (key, route)
        # Bypass the port cache while a run-scoped (ambient) interceptor chain is bound — the
        # same way resolution tracing bypasses it. A port cached *before* the binding (e.g.
        # DST's cooperative / fault / partition chain) would otherwise be reused bare and skip
        # the chain; re-resolving rewraps each call against the current chain. Production binds
        # no ambient interceptors, so the cache stays fully on (zero cost). Deps-scoped
        # interceptors are fixed at resolve time and stay cached — their proxy reads the ambient
        # chain per call, so only an ambient binding can go stale.
        use_cache = not self.resolution_tracer.enabled and not current_interceptors()

        if use_cache:
            cached = ctx.cached_port(cache_key, spec)

            if cached is not None:
                return cached

        frame = frame_for(key, route)
        token = self._resolution.push(frame)

        try:
            factory = self.store.get_provider(key, route=route)
            result = factory(ctx, spec)
            # Innermost (closest to the real port): interceptor chain, then runtime tracing, then the
            # OTel client span, then the resilience port policy outermost (so a fault interceptor's
            # transient error is retryable by the policy, and a retried call gets one OTel span per
            # attempt while a rejected call emits none).
            port = maybe_wrap_interceptors(self, ctx, key, spec, route, result)
            port = maybe_wrap_configurable(self, ctx, key, spec, route, port)
            port = maybe_wrap_otel_spans(self, ctx, key, spec, route, port)
            port = maybe_wrap_port_policy(self, ctx, key, route, port)

        finally:
            self._resolution.pop(token)

        if use_cache:
            ctx.store_port(cache_key, spec, port)

        return port

    # ....................... #

    def resolve_simple(
        self,
        ctx: "ExecutionContext",
        key: DepKey[Any],
        *,
        route: StrKey | None = None,
    ) -> Any:
        """Resolve a simple dependency: lookup factory and invoke with ``ctx`` only.

        Memoized per scope (keyed by ``(key, route)``) when port caching is enabled,
        mirroring :meth:`resolve_configurable`: a simple dep's factory is a synchronous,
        scope-stable builder (it takes only the scope ``ctx`` and defers per-request reads
        to call time), so it is built once per scope and reused. Caching is bypassed while
        resolution tracing is active so per-task resolution traces stay complete; the
        runtime tracer still records each access.
        """

        cache_key = (key, route)
        use_cache = not self.resolution_tracer.enabled

        if use_cache:
            cached = ctx.cached_simple(cache_key)

            if cached is not None:
                if self.runtime_tracer.enabled:
                    record_simple_resolve(self, ctx, key, route)

                return cached

        frame = frame_for(key, route)
        token = self._resolution.push(frame)

        try:
            factory = self.store.get_provider(key, route=route)
            result = factory(ctx)
            record_simple_resolve(self, ctx, key, route)

        finally:
            self._resolution.pop(token)

        if use_cache:
            ctx.store_simple(cache_key, result)

        return result

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
