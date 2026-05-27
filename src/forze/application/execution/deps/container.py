from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator, Self, cast, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.base import BaseSpec
from forze.application.contracts.deps import DepKey
from forze.application.execution.tracing import RuntimeTrace
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .port_instrumentation import maybe_wrap_configurable, record_simple_resolve
from .registry import DepsRegistry, PlainDepsMap, RoutedDeps
from .resolution import ResolutionFrame, frame_for
from .resolution_context import ResolutionContext
from .resolution_tracer import (
    NOOP_RESOLUTION_TRACER,
    ResolutionTracer,
    resolution_tracer_from_flag,
)
from .runtime_tracer import NOOP_RUNTIME_TRACER, RuntimeTracer, runtime_tracer_from_flag
from .trace import DepsResolutionTrace

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Deps[K: StrKey]:
    """In-memory dependency container used by the kernel.

    Composes a static :class:`DepsRegistry`, a per-task :class:`ResolutionContext`
    for cycle detection, and optional resolution/runtime tracers.
    """

    registry: DepsRegistry[K] = attrs.field(factory=DepsRegistry)
    """Registered dependency providers."""

    resolution_tracer: ResolutionTracer = attrs.field(default=NOOP_RESOLUTION_TRACER)
    """Optional recorder for observed resolution edges."""

    runtime_tracer: RuntimeTracer = attrs.field(default=NOOP_RUNTIME_TRACER)
    """Optional recorder for runtime port and transaction events."""

    _resolution: ResolutionContext = attrs.field(
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )

    # ....................... #

    @property
    def plain_deps(self) -> PlainDepsMap:
        """Registered plain dependencies (read-only view)."""

        return self.registry.plain_deps

    @property
    def routed_deps(self) -> RoutedDeps[K]:
        """Registered routed dependencies (read-only view)."""

        return self.registry.routed_deps

    @property
    def trace_resolution(self) -> bool:
        """Whether resolution edge recording is enabled (compat shim)."""

        return self.resolution_tracer.enabled

    @property
    def trace_runtime(self) -> bool:
        """Whether runtime event recording is enabled (compat shim)."""

        return self.runtime_tracer.enabled

    # ....................... #

    def __attrs_post_init__(self) -> None:
        object.__setattr__(
            self,
            "_resolution",
            ResolutionContext(self.resolution_tracer),
        )

    # ....................... #

    @classmethod
    def plain(
        cls,
        deps: PlainDepsMap,
        *,
        trace_resolution: bool = False,
        trace_runtime: bool = False,
        resolution_tracer: ResolutionTracer | None = None,
        runtime_tracer: RuntimeTracer | None = None,
    ) -> Deps[Any]:
        """Create a new dependency container from plain dependencies."""

        return cls(
            registry=DepsRegistry(plain_deps=deps),
            resolution_tracer=(
                resolution_tracer
                if resolution_tracer is not None
                else resolution_tracer_from_flag(trace_resolution)
            ),
            runtime_tracer=(
                runtime_tracer
                if runtime_tracer is not None
                else runtime_tracer_from_flag(trace_runtime)
            ),
        )

    # ....................... #

    @classmethod
    def routed[X: StrKey](
        cls,
        deps: RoutedDeps[X],
        *,
        trace_resolution: bool = False,
        resolution_tracer: ResolutionTracer | None = None,
    ) -> Deps[X]:
        """Create a new dependency container from routed dependencies."""

        if resolution_tracer is None:
            resolution_tracer = resolution_tracer_from_flag(trace_resolution)

        return cast(type[Deps[X]], cls)(
            registry=DepsRegistry(routed_deps=deps),
            resolution_tracer=resolution_tracer,
        )

    # ....................... #

    @classmethod
    def routed_group[X: StrKey](
        cls,
        deps: PlainDepsMap,
        *,
        routes: set[X] | frozenset[X],
        trace_resolution: bool = False,
        resolution_tracer: ResolutionTracer | None = None,
    ) -> Deps[X]:
        """Create routed dependencies by expanding one provider per many routing keys."""

        if not routes:
            raise exc.precondition("Routes must not be empty")

        expanded: RoutedDeps[X] = {
            key: {name: dep for name in routes} for key, dep in deps.items()
        }

        if resolution_tracer is None:
            resolution_tracer = resolution_tracer_from_flag(trace_resolution)

        return cast(type[Deps[X]], cls)(
            registry=DepsRegistry(routed_deps=expanded),
            resolution_tracer=resolution_tracer,
        )

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

    def get_provider[T](
        self,
        key: DepKey[T],
        *,
        route: K | None = None,
        fallback_to_plain: bool = True,
    ) -> T:
        """Look up a registered provider without cycle checks."""

        return self.registry.get_provider(
            key,
            route=route,
            fallback_to_plain=fallback_to_plain,
        )

    # ....................... #

    def provide[T](
        self,
        key: DepKey[T],
        *,
        route: K | None = None,
        fallback_to_plain: bool = True,
    ) -> T:
        """Return a dependency value for the given key."""

        frame = frame_for(key, route)
        self._resolution.assert_not_active(frame)
        self._resolution.record_provide_edge(frame)

        return self.registry.get_provider(
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
        route: K | None = None,
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
        route: K | None = None,
    ) -> Any:
        """Resolve a configurable dependency: lookup factory and invoke with ``spec``."""

        frame = frame_for(key, route)
        token = self._resolution.push(frame)

        try:
            factory = self.registry.get_provider(key, route=route)
            result = factory(ctx, spec)
            return maybe_wrap_configurable(self, ctx, key, spec, route, result)

        finally:
            self._resolution.pop(token)

    # ....................... #

    def resolve_simple(
        self,
        ctx: ExecutionContext,
        key: DepKey[Any],
        *,
        route: K | None = None,
    ) -> Any:
        """Resolve a simple dependency: lookup factory and invoke with ``ctx`` only."""

        frame = frame_for(key, route)
        token = self._resolution.push(frame)

        try:
            factory = self.registry.get_provider(key, route=route)
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

        return self.registry.registered_frames()

    # ....................... #

    def exists[T](self, key: DepKey[T], *, route: K | None = None) -> bool:
        """Return ``True`` if the dependency is registered."""

        return self.registry.exists(key, route=route)

    # ....................... #

    @hybridmethod
    def merge[X: StrKey](  # type: ignore[misc, override]
        cls: type[Deps[X]],  # type: ignore[misc, override]
        *deps: Deps[X],
        resolution_tracer: ResolutionTracer | None = None,
        runtime_tracer: RuntimeTracer | None = None,
    ) -> Deps[X]:
        """Merge multiple dependency containers into a single container."""

        logger.trace("Merging %s dependency container(s)", len(deps))

        merged_registry = DepsRegistry.merge(*(d.registry for d in deps))

        return cls(
            registry=merged_registry,
            resolution_tracer=resolution_tracer or NOOP_RESOLUTION_TRACER,
            runtime_tracer=runtime_tracer or NOOP_RUNTIME_TRACER,
        )

    # ....................... #

    @merge.instancemethod
    def _merge_instance[X: StrKey](  # type: ignore[misc, override]
        self: Deps[X],
        *deps: Deps[X],
        resolution_tracer: ResolutionTracer | None = None,
        runtime_tracer: RuntimeTracer | None = None,
    ) -> Deps[X]:
        """Merge this dependency container with other containers."""

        return type(self).merge(
            self,
            *deps,
            resolution_tracer=resolution_tracer,
            runtime_tracer=runtime_tracer,
        )

    # ....................... #

    def without[T](self, key: DepKey[T]) -> Self:
        """Create a new dependency container without the given key."""

        return type(self)(
            registry=self.registry.without(key),
            resolution_tracer=self.resolution_tracer,
            runtime_tracer=self.runtime_tracer,
        )

    # ....................... #

    def without_route[T](self, key: DepKey[T], route: K) -> Self:
        """Create a new dependency container without one routed route."""

        return type(self)(
            registry=self.registry.without_route(key, route),
            resolution_tracer=self.resolution_tracer,
            runtime_tracer=self.runtime_tracer,
        )

    # ....................... #

    def empty(self) -> bool:
        """Return ``True`` if the dependency container is empty."""

        return self.registry.empty()

    # ....................... #

    def count(self) -> int:
        """Return total number of registered dependency entries."""

        return self.registry.count()
