from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Any, Iterator, Mapping, Self, cast, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.base import DepKey
from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError
from forze.base.primitives import StrKey

from forze.application.execution.tracing.buffer import RuntimeTrace

from .resolution import ResolutionFrame, format_cycle_error, frame_for
from .trace import DepsResolutionTrace

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

type PlainDepsMap = Mapping[DepKey[Any], Any]
type RoutedDeps[K] = Mapping[DepKey[Any], Mapping[K, Any]]

# ....................... #
#! Maybe rename Deps -> DepsContainer or so ? To be explicit


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Deps[K: StrKey]:
    """In-memory dependency container used by the kernel.

    Supports two registration modes:

    - plain dependencies: ``DepKey -> provider``
    - routed dependencies: ``DepKey -> {routing_key -> provider}``

    Cyclic resolution is detected via a per-container, per-task resolution stack
    (:class:`contextvars.ContextVar`). Optional :attr:`trace_resolution` records
    observed edges for development diagnostics.
    """

    plain_deps: PlainDepsMap = attrs.field(factory=dict[DepKey[Any], Any])
    """Dependencies registered without affinity."""

    routed_deps: RoutedDeps[K] = attrs.field(factory=dict[DepKey[Any], dict[K, Any]])
    """Dependencies registered for specific affinity groups."""

    trace_resolution: bool = False
    """When ``True``, record observed resolution edges for the current async task."""

    trace_runtime: bool = False
    """When ``True``, record configurable port calls and transaction boundaries."""

    _resolution_stack: ContextVar[tuple[ResolutionFrame, ...]] = attrs.field(
        factory=lambda: ContextVar("deps_resolution_stack", default=()),
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )
    """Per-task resolution stack for this container."""

    _resolution_trace: ContextVar[DepsResolutionTrace | None] = attrs.field(
        factory=lambda: ContextVar("deps_resolution_trace", default=None),
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )
    """Per-task observed resolution graph when :attr:`trace_resolution` is enabled."""

    _runtime_trace: ContextVar[RuntimeTrace | None] = attrs.field(
        factory=lambda: ContextVar("deps_runtime_trace", default=None),
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )
    """Per-task runtime event buffer when :attr:`trace_runtime` is enabled."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        for key, routes in (self.routed_deps or {}).items():
            if not routes:
                raise CoreError(f"Routed dependency {key.name} has no routes")

    # ....................... #

    @classmethod
    def plain(
        cls,
        deps: PlainDepsMap,
        *,
        trace_resolution: bool = False,
        trace_runtime: bool = False,
    ) -> Deps[Any]:
        """Create a new dependency container from plain dependencies."""

        return cls(
            plain_deps=deps,
            trace_resolution=trace_resolution,
            trace_runtime=trace_runtime,
        )

    # ....................... #

    @classmethod
    def routed[X: StrKey](
        cls,
        deps: RoutedDeps[X],
        *,
        trace_resolution: bool = False,
    ) -> Deps[X]:
        """Create a new dependency container from routed dependencies."""

        return cast(type[Deps[X]], cls)(
            routed_deps=deps,
            trace_resolution=trace_resolution,
        )

    # ....................... #

    @classmethod
    def routed_group[X: StrKey](
        cls,
        deps: PlainDepsMap,
        *,
        routes: set[X] | frozenset[X],
        trace_resolution: bool = False,
    ) -> Deps[X]:
        """Create routed dependencies by expanding one provider per many routing keys.

        This is a convenience helper only. Internally routed dependencies are
        always normalized to ``DepKey -> {route -> provider}``.
        """

        if not routes:
            raise CoreError("Routes must not be empty")

        expanded: RoutedDeps[X] = {
            key: {name: dep for name in routes} for key, dep in deps.items()
        }

        return cast(type[Deps[X]], cls)(
            routed_deps=expanded,
            trace_resolution=trace_resolution,
        )

    # ....................... #

    def _resolution_stack_get(self) -> tuple[ResolutionFrame, ...]:
        return self._resolution_stack.get()

    # ....................... #

    def _trace_get_or_create(self) -> DepsResolutionTrace:
        trace = self._resolution_trace.get()

        if trace is None:
            trace = DepsResolutionTrace()
            self._resolution_trace.set(trace)

        return trace

    # ....................... #

    def _runtime_trace_get_or_create(self) -> RuntimeTrace:
        trace = self._runtime_trace.get()

        if trace is None:
            trace = RuntimeTrace()
            self._runtime_trace.set(trace)

        return trace

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
        """Append a runtime tracing event when :attr:`trace_runtime` is enabled."""

        if not self.trace_runtime:
            return

        self._runtime_trace_get_or_create().next_event(
            domain=domain,
            op=op,
            surface=surface,
            route=route,
            phase=phase,
            tx_depth=tx_depth,
            tx_route=tx_route,
        )

    # ....................... #

    def _record_edge(self, parent: ResolutionFrame, child: ResolutionFrame) -> None:
        if not self.trace_resolution:
            return

        self._trace_get_or_create().add_edge(parent, child)

    # ....................... #

    def _push_frame(self, frame: ResolutionFrame) -> Token[tuple[ResolutionFrame, ...]]:
        stack = self._resolution_stack_get()

        if frame in stack:
            raise CoreError(format_cycle_error(stack, frame))

        if stack:
            self._record_edge(stack[-1], frame)

        return self._resolution_stack.set((*stack, frame))

    # ....................... #

    def _pop_frame(self, token: Token[tuple[ResolutionFrame, ...]]) -> None:
        self._resolution_stack.reset(token)

    # ....................... #

    def _assert_frame_not_active(self, frame: ResolutionFrame) -> None:
        stack = self._resolution_stack_get()

        if frame in stack:
            raise CoreError(format_cycle_error(stack, frame))

    # ....................... #

    def _lookup[T](
        self,
        key: DepKey[T],
        *,
        route: K | None = None,
        fallback_to_plain: bool = True,
    ) -> T:
        """Look up a registered dependency without cycle checks."""

        if route is None:
            dep = self.plain_deps.get(key)

            if not dep:
                raise CoreError(f"Plain dependency '{key.name}' not found")

        else:
            routes = self.routed_deps.get(key)

            if routes is None:
                if fallback_to_plain:
                    return self._lookup(key, route=None, fallback_to_plain=False)

                raise CoreError(
                    f"Routed dependency '{key.name}' not found for route '{route}'"
                )

            dep = routes.get(route)

            if dep is None:
                if fallback_to_plain:
                    return self._lookup(key, route=None, fallback_to_plain=False)

                raise CoreError(
                    f"Dependency '{key.name}' not found for route '{route}'"
                )

        return cast(T, dep)

    # ....................... #

    def provide[T](
        self,
        key: DepKey[T],
        *,
        route: K | None = None,
        fallback_to_plain: bool = True,
    ) -> T:
        """Return a dependency value for the given key.

        :param key: Dependency key identifying the provider.
        :param route: Optional route for routed dependencies.
        :param fallback_to_plain: If True, fallback to plain dependencies if the routed dependency is not found.
        :returns: Registered provider or instance for the key.
        :raises CoreError: If the dependency is not registered or resolution would cycle.
        """

        frame = frame_for(key, route)
        self._assert_frame_not_active(frame)

        stack = self._resolution_stack_get()

        if stack:
            self._record_edge(stack[-1], frame)

        return self._lookup(key, route=route, fallback_to_plain=fallback_to_plain)

    # ....................... #

    @contextmanager
    def resolution_scope(
        self,
        key: DepKey[Any],
        *,
        route: K | None = None,
    ) -> Iterator[None]:
        """Enter a resolution scope for ``key`` (and optional ``route``).

        Pushes a frame onto the per-task resolution stack for this container for
        the duration of the block. Use before looking up and invoking a factory
        when the caller owns the invocation (for example transaction manager resolution).
        """

        frame = frame_for(key, route)
        token = self._push_frame(frame)

        try:
            yield

        finally:
            self._pop_frame(token)

    # ....................... #

    def resolve_configurable(
        self,
        ctx: ExecutionContext,
        key: DepKey[Any],
        spec: object,
        *,
        route: K | None = None,
    ) -> Any:
        """Resolve a configurable dependency: lookup factory and invoke with ``spec``."""

        frame = frame_for(key, route)
        token = self._push_frame(frame)

        try:
            factory = self._lookup(key, route=route)
            result = factory(ctx, spec)

            if not self.trace_runtime:
                return result

            from ..tracing.metadata import infer_port_metadata
            from ..tracing.port_proxy import wrap_port

            domain, surface, route_name, phase = infer_port_metadata(
                key,
                spec,
                route=route,
            )
            return wrap_port(
                result,
                deps=self,
                domain=domain,
                surface=surface,
                route=route_name,
                phase=phase,
                tx_depth_getter=ctx.tx.depth,
            )

        finally:
            self._pop_frame(token)

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
        token = self._push_frame(frame)

        try:
            factory = self._lookup(key, route=route)
            return factory(ctx)

        finally:
            self._pop_frame(token)

    # ....................... #

    def resolution_trace(self) -> DepsResolutionTrace | None:
        """Return the observed resolution trace for the current task, if any."""

        if not self.trace_resolution:
            return None

        return self._resolution_trace.get()

    # ....................... #

    def runtime_trace(self) -> RuntimeTrace | None:
        """Return the observed runtime trace for the current task, if any."""

        if not self.trace_runtime:
            return None

        return self._runtime_trace.get()

    # ....................... #

    def registered_frames(self) -> frozenset[ResolutionFrame]:
        """Return all registered dependency frames (static inventory)."""

        frames: set[ResolutionFrame] = set()

        for key in self.plain_deps:
            frames.add(frame_for(key, None))

        for key, routes in self.routed_deps.items():
            for route in routes:
                frames.add(frame_for(key, route))

        return frozenset(frames)

    # ....................... #

    def exists[T](self, key: DepKey[T], *, route: K | None = None) -> bool:
        """Return ``True`` if the dependency is registered."""

        if route is None:
            return key in self.plain_deps

        routes = self.routed_deps.get(key)

        if routes is None:
            return False

        return route in routes

    # ....................... #

    @hybridmethod
    def merge[X: StrKey](cls: type[Deps[X]], *deps: Deps[X]) -> Deps[X]:  # type: ignore[misc, override]
        """Merge multiple dependency containers into a single container.

        :param deps: Containers to merge.
        :returns: New container with all dependencies.
        :raises CoreError: If any key is registered in more than one container.
        """

        logger.trace("Merging %s dependency container(s)", len(deps))

        plain_acc: PlainDepsMap = {}
        routed_acc: RoutedDeps[X] = {}
        trace_resolution = any(d.trace_resolution for d in deps)
        trace_runtime = any(d.trace_runtime for d in deps)

        for d in deps:
            # 1. merge plain
            plain_overlap = set(plain_acc).intersection(d.plain_deps)

            if plain_overlap:
                names = ", ".join(sorted(k.name for k in plain_overlap))
                raise CoreError(f"Conflicting plain dependencies: {names}")

            # 2. plain vs routed conflicts
            cross_overlap_left = set(plain_acc).intersection(d.routed_deps)

            if cross_overlap_left:
                names = ", ".join(sorted(k.name for k in cross_overlap_left))
                raise CoreError(
                    f"Dependency keys registered both as plain and routed: {names}"
                )

            cross_overlap_right = set(routed_acc).intersection(d.plain_deps)

            if cross_overlap_right:
                names = ", ".join(sorted(k.name for k in cross_overlap_right))
                raise CoreError(
                    f"Dependency keys registered both as plain and routed: {names}"
                )

            plain_acc.update(d.plain_deps)  # type: ignore[attr-defined]

            # 3. merge routed
            for key, routes in d.routed_deps.items():
                existing = routed_acc.get(key)

                if existing is None:
                    routed_acc[key] = dict(routes)  # type: ignore[index]
                    continue

                existing = dict(existing)
                routing_key_overlap = set(existing).intersection(routes)

                if routing_key_overlap:
                    names = ", ".join(sorted(routing_key_overlap))
                    raise CoreError(
                        f"Conflicting routed dependencies for '{key.name}': {names}"
                    )

                existing.update(routes)
                routed_acc[key] = existing  # type: ignore[index]

        return cls(
            plain_deps=plain_acc,
            routed_deps=routed_acc,
            trace_resolution=trace_resolution,
            trace_runtime=trace_runtime,
        )

    # ....................... #

    @merge.instancemethod
    def _merge_instance[X: StrKey](self: Deps[X], *deps: Deps[X]) -> Deps[X]:  # type: ignore[misc, override]
        """Merge this dependency container with another containers.

        :param deps: Containers to merge.
        :returns: New container with all dependencies.
        :raises CoreError: If any key is registered in more than one container.
        """

        return type(self).merge(self, *deps)

    # ....................... #

    def without[T](self, key: DepKey[T]) -> Self:
        """Create a new dependency container without the given key.

        :param key: Key to remove.
        :returns: New container without the key.
        """

        logger.trace("Removing dependency '%s' from container copy", key.name)

        new_plain = dict(self.plain_deps or {})
        new_routed = dict(self.routed_deps or {})

        new_plain.pop(key, None)
        new_routed.pop(key, None)

        return type(self)(
            plain_deps=new_plain,
            routed_deps=new_routed,
            trace_resolution=self.trace_resolution,
            trace_runtime=self.trace_runtime,
        )

    # ....................... #

    def without_route[T](self, key: DepKey[T], route: K) -> Self:
        """Create a new dependency container without one routed route."""

        logger.trace(
            "Removing dependency '%s' for route '%s' from container copy",
            key.name,
            route,
        )

        if key not in (self.routed_deps or {}):
            return self

        new_routed = dict(self.routed_deps or {})
        routes = dict(new_routed[key])
        routes.pop(route, None)

        if routes:
            new_routed[key] = routes

        else:
            new_routed.pop(key)

        return type(self)(
            plain_deps=dict(self.plain_deps),
            routed_deps=new_routed,
            trace_resolution=self.trace_resolution,
            trace_runtime=self.trace_runtime,
        )

    # ....................... #

    def empty(self) -> bool:
        """Return ``True`` if the dependency container is empty."""

        return not self.plain_deps and not self.routed_deps

    # ....................... #

    def count(self) -> int:
        """Return total number of registered dependency entries.

        Plain deps count as 1 entry each.
        Routed deps count as 1 entry per route.
        """

        return len(self.plain_deps) + sum(
            len(routes) for routes in self.routed_deps.values()
        )
