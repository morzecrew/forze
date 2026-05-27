"""Static dependency registry (plain and routed providers)."""

from __future__ import annotations

from typing import Any, Mapping, Self, cast, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.deps import DepKey
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .resolution import ResolutionFrame, frame_for

# ----------------------- #

type PlainDepsMap = Mapping[DepKey[Any], Any]
type RoutedDeps[K] = Mapping[DepKey[Any], Mapping[K, Any]]

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DepsRegistry[K: StrKey]:
    """Registered dependency providers without resolution or tracing."""

    plain_deps: PlainDepsMap = attrs.field(factory=dict[DepKey[Any], Any])
    """Dependencies registered without affinity."""

    routed_deps: RoutedDeps[K] = attrs.field(factory=dict[DepKey[Any], dict[K, Any]])
    """Dependencies registered for specific affinity groups."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        for key, routes in (self.routed_deps or {}).items():
            if not routes:
                raise exc.configuration(f"Routed dependency {key.name} has no routes")

    # ....................... #

    def get_provider[T](
        self,
        key: DepKey[T],
        *,
        route: K | None = None,
        fallback_to_plain: bool = True,
    ) -> T:
        """Look up a registered provider or instance without cycle checks."""

        if route is None:
            dep = self.plain_deps.get(key)

            if not dep:
                raise exc.internal(f"Plain dependency '{key.name}' not found")

        else:
            routes = self.routed_deps.get(key)

            if routes is None:
                if fallback_to_plain:
                    return self.get_provider(key, route=None, fallback_to_plain=False)

                raise exc.internal(
                    f"Routed dependency '{key.name}' not found for route '{route}'"
                )

            dep = routes.get(route)

            if dep is None:
                if fallback_to_plain:
                    return self.get_provider(key, route=None, fallback_to_plain=False)

                raise exc.internal(
                    f"Dependency '{key.name}' not found for route '{route}'"
                )

        return cast(T, dep)

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

    def empty(self) -> bool:
        """Return ``True`` if the registry is empty."""

        return not self.plain_deps and not self.routed_deps

    # ....................... #

    def count(self) -> int:
        """Return total number of registered dependency entries."""

        return len(self.plain_deps) + sum(
            len(routes) for routes in self.routed_deps.values()
        )

    # ....................... #

    @hybridmethod
    def merge[X: StrKey](cls: type[DepsRegistry[X]], *registries: DepsRegistry[X]) -> DepsRegistry[X]:  # type: ignore[misc, override]
        """Merge multiple registries into one."""

        logger.trace("Merging %s dependency registry(ies)", len(registries))

        plain_acc: PlainDepsMap = {}
        routed_acc: RoutedDeps[X] = {}

        for reg in registries:
            plain_overlap = set(plain_acc).intersection(reg.plain_deps)

            if plain_overlap:
                names = ", ".join(sorted(k.name for k in plain_overlap))

                raise exc.internal(f"Conflicting plain dependencies: {names}")

            cross_overlap_left = set(plain_acc).intersection(reg.routed_deps)

            if cross_overlap_left:
                names = ", ".join(sorted(k.name for k in cross_overlap_left))

                raise exc.internal(
                    f"Dependency keys registered both as plain and routed: {names}"
                )

            cross_overlap_right = set(routed_acc).intersection(reg.plain_deps)

            if cross_overlap_right:
                names = ", ".join(sorted(k.name for k in cross_overlap_right))

                raise exc.internal(
                    f"Dependency keys registered both as plain and routed: {names}"
                )

            plain_acc.update(reg.plain_deps)  # type: ignore[attr-defined]

            for key, routes in reg.routed_deps.items():
                existing = routed_acc.get(key)

                if existing is None:
                    routed_acc[key] = dict(routes)  # type: ignore[index]
                    continue

                existing = dict(existing)
                routing_key_overlap = set(existing).intersection(routes)

                if routing_key_overlap:
                    names = ", ".join(sorted(routing_key_overlap))

                    raise exc.internal(
                        f"Conflicting routed dependencies for '{key.name}': {names}"
                    )

                existing.update(routes)
                routed_acc[key] = existing  # type: ignore[index]

        return cls(plain_deps=plain_acc, routed_deps=routed_acc)

    # ....................... #

    @merge.instancemethod
    def _merge_instance[X: StrKey](self: DepsRegistry[X], *registries: DepsRegistry[X]) -> DepsRegistry[X]:  # type: ignore[misc, override]
        return type(self).merge(self, *registries)

    # ....................... #

    def without[T](self, key: DepKey[T]) -> Self:
        """Return a copy without the given key."""

        logger.trace("Removing dependency '%s' from registry copy", key.name)

        new_plain = dict(self.plain_deps or {})
        new_routed = dict(self.routed_deps or {})

        new_plain.pop(key, None)
        new_routed.pop(key, None)

        return type(self)(plain_deps=new_plain, routed_deps=new_routed)

    # ....................... #

    def without_route[T](self, key: DepKey[T], route: K) -> Self:
        """Return a copy without one routed route."""

        logger.trace(
            "Removing dependency '%s' for route '%s' from registry copy",
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

        return type(self)(plain_deps=dict(self.plain_deps), routed_deps=new_routed)
