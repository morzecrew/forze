"""Internal provider store for plain and routed dependency registration."""

from __future__ import annotations

from typing import Any, Mapping, Self, cast, final

import attrs

from forze.application._logger import logger
from .keys import DepKey
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, StrKeyMapping

from .frame import ResolutionFrame, frame_for

# ----------------------- #

type PlainDepsMap = Mapping[DepKey[Any], Any]
type RoutedDeps = Mapping[DepKey[Any], StrKeyMapping[Any]]

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ProviderStore:
    """Registered dependency providers (internal; no resolution or tracing)."""

    plain_deps: PlainDepsMap = attrs.field(factory=dict[DepKey[Any], Any])
    """Dependencies registered without affinity."""

    routed_deps: RoutedDeps = attrs.field(factory=dict[DepKey[Any], dict[StrKey, Any]])
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
        route: StrKey | None = None,
        fallback_to_plain: bool = True,
        fallback_from_route: StrKey | None = None,
    ) -> T:
        """Look up a registered provider or instance without cycle checks."""

        if route is None:
            if key not in self.plain_deps:
                where = (
                    ""
                    if fallback_from_route is None
                    else f" (fallback from route '{fallback_from_route}')"
                )

                raise exc.configuration(
                    f"Plain dependency '{key.name}' not found{where}; "
                    f"{self._registered_hint(key, None)}. "
                    "Did you forget to register it in a DepsModule?"
                )

            dep = self.plain_deps[key]

        else:
            routes = self.routed_deps.get(key)

            if routes is None:
                if fallback_to_plain:
                    return self.get_provider(
                        key,
                        route=None,
                        fallback_to_plain=False,
                        fallback_from_route=route,
                    )

                raise exc.configuration(
                    f"Routed dependency '{key.name}' not found for route '{route}'; "
                    f"{self._registered_hint(key, route)}. "
                    "Did you forget a DepsModule entry for this route?"
                )

            dep = routes.get(route)

            if dep is None:
                if fallback_to_plain:
                    return self.get_provider(
                        key,
                        route=None,
                        fallback_to_plain=False,
                        fallback_from_route=route,
                    )

                raise exc.configuration(
                    f"Dependency '{key.name}' not found for route '{route}'; "
                    f"{self._registered_hint(key, route)}."
                )

        return cast(T, dep)

    # ....................... #

    def _registered_hint(self, key: DepKey[Any], route: StrKey | None) -> str:
        """A diagnostic naming what *is* registered, to make a missing-dep error actionable.

        A missing dependency is a server-side wiring mistake (a forgotten ``DepsModule``
        entry), so the error is a ``configuration`` error and this detail is logged
        server-side, not exposed to clients.
        """

        if route is None:
            names = sorted(k.name for k in self.plain_deps)
            return f"registered plain dependencies: {', '.join(names) or '<none>'}"

        routes = sorted(str(r) for r in (self.routed_deps.get(key) or {}))
        return f"registered routes for '{key.name}': {', '.join(routes) or '<none>'}"

    # ....................... #

    def exists[T](self, key: DepKey[T], *, route: StrKey | None = None) -> bool:
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

        frames: set[ResolutionFrame] = {frame_for(key, None) for key in self.plain_deps}

        for key, routes in self.routed_deps.items():
            for route in routes:
                frames.add(frame_for(key, route))

        return frozenset(frames)

    # ....................... #

    def empty(self) -> bool:
        """Return ``True`` if the store is empty."""

        return not self.plain_deps and not self.routed_deps

    # ....................... #

    def count(self) -> int:
        """Return total number of registered dependency entries."""

        return len(self.plain_deps) + sum(
            len(routes) for routes in self.routed_deps.values()
        )

    # ....................... #

    @hybridmethod
    def merge(  # type: ignore[misc, override]
        cls: type[ProviderStore],  # type: ignore[misc, override]
        *stores: ProviderStore,
    ) -> ProviderStore:
        """Merge multiple provider stores into one."""

        logger.trace("Merging %s provider store(s)", len(stores))

        plain_acc: PlainDepsMap = {}
        routed_acc: dict[DepKey[Any], dict[StrKey, Any]] = {}

        for store in stores:
            if plain_overlap := set(plain_acc).intersection(store.plain_deps):
                names = ", ".join(sorted(k.name for k in plain_overlap))

                raise exc.internal(f"Conflicting plain dependencies: {names}")

            if cross_overlap_left := set(plain_acc).intersection(store.routed_deps):
                names = ", ".join(sorted(k.name for k in cross_overlap_left))

                raise exc.internal(
                    f"Dependency keys registered both as plain and routed: {names}"
                )

            if cross_overlap_right := set(routed_acc).intersection(store.plain_deps):
                names = ", ".join(sorted(k.name for k in cross_overlap_right))

                raise exc.internal(
                    f"Dependency keys registered both as plain and routed: {names}"
                )

            plain_acc.update(store.plain_deps)  # type: ignore[attr-defined]

            for key, routes in store.routed_deps.items():
                existing = routed_acc.get(key)

                if existing is None:
                    routed_acc[key] = dict(routes)
                    continue

                existing = dict(existing)

                if routing_key_overlap := set(existing).intersection(routes):
                    names = ", ".join(sorted(str(r) for r in routing_key_overlap))

                    raise exc.internal(
                        f"Conflicting routed dependencies for '{key.name}': {names}"
                    )

                existing |= routes
                routed_acc[key] = existing

        return cls(plain_deps=plain_acc, routed_deps=routed_acc)

    # ....................... #

    @merge.instancemethod
    def _merge_instance(  # type: ignore[misc, override]
        self: ProviderStore,
        *stores: ProviderStore,
    ) -> ProviderStore:
        return type(self).merge(self, *stores)

    # ....................... #

    def without[T](self, key: DepKey[T]) -> Self:
        """Return a copy without the given key."""

        logger.trace("Removing dependency '%s' from store copy", key.name)

        new_plain = dict(self.plain_deps or {})
        new_routed = dict(self.routed_deps or {})

        new_plain.pop(key, None)
        new_routed.pop(key, None)

        return type(self)(plain_deps=new_plain, routed_deps=new_routed)

    # ....................... #

    def without_route[T](self, key: DepKey[T], route: StrKey) -> Self:
        """Return a copy without one routed route."""

        logger.trace(
            "Removing dependency '%s' for route '%s' from store copy",
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
