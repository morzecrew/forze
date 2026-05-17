from typing import Any, Mapping, Self, cast, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.base import DepKey
from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError
from forze.base.primitives import StrKey

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
    """

    plain_deps: PlainDepsMap | None = attrs.field(default=None)
    """Dependencies registered without affinity."""

    routed_deps: RoutedDeps[K] | None = attrs.field(default=None)
    """Dependencies registered for specific affinity groups."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # validate routed deps
        for key, routes in (self.routed_deps or {}).items():
            if not routes:
                raise CoreError(f"Routed dependency {key.name} has no routes")

    # ....................... #

    @classmethod
    def plain(cls, deps: PlainDepsMap) -> Self:
        """Create a new dependency container from plain dependencies."""

        return cls(plain_deps=deps or None)

    # ....................... #

    @classmethod
    def routed(cls, deps: RoutedDeps[K]) -> Self:
        """Create a new dependency container from routed dependencies."""

        return cls(routed_deps=deps or None)

    # ....................... #

    @classmethod
    def routed_group(
        cls,
        deps: PlainDepsMap,
        *,
        routes: set[K] | frozenset[K],
    ) -> Self:
        """Create routed dependencies by expanding one provider per many routing keys.

        This is a convenience helper only. Internally routed dependencies are
        always normalized to ``DepKey -> {route -> provider}``.
        """

        if not routes:
            raise CoreError("Routes must not be empty")

        expanded: RoutedDeps[K] = {
            key: {name: dep for name in routes} for key, dep in deps.items()
        }

        return cls(routed_deps=expanded or None)

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
        :returns: Cached or newly constructed instance of the dependency.
        :raises CoreError: If the dependency is not registered.
        """

        if route is None:
            dep = (self.plain_deps or {}).get(key)

            if not dep:
                raise CoreError(f"Plain dependency '{key.name}' not found")

        else:
            routes = (self.routed_deps or {}).get(key)

            if routes is None:
                if fallback_to_plain:
                    return self.provide(key, route=None, fallback_to_plain=False)

                raise CoreError(
                    f"Routed dependency '{key.name}' not found for route '{route}'"
                )

            dep = routes.get(route)

            if dep is None:
                if fallback_to_plain:
                    return self.provide(key, route=None, fallback_to_plain=False)

                raise CoreError(
                    f"Dependency '{key.name}' not found for route '{route}'"
                )

        return cast(T, dep)

    # ....................... #

    def exists[T](self, key: DepKey[T], *, route: K | None = None) -> bool:
        """Return ``True`` if the dependency is registered."""

        if route is None:
            return key in (self.plain_deps or {})

        routes = (self.routed_deps or {}).get(key)

        if routes is None:
            return False

        return route in routes

    # ....................... #

    @hybridmethod
    def merge(cls: type[Self], *deps: Self) -> Self:  # type: ignore[misc, override]
        """Merge multiple dependency containers into a single container.

        :param deps: Containers to merge.
        :returns: New container with all dependencies.
        :raises CoreError: If any key is registered in more than one container.
        """

        logger.trace("Merging %s dependency container(s)", len(deps))

        plain_acc: dict[DepKey[Any], Any] = {}
        routed_acc: dict[DepKey[Any], dict[K, Any]] = {}

        for d in deps:
            # 1. merge plain
            plain_overlap = set(plain_acc).intersection(d.plain_deps or {})

            if plain_overlap:
                names = ", ".join(sorted(k.name for k in plain_overlap))
                raise CoreError(f"Conflicting plain dependencies: {names}")

            # 2. plain vs routed conflicts
            cross_overlap_left = set(plain_acc).intersection(d.routed_deps or {})

            if cross_overlap_left:
                names = ", ".join(sorted(k.name for k in cross_overlap_left))
                raise CoreError(
                    f"Dependency keys registered both as plain and routed: {names}"
                )

            cross_overlap_right = set(routed_acc).intersection(d.plain_deps or {})

            if cross_overlap_right:
                names = ", ".join(sorted(k.name for k in cross_overlap_right))
                raise CoreError(
                    f"Dependency keys registered both as plain and routed: {names}"
                )

            plain_acc.update(d.plain_deps or {})

            # 3. merge routed
            for key, routes in (d.routed_deps or {}).items():
                existing = routed_acc.get(key)

                if existing is None:
                    routed_acc[key] = dict(routes)
                    continue

                existing = dict(existing)
                routing_key_overlap = set(existing).intersection(routes)

                if routing_key_overlap:
                    names = ", ".join(sorted(routing_key_overlap))
                    raise CoreError(
                        f"Conflicting routed dependencies for '{key.name}': {names}"
                    )

                existing.update(routes)
                routed_acc[key] = existing

        return cls(plain_deps=plain_acc or None, routed_deps=routed_acc or None)

    # ....................... #

    @merge.instancemethod
    def _merge_instance(self: Self, *deps: Self) -> Self:  # type: ignore[misc, override]
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

        return type(self)(plain_deps=new_plain or None, routed_deps=new_routed or None)

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
            plain_deps=dict(self.plain_deps or {}) or None,
            routed_deps=new_routed or None,
        )

    # ....................... #

    def empty(self) -> bool:
        """Return ``True`` if the dependency container is empty."""

        return not (self.plain_deps or {}) and not (self.routed_deps or {})

    # ....................... #

    def count(self) -> int:
        """Return total number of registered dependency entries.

        Plain deps count as 1 entry each.
        Routed deps count as 1 entry per route.
        """

        return len(self.plain_deps or {}) + sum(
            len(routes) for routes in (self.routed_deps or {}).values()
        )
