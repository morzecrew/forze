"""Internal provider store for plain and routed dependency registration."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Self, cast, final

import attrs

from forze.application._logger import logger
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, StrKeyMapping

from .fallback import FallbackReport, ShadowedFallback
from .frame import ResolutionFrame, frame_for
from .keys import DepKey

# ----------------------- #

type PlainDepsMap = Mapping[DepKey[Any], Any]
type RoutedDeps = Mapping[DepKey[Any], StrKeyMapping[Any]]
type FallbackRoutes = Mapping[DepKey[Any], frozenset[StrKey]]

# ....................... #


def _snapshot_plain(deps: PlainDepsMap) -> PlainDepsMap:
    """Copy the caller's plain registrations into the store.

    A store is frozen and its fallback marks are captured at construction, so an aliased
    mapping the caller keeps mutating would drift the registrations away from their
    provenance — a key added later would be unmarked, and composing it with a real module
    would report a same-tier conflict instead of the intended fallback.
    """

    return dict(deps)


# ....................... #


def _snapshot_routed(deps: RoutedDeps) -> RoutedDeps:
    """Copy both levels of the caller's routed registrations — see :func:`_snapshot_plain`."""

    return {key: dict(routes) for key, routes in deps.items()}


# ....................... #


def _freeze_keys(keys: Iterable[DepKey[Any]]) -> frozenset[DepKey[Any]]:
    """Normalize a set of marked keys."""

    return frozenset(keys)


# ....................... #


def _freeze_routes(routes: Mapping[DepKey[Any], Iterable[StrKey]]) -> FallbackRoutes:
    """Normalize a per-key route marker map to frozensets."""

    return {key: frozenset(marked) for key, marked in routes.items() if marked}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _Entry:
    """One registration of a key (or key + route) with its provenance."""

    dep: Any
    fallback: bool
    origin: int
    """Index of the store this registration came from. Overlap *within* one store is not a
    composition question — a store may register a key both plain and routed deliberately,
    and resolution prefers the route with the plain entry as its catch-all."""


# ....................... #


def _has_tier_conflict(entries: Sequence[_Entry]) -> bool:
    """Whether two registrations of the *same* provenance claim one slot.

    Same-tier overlap is the wiring bug the merge guards exist for: two real modules
    registering one key, or two fallback environments in one context. Cross-tier overlap
    is the case this store resolves instead of raising.
    """

    real = sum(1 for entry in entries if not entry.fallback)

    return real > 1 or (len(entries) - real) > 1


# ....................... #


def _winner(entries: Sequence[_Entry]) -> _Entry:
    """Pick the surviving registration: a non-fallback one outranks fallbacks."""

    for entry in entries:
        if not entry.fallback:
            return entry

    return entries[0]


# ....................... #


def _reject_plain_conflicts(plain_entries: Mapping[DepKey[Any], Sequence[_Entry]]) -> None:
    """Raise when two same-provenance registrations claim one plain key."""

    conflicting = sorted(
        key.name for key, entries in plain_entries.items() if _has_tier_conflict(entries)
    )

    if conflicting:
        raise exc.internal(f"Conflicting plain dependencies: {', '.join(conflicting)}")


# ....................... #


def _reject_cross_conflicts(
    plain_entries: Mapping[DepKey[Any], Sequence[_Entry]],
    routed_entries: Mapping[DepKey[Any], Mapping[StrKey, Sequence[_Entry]]],
) -> Mapping[DepKey[Any], frozenset[StrKey]]:
    """Resolve keys two *different* stores registered plain on one side and routed on the
    other; return the routes that lose to a real plain registration.

    Same provenance on both sides stays an error (the ambiguity guard that has always
    made a plain/routed collision fail loud). Otherwise the real side wins: a real plain
    catch-all displaces fallback routes outright — keeping them would let a fallback
    answer *ahead* of it, since routed lookup runs first — while a fallback plain entry
    coexists with real routes, serving only what the real module does not cover.

    Every cross-store *pair* is judged, not just the two winners: a real registration must
    not disarm the guard between the entries it outranks, the same way it does not for the
    plain-vs-plain and route-vs-route checks. Otherwise a second fallback environment could
    hide behind a real override and be discovered only as wrong behaviour.
    """

    conflicting: set[str] = set()
    displaced: dict[DepKey[Any], set[StrKey]] = {}

    for key, entries in plain_entries.items():
        per_route = routed_entries.get(key)

        if not per_route:
            continue

        for plain in entries:
            for route, route_entries in per_route.items():
                for routed in route_entries:
                    if routed.origin == plain.origin:
                        continue

                    if routed.fallback == plain.fallback:
                        conflicting.add(key.name)

                    elif not plain.fallback:
                        displaced.setdefault(key, set()).add(route)

    if conflicting:
        raise exc.internal(
            f"Dependency keys registered both as plain and routed: {', '.join(sorted(conflicting))}"
        )

    return {key: frozenset(routes) for key, routes in displaced.items()}


# ....................... #


def _reject_route_conflicts(
    routed_entries: Mapping[DepKey[Any], Mapping[StrKey, Sequence[_Entry]]],
) -> None:
    """Raise when two same-provenance registrations claim one ``(key, route)`` slot."""

    for key in sorted(routed_entries, key=lambda dep_key: dep_key.name):
        clashing = sorted(
            str(route)
            for route, entries in routed_entries[key].items()
            if _has_tier_conflict(entries)
        )

        if clashing:
            raise exc.internal(
                f"Conflicting routed dependencies for '{key.name}': {', '.join(clashing)}"
            )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ProviderStore:
    """Registered dependency providers (internal; no resolution or tracing)."""

    plain_deps: PlainDepsMap = attrs.field(
        factory=dict[DepKey[Any], Any],
        converter=_snapshot_plain,
    )
    """Dependencies registered without affinity (snapshotted at construction)."""

    routed_deps: RoutedDeps = attrs.field(
        factory=dict[DepKey[Any], dict[StrKey, Any]],
        converter=_snapshot_routed,
    )
    """Dependencies registered for specific affinity groups (snapshotted, both levels)."""

    fallback_plain: frozenset[DepKey[Any]] = attrs.field(
        factory=frozenset,
        converter=_freeze_keys,
    )
    """Which :attr:`plain_deps` keys are *fallback* registrations — a background
    environment (the mock) that yields to any real registration of the same key at merge
    instead of colliding with it. Default-empty: a store nobody marked behaves exactly as
    before, and production wiring never marks anything."""

    fallback_routes: FallbackRoutes = attrs.field(
        factory=dict[DepKey[Any], frozenset[StrKey]],
        converter=_freeze_routes,
    )
    """Which :attr:`routed_deps` routes are fallback registrations, per key."""

    shadowed_fallbacks: tuple[ShadowedFallback, ...] = attrs.field(default=(), eq=False)
    """Merge provenance (not registration): fallback entries a real registration
    displaced. Carried so :meth:`fallback_report` can name them at freeze — a hybrid
    wiring stays observable rather than ambient."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        for key, routes in (self.routed_deps or {}).items():
            if not routes:
                raise exc.configuration(f"Routed dependency {key.name} has no routes")

        if unknown := self.fallback_plain.difference(self.plain_deps or {}):
            names = ", ".join(sorted(key.name for key in unknown))

            raise exc.configuration(f"Fallback marks name unregistered plain dependencies: {names}")

        for key, marked in (self.fallback_routes or {}).items():
            registered = (self.routed_deps or {}).get(key) or {}

            if stray := marked.difference(registered):
                stray_names = ", ".join(sorted(str(route) for route in stray))

                raise exc.configuration(
                    f"Fallback marks name unregistered routes for '{key.name}': {stray_names}"
                )

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

        return len(self.plain_deps) + sum(len(routes) for routes in self.routed_deps.values())

    # ....................... #

    def _has_real_route(self, key: DepKey[Any]) -> bool:
        """Whether any route registered under *key* is a non-fallback registration."""

        routes = self.routed_deps.get(key) or {}

        return bool(routes.keys() - (self.fallback_routes.get(key) or frozenset()))

    # ....................... #

    def fallback_report(self) -> FallbackReport:
        """Describe what this store owes to fallback registrations.

        The visibility half of hybrid wiring: which fallback entries a real module took
        over, and which ones still answer calls. See
        :class:`~forze.application.contracts.deps.FallbackReport`.
        """

        marked_routes = sum(len(routes) for routes in self.fallback_routes.values())
        total_routes = sum(len(routes) for routes in self.routed_deps.values())
        fallback_count = len(self.fallback_plain) + marked_routes
        real_count = (len(self.plain_deps) - len(self.fallback_plain)) + (
            total_routes - marked_routes
        )

        return FallbackReport(
            shadowed=self.shadowed_fallbacks,
            served_plain=self.fallback_plain,
            served_routes=dict(self.fallback_routes),
            # A fallback plain entry under a key a real module also routes is the one that
            # can absorb a mistyped route — the real routes answer, and everything else
            # quietly does not fail. Naming it separately is what makes the report a
            # mitigation rather than a list. Routes that are themselves fallback do not
            # count: that key is simply an unwired plane, not a half-real one.
            catch_all=frozenset(key for key in self.fallback_plain if self._has_real_route(key)),
            mixed=bool(fallback_count) and bool(real_count),
        )

    # ....................... #

    @hybridmethod
    def merge(  # type: ignore[misc, override]
        cls: type[ProviderStore],  # type: ignore[misc, override]
        *stores: ProviderStore,
    ) -> ProviderStore:
        """Merge multiple provider stores into one.

        Overlap between two registrations of the same provenance raises exactly as it
        always has — production wiring keeps its fail-loud guarantee, and two fallback
        environments in one context is still a bug. Overlap between a fallback and a real
        registration resolves in favor of the real one (see :attr:`fallback_plain`), which
        is what lets one context combine a mock with real backend modules.

        Provenance, not argument position, decides: merging the same stores in any order
        yields the same result, and the same errors.
        """

        logger.trace("Merging %s provider store(s)", len(stores))

        plain_entries: dict[DepKey[Any], list[_Entry]] = {}
        routed_entries: dict[DepKey[Any], dict[StrKey, list[_Entry]]] = {}
        shadowed: dict[ShadowedFallback, None] = {}

        for origin, store in enumerate(stores):
            shadowed.update(dict.fromkeys(store.shadowed_fallbacks))

            for key, dep in store.plain_deps.items():
                plain_entries.setdefault(key, []).append(
                    _Entry(dep=dep, fallback=key in store.fallback_plain, origin=origin)
                )

            for key, routes in store.routed_deps.items():
                marked = store.fallback_routes.get(key) or frozenset()
                per_route = routed_entries.setdefault(key, {})

                for route, dep in routes.items():
                    per_route.setdefault(route, []).append(
                        _Entry(dep=dep, fallback=route in marked, origin=origin)
                    )

        _reject_plain_conflicts(plain_entries)
        displaced_routes = _reject_cross_conflicts(plain_entries, routed_entries)
        _reject_route_conflicts(routed_entries)

        plain_acc: dict[DepKey[Any], Any] = {}
        fallback_plain: set[DepKey[Any]] = set()

        for key, entries in plain_entries.items():
            winner = _winner(entries)
            plain_acc[key] = winner.dep

            if winner.fallback:
                fallback_plain.add(key)

            elif len(entries) > 1:
                shadowed[ShadowedFallback(key=key)] = None

        routed_acc: dict[DepKey[Any], dict[StrKey, Any]] = {}
        fallback_routes: dict[DepKey[Any], frozenset[StrKey]] = {}

        for key, per_route in routed_entries.items():
            displaced = displaced_routes.get(key) or frozenset()
            routes_acc: dict[StrKey, Any] = {}
            marked_acc: set[StrKey] = set()

            for route, entries in per_route.items():
                if route in displaced:
                    shadowed[ShadowedFallback(key=key, route=route)] = None
                    continue

                winner = _winner(entries)
                routes_acc[route] = winner.dep

                if winner.fallback:
                    marked_acc.add(route)

                elif len(entries) > 1:
                    shadowed[ShadowedFallback(key=key, route=route)] = None

            if not routes_acc:
                continue

            routed_acc[key] = routes_acc

            if marked_acc:
                fallback_routes[key] = frozenset(marked_acc)

        return cls(
            plain_deps=plain_acc,
            routed_deps=routed_acc,
            fallback_plain=frozenset(fallback_plain),
            fallback_routes=fallback_routes,
            shadowed_fallbacks=tuple(
                sorted(shadowed, key=lambda entry: (entry.key.name, str(entry.route or "")))
            ),
        )

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

        return type(self)(
            plain_deps=new_plain,
            routed_deps=new_routed,
            fallback_plain=self.fallback_plain - {key},
            fallback_routes={k: v for k, v in self.fallback_routes.items() if k != key},
            # A shadow record describes a slot that still exists; once the key is gone,
            # keeping it would report a hybrid the store no longer has.
            shadowed_fallbacks=tuple(
                entry for entry in self.shadowed_fallbacks if entry.key != key
            ),
        )

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

        new_marked = dict(self.fallback_routes)
        remaining = (new_marked.pop(key, frozenset())) - {route}

        if routes:
            new_routed[key] = routes

            if remaining:
                new_marked[key] = remaining

        else:
            new_routed.pop(key)

        return type(self)(
            plain_deps=dict(self.plain_deps),
            routed_deps=new_routed,
            fallback_plain=self.fallback_plain,
            fallback_routes=new_marked,
            # Drop the shadow record for the route that just went away — see :meth:`without`.
            shadowed_fallbacks=tuple(
                entry
                for entry in self.shadowed_fallbacks
                if not (entry.key == key and entry.route == route)
            ),
        )
