"""Registration-only dependency blobs for modules and plans."""

from __future__ import annotations

from typing import Any, final

import attrs

from forze.application._logger import logger
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .frame import ResolutionFrame
from .keys import DepKey
from .store import PlainDepsMap, ProviderStore, RoutedDeps

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Deps:
    """Registration-only dependency blob.

    Produced by :class:`DepsModule` and merged by :class:`DepsRegistry` at freeze.
    Use :meth:`forze.application.execution.deps.frozen.FrozenDepsRegistry.resolve`
    for runtime resolution.
    """

    store: ProviderStore = attrs.field(factory=ProviderStore)
    """Registered dependency providers."""

    # ....................... #

    @property
    def plain_deps(self) -> PlainDepsMap:
        """Registered plain dependencies (read-only view)."""

        return self.store.plain_deps

    @property
    def routed_deps(self) -> RoutedDeps:
        """Registered routed dependencies (read-only view)."""

        return self.store.routed_deps

    # ....................... #

    @classmethod
    def plain(
        cls,
        deps: PlainDepsMap,
    ) -> Deps:
        """Create a registration blob from plain dependencies."""

        return cls(store=ProviderStore(plain_deps=deps))

    # ....................... #

    @classmethod
    def routed(
        cls,
        deps: RoutedDeps,
    ) -> Deps:
        """Create a registration blob from routed dependencies."""

        return cls(store=ProviderStore(routed_deps=deps))

    # ....................... #

    @classmethod
    def routed_group(
        cls,
        deps: PlainDepsMap,
        *,
        routes: set[StrKey] | frozenset[StrKey],
    ) -> Deps:
        """Create routed dependencies by expanding one provider per many routing keys."""

        if not routes:
            raise exc.precondition("Routes must not be empty")

        expanded: dict[DepKey[Any], dict[StrKey, Any]] = {
            key: dict.fromkeys(routes, dep) for key, dep in deps.items()
        }

        return cls(store=ProviderStore(routed_deps=expanded))

    # ....................... #

    def exists[T](self, key: DepKey[T], *, route: StrKey | None = None) -> bool:
        """Return ``True`` if the dependency is registered."""

        return self.store.exists(key, route=route)

    # ....................... #

    def registered_frames(self) -> frozenset[ResolutionFrame]:
        """Return all registered dependency frames (static inventory)."""

        return self.store.registered_frames()

    # ....................... #

    @hybridmethod
    def merge(cls: type[Deps], *deps: Deps) -> Deps:  # type: ignore[misc, override]
        """Merge multiple registration blobs into one."""

        logger.trace("Merging %s registration deps blob(s)", len(deps))

        merged_store = ProviderStore.merge(*(d.store for d in deps))

        return cls(store=merged_store)

    # ....................... #

    @merge.instancemethod
    def _merge_instance(self: Deps, *deps: Deps) -> Deps:  # type: ignore[misc, override]
        """Merge this registration blob with others."""

        return type(self).merge(self, *deps)

    # ....................... #

    def empty(self) -> bool:
        """Return ``True`` if the registration blob is empty."""

        return self.store.empty()

    # ....................... #

    def count(self) -> int:
        """Return total number of registered dependency entries."""

        return self.store.count()
