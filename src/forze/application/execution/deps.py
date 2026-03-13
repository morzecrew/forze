"""Dependency injection container and plans.

Provides :class:`Deps` (in-memory container implementing :class:`DepsPort`),
:class:`DepsModule` protocol, and :class:`DepsPlan` for declarative assembly.
"""

from typing import Any, Protocol, Self, TypeVar, cast, final

import attrs

from forze.base.errors import CoreError
from forze.base.logging import getLogger

from ..contracts.deps import DepKey, DepsPort

# ----------------------- #

logger = getLogger(__name__).bind(scope="deps")

# ....................... #

T = TypeVar("T")

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class Deps(DepsPort):
    """In-memory dependency container used by the kernel."""

    deps: dict[DepKey[Any], Any] = attrs.field(factory=dict)
    """Dependencies by key (type-parameterized)."""

    # ....................... #

    def provide(self, key: DepKey[T]) -> T:
        """Return a dependency value for the given key.

        :param key: Dependency key identifying the provider.
        :returns: Cached or newly constructed instance of the dependency.
        :raises CoreError: If the dependency is not registered.
        """

        dep = self.deps.get(key)

        if not dep:
            raise CoreError(f"Dependency {key.name} not found")

        return cast(T, dep)

    # ....................... #

    def exists(self, key: DepKey[T]) -> bool:
        """Return ``True`` if the dependency is registered."""

        return key in self.deps

    # ....................... #

    @classmethod
    def merge(cls, *deps: Self) -> Self:
        """Merge multiple dependency containers into a single container.

        :param deps: Containers to merge.
        :returns: New container with all dependencies.
        :raises CoreError: If any key is registered in more than one container.
        """

        logger.trace("Merging %d dependency container(s)", len(deps))

        acc: dict[DepKey[Any], Any] = {}

        for d in deps:
            overlap = set(acc.keys()).intersection(d.deps.keys())

            if overlap:
                names = ", ".join(k.name for k in overlap)
                raise CoreError(f"Conflicting dependencies: {names}")

            acc.update(d.deps)

        return cls(deps=acc)

    # ....................... #

    def without(self, key: DepKey[T]) -> Self:
        """Create a new dependency container without the given key.

        :param key: Key to remove.
        :returns: New container without the key.
        """

        logger.trace("Removing dependency %s from container copy", key.name)

        new = dict(self.deps)
        new.pop(key)

        return type(self)(deps=new)

    # ....................... #

    def empty(self) -> bool:
        """Return ``True`` if the dependency container is empty."""

        return len(self.deps) == 0


# ....................... #


class DepsModule(Protocol):
    """Protocol for a module that returns a dependency container.

    Callables are invoked to produce a :class:`Deps` instance; multiple
    modules are merged via :meth:`Deps.merge` when building a plan.
    """

    def __call__(self) -> Deps:
        """Return a dependency container."""
        ...


# ....................... #
#! It's not really a plan and basically just defers 'merge' call


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DepsPlan:
    """Declarative plan for building dependency containers.

    Collects :class:`DepsModule` callables and merges them into a single
    :class:`Deps` instance on :meth:`build`. Merging fails if any module
    registers a conflicting dependency key.
    """

    modules: tuple[DepsModule, ...] = attrs.field(factory=tuple)
    """Modules to invoke and merge when building."""

    # ....................... #

    @classmethod
    def from_modules(cls, *modules: DepsModule) -> Self:
        """Create a plan from modules.

        :param modules: Modules to include.
        :returns: New plan instance.
        """

        return cls(modules=modules)

    # ....................... #

    def with_modules(self, *modules: DepsModule) -> Self:
        """Return a new plan with additional modules appended.

        :param modules: Modules to append.
        :returns: New plan instance.
        """

        logger.trace(
            "Appending %d module(s) to deps plan with %d existing module(s)",
            len(modules),
            len(self.modules),
        )

        return attrs.evolve(self, modules=(*self.modules, *modules))

    # ....................... #

    def build(self) -> Deps:
        """Build a merged dependency container from all modules.

        :returns: Merged :class:`Deps` instance.
        :raises CoreError: If any module registers a conflicting key.
        """

        logger.trace(
            "Building dependency container from %d module(s)",
            len(self.modules),
        )

        if not self.modules:
            logger.trace("Deps plan is empty; returning empty container")
            return Deps()

        built: list[Deps] = []

        for i, module in enumerate(self.modules, 1):
            deps = module()
            logger.trace(
                "Built deps module #%d with %d dependency(ies)",
                i,
                len(deps.deps),
            )
            built.append(deps)

        merged = Deps.merge(*built)

        return merged
