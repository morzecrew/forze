from typing import Any, Protocol, Self, TypeVar, cast, final

import attrs

from forze.application.contracts.deps import DepKey, DepsPort
from forze.base.errors import CoreError

# ----------------------- #

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
        """Merge multiple dependency containers into a single container."""

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
        """Create a new dependency container without the given key."""

        new = dict(self.deps)
        new.pop(key)

        return type(self)(deps=new)

    # ....................... #

    def empty(self) -> bool:
        """Return ``True`` if the dependency container is empty."""

        return len(self.deps) == 0


# ....................... #


class DepsModule(Protocol):
    def __call__(self) -> Deps:
        """Return a dependency container."""
        ...


# ....................... #
#! It's not really a plan and basically just defers 'merge' call


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DepsPlan:
    """Declarative plan for building dependency containers."""

    modules: tuple[DepsModule, ...] = attrs.field(factory=tuple)

    # ....................... #

    @classmethod
    def from_modules(cls, *modules: DepsModule) -> Self:
        return cls(modules=modules)

    # ....................... #

    def with_modules(self, *modules: DepsModule) -> Self:
        return attrs.evolve(self, modules=(*self.modules, *modules))

    # ....................... #

    def build(self) -> Deps:
        if not self.modules:
            return Deps()

        return Deps.merge(*(m() for m in self.modules))
