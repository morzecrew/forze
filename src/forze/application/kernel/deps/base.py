from enum import StrEnum
from typing import (
    Any,
    Literal,
    Optional,
    Protocol,
    Self,
    TypeVar,
    cast,
    final,
    overload,
)

import attrs

from forze.base.errors import CoreError

# ----------------------- #

T = TypeVar("T")

RoutingKey = str | StrEnum

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class DepKey[T]:
    """Typed key used to identify dependencies in the kernel.

    The ``name`` is only used for diagnostics; type information is carried
    through the type parameter ``T``.
    """

    name: str
    """Name of the dependency."""


# ....................... #


class DepsPort(Protocol):
    """Abstract access to dependency resolution."""

    def provide(self, key: DepKey[T]) -> T:
        """Return the dependency instance registered under ``key``."""
        ...

    def exists(self, key: DepKey[T]) -> bool:
        """Return ``True`` if the dependency is registered."""
        ...


# ....................... #


@final
@attrs.define(slots=True)
class Deps(DepsPort):
    """In-memory dependency container used by the kernel."""

    deps: dict[DepKey[Any], Any] = attrs.field(factory=dict)
    """Dependencies by key (type-parameterized)."""

    # ....................... #

    @overload
    def register(
        self,
        key: DepKey[T],
        dep: T,
        *,
        inplace: Literal[True],
    ) -> None:
        """Register a dependency provider for a given key.

        :param key: Dependency key identifying the dependency.
        :param dep: Dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If the dependency is already registered.
        """
        ...

    @overload
    def register(
        self,
        key: DepKey[T],
        dep: T,
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Register a dependency provider for a given key.

        :param key: Dependency key identifying the dependency.
        :param dep: Dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If the dependency is already registered.
        """
        ...

    def register(
        self,
        key: DepKey[T],
        dep: T,
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        """Register a dependency provider for a given key.

        :param key: Dependency key identifying the dependency.
        :param dep: Dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If the dependency is already registered.
        """

        if key in self.deps:
            raise CoreError(f"Dependency {key.name} already registered")

        new = dict(self.deps)
        new[key] = dep

        if inplace:
            self.deps = new
            return

        else:
            new_instance = type(self)(deps=new)
            return new_instance

    # ....................... #

    @overload
    def register_many(
        self,
        deps: dict[DepKey[T], T],
        *,
        inplace: Literal[True],
    ) -> None:
        """Register multiple dependencies at once.

        :param deps: Mapping from dependency key to dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If any of the dependencies are already registered.
        """
        ...

    @overload
    def register_many(
        self,
        deps: dict[DepKey[T], T],
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Register multiple dependencies at once.

        :param deps: Mapping from dependency key to dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If any of the dependencies are already registered.
        """
        ...

    def register_many(
        self,
        deps: dict[DepKey[T], T],
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        """Register multiple dependencies at once.

        :param deps: Mapping from dependency key to dependency instance.
        :param inplace: When ``True``, mutate the dependencies container in place, otherwise return a new instance.
        :returns: The dependencies container instance if ``inplace`` is ``False``, otherwise ``None``.
        :raises CoreError: If any of the dependencies are already registered.
        """

        already_registered = set(self.deps.keys()).intersection(deps.keys())

        if already_registered:
            raise CoreError(
                f"Dependencies are already registered for keys: {already_registered}"
            )

        new = dict(self.deps)
        new.update(deps)

        if inplace:
            self.deps = new
            return

        else:
            new_instance = type(self)(deps=new)
            return new_instance

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
