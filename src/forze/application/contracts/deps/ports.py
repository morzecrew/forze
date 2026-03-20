"""Dependency container protocol."""

from typing import Protocol, Self, TypeVar

from forze.base.descriptors import hybridmethod

from .key import DepKey

# ----------------------- #

T = TypeVar("T")

# ....................... #


class DepsPort(Protocol):
    """Abstract access to dependency resolution.

    Implementations provide a registry of dependencies keyed by :class:`DepKey`.
    Merging is used when combining multiple modules; ``without`` supports
    routers that extract a dependency from a container.
    """

    def provide(self, key: DepKey[T]) -> T:
        """Return the dependency instance registered under ``key``."""
        ...  # pragma: no cover

    # ....................... #

    def exists(self, key: DepKey[T]) -> bool:
        """Return ``True`` if the dependency is registered."""
        ...  # pragma: no cover

    # ....................... #

    @hybridmethod
    def merge(cls: type[Self], *deps: Self) -> Self:  # type: ignore[misc]
        """Merge multiple dependency containers into a single container."""
        ...  # pragma: no cover

    # ....................... #

    @merge.instancemethod
    def _merge_instance(self, *deps: Self) -> Self:
        """Merge this dependency container with another containers."""
        ...  # pragma: no cover

    # ....................... #

    def without(self, key: DepKey[T]) -> Self:
        """Create a new dependency container without the given key."""
        ...  # pragma: no cover

    # ....................... #

    def empty(self) -> bool:
        """Return ``True`` if the dependency container is empty."""
        ...  # pragma: no cover
