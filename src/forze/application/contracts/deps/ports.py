from typing import Protocol, Self, TypeVar

from .value_objects import DepKey

# ----------------------- #

T = TypeVar("T")

# ....................... #


class DepsPort(Protocol):
    """Abstract access to dependency resolution."""

    def provide(self, key: DepKey[T]) -> T:
        """Return the dependency instance registered under ``key``."""
        ...

    # ....................... #

    def exists(self, key: DepKey[T]) -> bool:
        """Return ``True`` if the dependency is registered."""
        ...

    # ....................... #

    @classmethod
    def merge(cls, *deps: Self) -> Self:
        """Merge multiple dependency containers into a single container."""
        ...

    # ....................... #

    def without(self, key: DepKey[T]) -> Self:
        """Create a new dependency container without the given key."""
        ...

    # ....................... #

    def empty(self) -> bool:
        """Return ``True`` if the dependency container is empty."""
        ...
