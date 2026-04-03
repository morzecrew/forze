from typing import TYPE_CHECKING, Protocol, Self, TypeVar, final

import attrs

from forze.base.descriptors import hybridmethod

from .specs import BaseSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

T = TypeVar("T")

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class DepKey[T]:
    """Typed key used to identify dependencies in the kernel.

    The ``name`` is used for diagnostics and error messages; type information
    is carried through the type parameter ``T`` for static resolution.
    """

    name: str
    """Human-readable name for diagnostics and error messages."""


# ....................... #


class DepsPort(Protocol):
    """Abstract access to dependency resolution.

    Implementations provide a registry of dependencies keyed by :class:`DepKey`.
    Merging is used when combining multiple modules; ``without`` supports
    routers that extract a dependency from a container.
    """

    def provide(
        self,
        key: DepKey[T],
        *,
        route: str | None = None,
        fallback_to_plain: bool = True,
    ) -> T:
        """Return the dependency instance registered under ``key``."""
        ...  # pragma: no cover

    # ....................... #

    def exists(self, key: DepKey[T], *, route: str | None = None) -> bool:
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

    def without_route(self, key: DepKey[T], route: str) -> Self:
        """Create a new dependency container without the given route."""
        ...  # pragma: no cover

    # ....................... #

    def empty(self) -> bool:
        """Return ``True`` if the dependency container is empty."""
        ...  # pragma: no cover

    # ....................... #

    def count(self) -> int:
        """Return total number of registered dependency entries."""
        ...  # pragma: no cover


# ....................... #


class BaseDepPort[S: BaseSpec, Port](Protocol):
    """Base protocol for building resource ports."""

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: S,
    ) -> Port: ...  # pragma: no cover
