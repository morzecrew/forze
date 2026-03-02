"""Dependency router for spec-based provider selection."""

from enum import StrEnum
from typing import Any, Callable, ClassVar, Generic, Optional, Self, TypeVar

import attrs

from forze.base.errors import CoreError

from .ports import DepsPort
from .value_objects import DepKey

# ----------------------- #

SpecT = TypeVar("SpecT")
PortT = TypeVar("PortT")
DepPortT = TypeVar("DepPortT")

RoutingKey = str | StrEnum
"""Key used to select a route (e.g. bucket name, namespace)."""

Selector = Callable[[SpecT], RoutingKey]
"""Function that extracts a routing key from a specification."""

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class DepRouter(Generic[SpecT, DepPortT]):
    """Dependency router that selects a provider based on a specification.

    Uses a selector to derive a routing key from the spec; looks up the
    corresponding provider in the routes map. Falls back to :attr:`default`
    when the selector's result is not in routes. Subclasses implement
    :class:`DepKey` and are used as :class:`DepsPort` providers that extract
    the router from the container and delegate to the selected route.
    """

    selector: Selector[SpecT]
    """Function to select the routing key based on the specification."""

    routes: dict[RoutingKey, DepPortT]
    """Mapping from routing key to dependency provider."""

    default: RoutingKey
    """Default routing key when selector result is not in routes."""

    dep_key: ClassVar[DepKey[Any]]
    """Dependency key used to register this router in the container."""

    # ....................... #

    def __attrs_post_init__(self):
        if self.default not in self.routes:
            raise CoreError(f"Default routing key `{self.default}` not found")

    # ....................... #

    def _select(self, spec: SpecT) -> DepPortT:
        sel = self.selector(spec)

        return self.routes.get(sel) or self.routes[self.default]

    # ....................... #

    @classmethod
    def from_deps(
        cls,
        *,
        deps: dict[RoutingKey, DepsPort],
        selector: Selector[SpecT],
        default: RoutingKey,
    ) -> tuple[Self, Optional[DepsPort]]:
        """Create a new dependency router from a dictionary of dependencies.

        :param deps: Dictionary of dependencies to use for the router.
        :param selector: Function to select the routing key based on the specification.
        :param default: Default routing key to use if the selector does not return a valid routing key.
        :returns: A tuple containing the new router and the remainder of dependencies.
        """

        routes: dict[RoutingKey, DepPortT] = {}
        glob_remainder: Optional[DepsPort] = None

        for key, dep in deps.items():
            routes[key] = dep.provide(cls.dep_key)
            remainder = dep.without(cls.dep_key)

            if glob_remainder is None:
                glob_remainder = remainder

            else:
                glob_remainder = glob_remainder.merge(glob_remainder, remainder)

        if glob_remainder is not None and glob_remainder.empty():
            glob_remainder = None

        return cls(selector=selector, routes=routes, default=default), glob_remainder
