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
Selector = Callable[[SpecT], RoutingKey]

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class DepRouter(Generic[SpecT, DepPortT]):
    """Dependency router used to select and route dependencies based on a specification."""

    selector: Selector[SpecT]
    """Function to select the routing key based on the specification."""

    routes: dict[RoutingKey, DepPortT]
    """Mapping from routing key to dependency container."""

    default: RoutingKey
    """Default routing key to use if the selector does not return a valid routing key."""

    dep_key: ClassVar[DepKey[Any]]
    """Dependency key to use for the router."""

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
