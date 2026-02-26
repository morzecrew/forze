from typing import TYPE_CHECKING, Callable, Protocol, final, runtime_checkable

import attrs

from ..ports import StoragePort
from .base import DepKey, RoutingKey

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@runtime_checkable
class StorageDepPort(Protocol):
    """Factory protocol for building :class:`StoragePort` instances."""

    def __call__(self, context: "ExecutionContext", bucket: str) -> StoragePort:
        """Build a storage port bound to the given context and bucket."""
        ...


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class StorageDepRouter(StorageDepPort):
    selector: Callable[[str], RoutingKey]
    routes: dict[RoutingKey, StorageDepPort]
    default: StorageDepPort

    # ....................... #

    def __call__(self, context: "ExecutionContext", bucket: str) -> StoragePort:
        sel = self.selector(bucket)
        route = self.routes.get(sel, self.default)
        return route(context, bucket)


# ....................... #

StorageDepKey: DepKey[StorageDepPort] = DepKey("storage")
"""Key used to register the :class:`StorageDepPort` implementation."""
