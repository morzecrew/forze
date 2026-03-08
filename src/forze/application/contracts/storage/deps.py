"""Storage dependency keys and routers."""

from typing import TYPE_CHECKING, Protocol, final, runtime_checkable

import attrs

from ..deps import DepKey, DepRouter
from .ports import StoragePort

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@runtime_checkable
class StorageDepPort(Protocol):
    """Factory protocol for building :class:`StoragePort` instances."""

    def __call__(self, context: "ExecutionContext", bucket: str) -> StoragePort:
        """Build a storage port bound to the given context and bucket."""
        ...


# ....................... #

StorageDepKey = DepKey[StorageDepPort]("storage")
"""Key used to register the :class:`StorageDepPort` implementation."""


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class StorageDepRouter(DepRouter[str, StorageDepPort], StorageDepPort):
    dep_key = StorageDepKey

    def __call__(self, context: "ExecutionContext", bucket: str) -> StoragePort:
        route = self._select(bucket)

        return route(context, bucket)
