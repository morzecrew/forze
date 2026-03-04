"""Document dependency keys and routers."""

from typing import TYPE_CHECKING, Any, Protocol, final, runtime_checkable

import attrs

from ..deps import DepKey, DepRouter
from .internal import SearchSpec
from .ports import SearchReadPort, SearchWritePort

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@runtime_checkable
class SearchReadDepPort(Protocol):
    """Factory protocol for building :class:`SearchReadPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: SearchSpec,
    ) -> SearchReadPort[Any]: ...


# ....................... #


@runtime_checkable
class SearchWriteDepPort(Protocol):
    """Factory protocol for building :class:`SearchWritePort` instances."""

    def __call__(
        self, context: "ExecutionContext", spec: SearchSpec
    ) -> SearchWritePort[Any]: ...


# ....................... #

SearchReadDepKey = DepKey[SearchReadDepPort]("search_read")
"""Key used to register the :class:`SearchReadDepPort` implementation."""

SearchWriteDepKey = DepKey[SearchWriteDepPort]("search_write")
"""Key used to register the :class:`SearchWriteDepPort` implementation."""

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SearchDepRouter(DepRouter[SearchSpec, SearchReadDepPort], SearchReadDepPort):
    dep_key = SearchReadDepKey
    def __call__(
        self, context: "ExecutionContext", spec: SearchSpec
    ) -> SearchReadPort[Any]:
        route = self._select(spec)

        return route(context, spec)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SearchWriteDepRouter(DepRouter[SearchSpec, SearchWriteDepPort], SearchWriteDepPort):
    dep_key = SearchWriteDepKey
    def __call__(
        self, context: "ExecutionContext", spec: SearchSpec
    ) -> SearchWritePort[Any]:
        route = self._select(spec)

        return route(context, spec)
