"""Search dependency keys and routers."""

from typing import TYPE_CHECKING, Any, Protocol, TypeVar, final, runtime_checkable

import attrs
from pydantic import BaseModel

from ..deps import DepKey, DepRouter
from .ports import SearchReadPort, SearchWritePort
from .specs import SearchSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

M = TypeVar("M", bound=BaseModel)

# ....................... #


@runtime_checkable
class SearchReadDepPort(Protocol):
    """Factory protocol for building :class:`SearchReadPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: SearchSpec[M],
    ) -> SearchReadPort[M]: ...


# ....................... #


@runtime_checkable
class SearchWriteDepPort(Protocol):
    """Factory protocol for building :class:`SearchWritePort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: SearchSpec[M],
    ) -> SearchWritePort[M]: ...


# ....................... #

SearchReadDepKey = DepKey[SearchReadDepPort]("search_read")
"""Key used to register the :class:`SearchReadDepPort` implementation."""

SearchWriteDepKey = DepKey[SearchWriteDepPort]("search_write")
"""Key used to register the :class:`SearchWriteDepPort` implementation."""

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SearchReadDepRouter(
    DepRouter[SearchSpec[Any], SearchReadDepPort],
    SearchReadDepPort,
    dep_key=SearchReadDepKey,
):
    """Router that dispatches :class:`SearchReadDepPort` calls by spec."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: SearchSpec[Any],
    ) -> SearchReadPort[Any]:
        route = self._select(spec)

        return route(context, spec)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SearchWriteDepRouter(
    DepRouter[SearchSpec[Any], SearchWriteDepPort],
    SearchWriteDepPort,
    dep_key=SearchWriteDepKey,
):
    """Router that dispatches :class:`SearchWriteDepPort` calls by spec."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: SearchSpec[Any],
    ) -> SearchWritePort[Any]:
        route = self._select(spec)

        return route(context, spec)
