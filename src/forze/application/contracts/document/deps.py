"""Document dependency keys and routers."""

from typing import TYPE_CHECKING, Any, Optional, Protocol, final, runtime_checkable

import attrs

from ..cache import CachePort
from ..deps import DepKey, DepRouter
from .ports import DocumentReadPort, DocumentWritePort
from .specs import DocumentSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

DocSpec = DocumentSpec[Any, Any, Any, Any]
DocReadPort = DocumentReadPort[Any]
DocWritePort = DocumentWritePort[Any, Any, Any, Any]

# ....................... #


@runtime_checkable
class DocumentReadDepPort(Protocol):
    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: Optional[CachePort] = None,
    ) -> DocReadPort: ...


# ....................... #


@runtime_checkable
class DocumentWriteDepPort(Protocol):
    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: Optional[CachePort] = None,
    ) -> DocWritePort: ...


# ....................... #

DocumentReadDepKey = DepKey[DocumentReadDepPort]("document_read")
DocumentWriteDepKey = DepKey[DocumentWriteDepPort]("document_write")

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DocumentReadDepRouter(
    DepRouter[DocSpec, DocumentReadDepPort],
    DocumentReadDepPort,
    dep_key=DocumentReadDepKey,
):
    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: Optional[CachePort] = None,
    ) -> DocReadPort:
        route = self._select(spec)

        return route(context, spec, cache=cache)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DocumentWriteDepRouter(
    DepRouter[DocSpec, DocumentWriteDepPort],
    DocumentWriteDepPort,
    dep_key=DocumentWriteDepKey,
):
    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: Optional[CachePort] = None,
    ) -> DocWritePort:
        route = self._select(spec)

        return route(context, spec, cache=cache)
