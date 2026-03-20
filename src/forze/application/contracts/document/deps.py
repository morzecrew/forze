"""Document dependency keys and routers."""

from typing import TYPE_CHECKING, Any, Protocol, final, runtime_checkable

import attrs

from ..cache import CachePort
from ..deps import DepKey, DepRouter
from .ports import DocumentReadPort, DocumentWritePort
from .specs import DocumentSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

DocSpec = DocumentSpec[Any, Any, Any, Any]
"""Type-erased document specification."""

DocReadPort = DocumentReadPort[Any]
"""Type-erased document read port."""

DocWritePort = DocumentWritePort[Any, Any, Any, Any]
"""Type-erased document write port."""

# ....................... #


@runtime_checkable
class DocumentReadDepPort(Protocol):
    """Factory protocol for building :class:`DocumentReadPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: CachePort | None = None,
    ) -> DocReadPort:
        """Build a document read port, optionally backed by a cache."""
        ...


# ....................... #


@runtime_checkable
class DocumentWriteDepPort(Protocol):
    """Factory protocol for building :class:`DocumentWritePort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: CachePort | None = None,
    ) -> DocWritePort:
        """Build a document write port, optionally backed by a cache."""
        ...


# ....................... #

DocumentReadDepKey = DepKey[DocumentReadDepPort]("document_read")
"""Key used to register the :class:`DocumentReadDepPort` implementation."""

DocumentWriteDepKey = DepKey[DocumentWriteDepPort]("document_write")
"""Key used to register the :class:`DocumentWriteDepPort` implementation."""

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DocumentReadDepRouter(
    DepRouter[DocSpec, DocumentReadDepPort],
    DocumentReadDepPort,
    dep_key=DocumentReadDepKey,
):
    """Router that dispatches :class:`DocumentReadDepPort` calls by spec."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: CachePort | None = None,
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
    """Router that dispatches :class:`DocumentWriteDepPort` calls by spec."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: CachePort | None = None,
    ) -> DocWritePort:
        route = self._select(spec)

        return route(context, spec, cache=cache)
