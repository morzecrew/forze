"""Document dependency keys and routers."""

from typing import TYPE_CHECKING, Any, Optional, Protocol, final, runtime_checkable

import attrs

from ..deps import DepKey, DepRouter
from .ports import DocumentCachePort, DocumentPort
from .specs import DocumentSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

DocSpec = DocumentSpec[Any, Any, Any, Any]
DocPort = DocumentPort[Any, Any, Any, Any]

# ....................... #


@runtime_checkable
class DocumentCacheDepPort(Protocol):
    """Factory protocol for building :class:`DocumentCachePort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
    ) -> DocumentCachePort:
        """Build a document cache port bound to the given context and spec."""
        ...


# ....................... #

DocumentCacheDepKey = DepKey[DocumentCacheDepPort]("document_cache")
"""Key used to register the :class:`DocumentCacheDepPort` implementation."""

# ....................... #


@runtime_checkable
class DocumentDepPort(Protocol):
    """Factory protocol for building :class:`DocumentPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: Optional[DocumentCachePort] = None,
    ) -> DocPort:
        """Build a document port bound to the given context, spec,
        and optional cache.
        """
        ...


# ....................... #

DocumentDepKey = DepKey[DocumentDepPort]("document")
"""Key used to register the :class:`DocumentDepPort` implementation."""


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DocumentDepRouter(DepRouter[DocSpec, DocumentDepPort], DocumentDepPort):
    dep_key = DocumentDepKey

    # ....................... #

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: Optional[DocumentCachePort] = None,
    ) -> DocPort:
        route = self._select(spec)

        return route(context, spec, cache=cache)
