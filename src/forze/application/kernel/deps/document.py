from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Protocol,
    final,
    runtime_checkable,
)

import attrs

from ..ports import DocumentCachePort, DocumentPort
from ..specs import DocumentSpec
from .base import DepKey, RoutingKey

if TYPE_CHECKING:
    from ..context import ExecutionContext

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


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DocumentDepRouter(DocumentDepPort):
    selector: Callable[[DocSpec], RoutingKey]
    routes: dict[RoutingKey, DocumentDepPort]
    default: DocumentDepPort

    # ....................... #

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: Optional[DocumentCachePort] = None,
    ) -> DocPort:
        sel = self.selector(spec)
        route = self.routes.get(sel, self.default)

        return route(context, spec, cache=cache)


# ....................... #

DocumentCacheDepKey: DepKey[DocumentCacheDepPort] = DepKey("document_cache")
"""Key used to register the :class:`DocumentCacheDepPort` implementation."""


DocumentDepKey: DepKey[DocumentDepPort] = DepKey("document")
"""Key used to register the :class:`DocumentDepPort` implementation."""
