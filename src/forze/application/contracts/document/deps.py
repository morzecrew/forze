"""Document dependency keys and routers."""

from typing import TYPE_CHECKING, Any, Optional, Protocol, final, runtime_checkable

import attrs

from ..cache import CachePort
from ..deps import DepKey, DepRouter
from .ports import DocumentPort
from .specs import DocumentSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

DocSpec = DocumentSpec[Any, Any, Any, Any]
DocPort = DocumentPort[Any, Any, Any, Any]

# ....................... #


@runtime_checkable
class DocumentDepPort(Protocol):
    """Factory protocol for building :class:`DocumentPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: Optional[CachePort] = None,
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

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: Optional[CachePort] = None,
    ) -> DocPort:
        route = self._select(spec)

        return route(context, spec, cache=cache)
