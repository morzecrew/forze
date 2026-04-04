"""Document dependency keys and routers."""

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from ..base import DepKey
from ..cache import CachePort
from .ports import DocumentCommandPort, DocumentQueryPort
from .specs import DocumentSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

DocSpec = DocumentSpec[Any, Any, Any, Any]
"""Type-erased document specification."""

DocQueryPort = DocumentQueryPort[Any]
"""Type-erased document query port."""

DocCommandPort = DocumentCommandPort[Any, Any, Any, Any]
"""Type-erased document command port."""

# ....................... #


@runtime_checkable
class DocumentQueryDepPort(Protocol):
    """Factory protocol for building :class:`DocumentQueryPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: CachePort | None = None,
    ) -> DocQueryPort:
        """Build a document query port, optionally backed by a cache."""
        ...


# ....................... #


@runtime_checkable
class DocumentCommandDepPort(Protocol):
    """Factory protocol for building :class:`DocumentCommandPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocSpec,
        cache: CachePort | None = None,
    ) -> DocCommandPort:
        """Build a document command port, optionally backed by a cache."""
        ...


# ....................... #

DocumentQueryDepKey = DepKey[DocumentQueryDepPort]("document_query")
"""Key used to register the :class:`DocumentQueryDepPort` implementation."""

DocumentCommandDepKey = DepKey[DocumentCommandDepPort]("document_command")
"""Key used to register the :class:`DocumentCommandDepPort` implementation."""
