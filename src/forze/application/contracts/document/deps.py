"""Document dependency keys and routers."""

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from ..base import DepKey
from ..cache import CachePort
from .ports import C, D, DocumentCommandPort, DocumentQueryPort, R, U
from .specs import DocumentSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@runtime_checkable
class DocumentQueryDepPort(Protocol):
    """Factory protocol for building :class:`DocumentQueryPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocumentSpec[R, D, C, U],
        cache: CachePort | None = None,
    ) -> DocumentQueryPort[R]:
        """Build a document query port, optionally backed by a cache."""
        ...


# ....................... #


@runtime_checkable
class DocumentCommandDepPort(Protocol):
    """Factory protocol for building :class:`DocumentCommandPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocumentSpec[R, D, C, U],
        cache: CachePort | None = None,
    ) -> DocumentCommandPort[R, D, C, U]:
        """Build a document command port, optionally backed by a cache."""
        ...


# ....................... #

DocumentQueryDepKey = DepKey[DocumentQueryDepPort]("document_query")
"""Key used to register the :class:`DocumentQueryDepPort` implementation."""

DocumentCommandDepKey = DepKey[DocumentCommandDepPort]("document_command")
"""Key used to register the :class:`DocumentCommandDepPort` implementation."""
