"""Document dependency keys and routers."""

from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..base import DepKey
from ..cache import CachePort
from .ports import DocumentCommandPort, DocumentQueryPort
from .specs import DocumentSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #
#! TODO: remove cache port from dep factory and use internally in adapter constructors (?)


@runtime_checkable
class DocumentQueryDepPort(Protocol):
    """Factory protocol for building :class:`DocumentQueryPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: DocumentSpec[R, D, C, U],
        cache: (
            CachePort | None
        ) = None,  #! should it be part of internal adapter semantics instead?
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
        cache: (
            CachePort | None
        ) = None,  #! should it be part of internal adapter semantics instead?
    ) -> DocumentCommandPort[R, D, C, U]:
        """Build a document command port, optionally backed by a cache."""
        ...


# ....................... #

DocumentQueryDepKey = DepKey[DocumentQueryDepPort]("document_query")
"""Key used to register the :class:`DocumentQueryDepPort` implementation."""

DocumentCommandDepKey = DepKey[DocumentCommandDepPort]("document_command")
"""Key used to register the :class:`DocumentCommandDepPort` implementation."""
