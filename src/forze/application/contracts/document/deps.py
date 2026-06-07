"""Document dependency keys and routers."""

from typing import Any, TypeVar

from pydantic import BaseModel

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import DocumentCommandPort, DocumentQueryPort
from .specs import DocumentSpec

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #

DocumentQueryDepPort = ConfigurableDepPort[
    DocumentSpec[R, Any, Any, Any],
    DocumentQueryPort[R],
]

DocumentCommandDepPort = ConfigurableDepPort[
    DocumentSpec[R, D, C, U],
    DocumentCommandPort[R, D, C, U],
]

# ....................... #

DocumentQueryDepKey = DepKey[DocumentQueryDepPort[Any]]("document_query")
"""Key used to register the ``DocumentQueryDepPort`` implementation."""

DocumentCommandDepKey = DepKey[DocumentCommandDepPort[Any, Any, Any, Any]](
    "document_command"
)
"""Key used to register the ``DocumentCommandDepPort`` implementation."""

# ....................... #


class DocumentDeps(ConvenientDeps):
    """Convenience wrapper for document dependencies."""

    def query(self, spec: DocumentSpec[R, Any, Any, Any]) -> DocumentQueryPort[R]:
        """Resolve a document query port for the given spec."""

        return self._resolve_configurable(
            DocumentQueryDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def command(
        self,
        spec: DocumentSpec[R, D, C, U],
    ) -> DocumentCommandPort[R, D, C, U]:
        """Resolve a document command port for the given spec."""

        return self._resolve_command(
            DocumentCommandDepKey,
            spec,
            route=spec.name,
        )
