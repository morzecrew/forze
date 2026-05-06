"""Document dependency keys and routers."""

from typing import Any, TypeVar

from pydantic import BaseModel

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..base import BaseDepPort, DepKey
from .ports import DocumentCommandPort, DocumentQueryPort
from .specs import DocumentSpec

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #

DocumentQueryDepPort = BaseDepPort[
    DocumentSpec[R, Any, Any, Any],
    DocumentQueryPort[R],
]

DocumentCommandDepPort = BaseDepPort[
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
