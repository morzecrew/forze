"""Write-side model type bundle for document aggregates."""

from typing import Generic, NotRequired, TypedDict, TypeVar, final

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

# ----------------------- #

D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
class DocumentWriteTypes(TypedDict, Generic[D, C, U]):
    """Write models for a document aggregate."""

    domain: type[D]
    """Model type for the domain model."""

    create_cmd: type[C]
    """Model type for the create command."""

    update_cmd: NotRequired[type[U]]
    """Model type for the update command."""
