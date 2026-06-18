"""Write-side model type bundle for document aggregates."""

from typing import Any, Generic, NotRequired, TypedDict, TypeVar, final

from forze.domain.models import BaseDTO, Document

# ----------------------- #

D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=Any)

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
