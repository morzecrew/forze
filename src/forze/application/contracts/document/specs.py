"""Specifications for document models and storage layout."""

from datetime import timedelta
from typing import Generic, TypedDict, TypeVar, final

import attrs

from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)  #! Arbitrary read model (CoreModel or so)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
class DocumentCacheSpec(TypedDict, total=False):
    """Cache specification for a document aggregate."""

    enabled: bool
    """Enable caching for the document aggregate."""

    ttl: timedelta
    """Default TTL for cache entries."""


# ....................... #


@final
class DocumentReadSpec(TypedDict, Generic[R]):
    """Read specification for a document aggregate."""

    source: str
    """Source name for the read operations."""

    model: type[R]
    """Model type for the read operations."""


# ....................... #


@final
class DocumentWriteModels(TypedDict, Generic[D, C, U]):
    """Write models for a document aggregate."""

    domain: type[D]
    """Model type for the domain model."""

    create_cmd: type[C]
    """Model type for the create command."""

    update_cmd: type[U]  #! not required ?
    """Model type for the update command."""


# ....................... #


@final
class DocumentWriteSpec(TypedDict, Generic[D, C, U]):
    """Write specification for a document aggregate."""

    source: str
    """Source name for the write operations."""

    models: DocumentWriteModels[D, C, U]
    """Write models for the document aggregate."""


# ....................... #


@final
class DocumentHistorySpec(TypedDict):
    """History specification for a document aggregate."""

    source: str
    """Source name for the history operations."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentSpec(Generic[R, D, C, U]):
    """Declarative specification for a document aggregate."""

    namespace: str
    """Primary namespace for the document aggregate."""

    read: DocumentReadSpec[R]
    """Read specification for the document aggregate."""

    write: DocumentWriteSpec[D, C, U] | None = None
    """Write specification for the document aggregate."""

    history: DocumentHistorySpec | None = None
    """History specification for the document aggregate."""

    cache: DocumentCacheSpec | None = None
    """Cache specification for the document aggregate."""

    # ....................... #

    def supports_soft_delete(self) -> bool:
        """Return ``True`` when the domain model supports soft deletion."""

        if self.write is None:
            return False

        return issubclass(self.write["models"]["domain"], SoftDeletionMixin)

    # ....................... #

    def supports_update(self) -> bool:
        """Return ``True`` when the update command exposes writable fields."""

        if self.write is None:
            return False

        return self.write["models"]["update_cmd"].model_fields != {}
