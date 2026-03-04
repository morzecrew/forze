"""Specifications for document models and storage layout."""

from datetime import timedelta
from typing import Generic, NotRequired, Optional, TypedDict, TypeVar, final

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

#! TODO: review and add support for read-only documents (no domain model, only read model)

# ....................... #


@final
class DocumentModelSpec(TypedDict, Generic[R, D, C, U]):
    """Concrete model classes that make up a document aggregate."""

    read: type[R]
    """Read model exposed to consumers."""

    domain: type[D]
    """Domain model used for invariants and business rules."""

    create_cmd: type[C]
    """Command DTO used to create new domain instances."""

    update_cmd: type[U]
    """Command DTO used for partial updates of existing instances."""


# ....................... #


@final
class DocumentSourceSpec(TypedDict):
    """Storage-level source names associated with a document aggregate."""

    read: str
    """Primary readable source (e.g. Postgres view or table name)."""

    write: str
    """Writable source backing persistence for the aggregate."""

    history: NotRequired[str]
    """Optional source used to store history or audit events."""


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
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentSpec(Generic[R, D, C, U]):
    """Declarative specification for a document aggregate.

    A :class:`DocumentSpec` binds together:

    * namespace used for cache keys
    * data storage sources
    * concrete model types for read/domain/commands
    * optional search configuration

    Implementations of :class:`DocumentPort` and related ports use this spec
    to configure themselves.
    """

    namespace: str
    """Namespace used for cache keys."""

    sources: DocumentSourceSpec
    """Data storage sources."""

    models: DocumentModelSpec[R, D, C, U]
    """Concrete model types for read/domain/commands."""

    cache: Optional[DocumentCacheSpec] = None
    """Cache specification for the document aggregate."""

    # ....................... #

    def supports_soft_delete(self) -> bool:
        """Return ``True`` when the domain model supports soft deletion."""

        return issubclass(self.models["domain"], SoftDeletionMixin)

    # ....................... #

    def supports_update(self) -> bool:
        """Return ``True`` when the update command exposes writable fields."""

        return self.models["update_cmd"].model_fields != {}
