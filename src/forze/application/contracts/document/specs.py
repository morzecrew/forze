"""Specifications for document models and storage layout."""

from typing import Generic, TypedDict, TypeVar, final

import attrs

from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

from ..base import BaseSpec
from ..cache import CacheSpec

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)  #! Arbitrary read model (CoreModel or so)
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

    update_cmd: type[U]  #! not required ?
    """Model type for the update command."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentSpec(BaseSpec, Generic[R, D, C, U]):
    """Declarative specification for a document aggregate."""

    read: type[R]
    """Read specification for the document aggregate."""

    write: DocumentWriteTypes[D, C, U] | None = attrs.field(default=None)
    """Write specification for the document aggregate."""

    history_enabled: bool = attrs.field(default=False)
    """Enable history for the document aggregate. Defaults to ``False``."""

    cache: CacheSpec | None = attrs.field(default=None)
    """Cache specification for the document aggregate."""

    # ....................... #

    def supports_soft_delete(self) -> bool:
        """Return ``True`` when the domain model supports soft deletion."""

        if self.write is None:
            return False

        return issubclass(self.write["domain"], SoftDeletionMixin)

    # ....................... #

    def supports_update(self) -> bool:
        """Return ``True`` when the update command exposes writable fields."""

        if self.write is None:
            return False

        return self.write["update_cmd"].model_fields != {}
