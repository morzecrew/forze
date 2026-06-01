"""Specifications for document models and storage layout."""

from typing import Any, Generic, NotRequired, TypedDict, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..base import BaseSpec
from ..cache import CacheSpec
from ..querying import QuerySortExpression
from ..querying.sort_resolution import read_fields_for_model, validate_sort_fields

# ----------------------- #

R = TypeVar("R", bound=BaseModel)

# Any is default to avoid separate spec for read-only documents
D = TypeVar("D", bound=Document, default=Any)
C = TypeVar("C", bound=CreateDocumentCmd, default=Any)
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

    default_sort: QuerySortExpression | None = attrs.field(default=None)
    """Default ``sorts`` when callers omit them (required for read models without ``id``)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.default_sort is not None:
            validate_sort_fields(
                self.default_sort,
                read_fields=read_fields_for_model(self.read),
                spec_name=self.name,
            )

    # ....................... #

    def supports_update(self) -> bool:
        """Return ``True`` when the update command exposes writable fields."""

        if self.write is None:
            return False

        if "update_cmd" not in self.write:
            return False

        return self.write["update_cmd"].model_fields != {}
