"""Specifications for document models and storage layout."""

from typing import Any, Generic, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.domain.models import BaseDTO, Document

from ..base import BaseSpec
from ..cache import CacheSpec
from ..querying import QuerySortExpression
from ..querying.sort_resolution import read_fields_for_model, validate_sort_fields
from ..codecs import stored_field_names_for
from .codecs import DocumentCodecs, document_codecs_for_spec
from .write_types import DocumentWriteTypes

# ----------------------- #

R = TypeVar("R", bound=BaseModel)

# Any is default to avoid separate spec for read-only documents
D = TypeVar("D", bound=Document, default=Any)
C = TypeVar("C", bound=BaseDTO, default=Any)
U = TypeVar("U", bound=BaseDTO, default=Any)

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

    codecs: DocumentCodecs[R, D, C, U] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Optional codec overrides; defaults are derived from model types."""

    # ....................... #

    @property
    def resolved_codecs(self) -> DocumentCodecs[R, D, C, U]:
        """Codecs for this aggregate (explicit or auto-derived)."""

        if self.codecs is not None:
            return self.codecs

        return document_codecs_for_spec(
            read=self.read,
            write=self.write,
            history_enabled=self.history_enabled,
        )

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

        return bool(
            stored_field_names_for(
                self.write["update_cmd"],
                include_computed=False,
            )
        )
