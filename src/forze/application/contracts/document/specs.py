"""Specifications for document models and storage layout."""

from typing import Any, Generic, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.domain.models import BaseDTO, Document

from ..base import BaseSpec
from ..cache import CacheSpec
from ..querying import QueryFieldPolicy, QuerySortExpression
from ..querying.field_policy import validate_field_policy
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

    sensitive: bool = attrs.field(default=False)
    """Read model carries credential/secret material (password hashes, token digests);
    generated external surfaces (HTTP route generators, MCP tools/resources) must refuse
    to project it. Defaults to ``False``."""

    cache: CacheSpec | None = attrs.field(default=None)
    """Cache specification for the document aggregate."""

    default_sort: QuerySortExpression | None = attrs.field(default=None)
    """Default ``sorts`` when callers omit them (required for read models without ``id``)."""

    query_policy: QueryFieldPolicy | None = attrs.field(default=None)
    """Optional allow-sets restricting which fields a governed caller may filter / sort by.
    ``None`` (default) allows every read-model field. Drives discovery and (when enforced)
    boundary validation."""

    encrypted_fields: frozenset[str] = attrs.field(
        default=frozenset(),
        converter=frozenset,
    )
    """Stored field names to encrypt at rest (randomized field-level encryption).

    Empty (default) = no field encryption. When set, a backend that wires a keyring
    transparently encrypts these fields on write and decrypts on read; the rest stay
    plaintext and queryable. Encrypted fields cannot be filtered/sorted on. They are
    decrypted on full-model reads and on both typed (``select_*``) and raw (``project_*``)
    projections that select them — except that a projection of an
    :attr:`encryption_binds_record_id` field must also select ``id`` (the AAD needs it).
    Requires a ``KeyringDepKey`` in the deps (e.g. via ``CryptoDepsModule``)."""

    searchable_fields: frozenset[str] = attrs.field(
        default=frozenset(),
        converter=frozenset,
    )
    """Stored field names to encrypt *deterministically* so equality queries work.

    Same plaintext maps to the same ciphertext, so `$eq`/`$neq`/`$in`/`$nin` filters on
    these fields are transparently rewritten to match the value at rest — no separate
    blind-index column. The trade: deterministic encryption leaks equality/frequency
    within a tenant, and only equality (not range/sort/like) is supported. Disjoint from
    :attr:`encrypted_fields`. Requires a ``DeterministicCipherDepKey`` in the deps."""

    encryption_binds_record_id: bool = attrs.field(default=False)
    """Bind the record's ``id`` into the AAD of every :attr:`encrypted_fields` ciphertext.

    Defaults to ``False`` (the AAD already binds field name + tenant). When ``True``, a
    ciphertext additionally cannot be transplanted to a *different record of the same
    tenant and field*. Applies only to randomized :attr:`encrypted_fields` (never to
    :attr:`searchable_fields`, whose ciphertext must stay record-independent for equality
    queries). Two consequences: a filter-based bulk ``update_matching`` of a bound field is
    refused (no per-record id), and existing ciphertext written before enabling this still
    reads (decrypt falls back to the legacy AAD) — run ``reencrypt_documents`` to upgrade it
    to the bound form."""

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
        read_fields = read_fields_for_model(self.read)

        if self.default_sort is not None:
            validate_sort_fields(
                self.default_sort,
                read_fields=read_fields,
                spec_name=str(self.name),
            )

        if self.query_policy is not None:
            validate_field_policy(
                self.query_policy,
                read_fields=read_fields,
                spec_name=str(self.name),
            )

    # ....................... #

    def filterable_fields(self) -> frozenset[str]:
        """Field names a governed caller may filter on (policy allow-set, or all read fields)."""

        read_fields = read_fields_for_model(self.read)

        if self.query_policy is None:
            return read_fields

        return self.query_policy.resolve_filterable(read_fields)

    # ....................... #

    def sortable_fields(self) -> frozenset[str]:
        """Field names a governed caller may sort by (policy allow-set, or all read fields)."""

        read_fields = read_fields_for_model(self.read)

        if self.query_policy is None:
            return read_fields

        return self.query_policy.resolve_sortable(read_fields)

    # ....................... #

    def aggregatable_fields(self) -> frozenset[str]:
        """Field names a governed caller may group by / aggregate (allow-set, or all read fields)."""

        read_fields = read_fields_for_model(self.read)

        if self.query_policy is None:
            return read_fields

        return self.query_policy.resolve_aggregatable(read_fields)

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
