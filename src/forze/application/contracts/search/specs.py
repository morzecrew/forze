from datetime import timedelta
from typing import Any, Mapping, Sequence, TypeAlias, TypedDict

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.serialization import (
    ModelCodec,
    default_model_codec,
    stored_field_names_for,
)

from ..base import BaseSpec
from ..crypto import FieldEncryption
from ..querying import QuerySortExpression
from ..querying.sort_resolution import read_fields_for_model, validate_sort_fields


# ----------------------- #


def _validate_search_encryption(
    *,
    spec_name: object,
    model_type: type[BaseModel],
    encryption: FieldEncryption | None,
    default_sort: QuerySortExpression | None,
) -> None:
    """Reject typo'd sealed field names and sealed fields used as ``default_sort`` keys."""

    if encryption is None:
        return

    encryption.validate_fields_exist(
        stored_field_names_for(model_type), spec_name=spec_name
    )

    if default_sort is not None and (
        forbidden := encryption.forbidden_sort_fields(default_sort)
    ):
        raise exc.configuration(
            f"Search spec {spec_name!r} default_sort uses field-encrypted field(s) "
            f"{forbidden}: sealed fields have no order at rest and cannot be sort keys."
        )

# ----------------------- #


class SearchFuzzySpec(TypedDict, total=False):
    """Fuzzy matching configuration for a search index."""

    max_distance_ratio: float
    """Maximum edit-distance ratio (0.0–1.0) for fuzzy matches."""

    prefix_length: int
    """Number of leading characters that must match exactly."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchResultSnapshotSpec(BaseSpec):
    """Result-ID snapshot: defaults for a search surface, or DI registration for a snapshot port."""

    enabled: bool | None = None
    """If set, used when a request :class:`.types.SearchResultSnapshotOptions` omits ``mode``."""

    ttl: timedelta = timedelta(minutes=5)
    """Default time-to-live for a stored ordered-ID snapshot."""

    max_ids: int = 50_000
    """Upper bound on how many IDs a snapshot may hold (enforced by the search layer)."""

    chunk_size: int = 5_000
    """Size of each KV chunk when materializing ID lists."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchSpec[M: BaseModel](BaseSpec):
    """Specification for simple search (one index)."""

    model_type: type[M]
    """Pydantic model class for searchable documents."""

    fields: Sequence[str] = attrs.field(validator=attrs.validators.min_len(1))
    """Indexed fields."""

    default_weights: Mapping[str, float] | None = attrs.field(default=None)
    """Default weights for fields."""

    fuzzy: SearchFuzzySpec | None = attrs.field(default=None)
    """Fuzzy matching configuration."""

    sensitive: bool = attrs.field(default=False)
    """Read model carries credential/secret material (password hashes, token digests);
    generated external surfaces (HTTP route generators, MCP tools/resources) must refuse
    to project it. Defaults to ``False``."""

    snapshot: SearchResultSnapshotSpec | None = attrs.field(default=None)
    """Optional defaults for result-ID snapshotting."""

    default_sort: QuerySortExpression | None = attrs.field(default=None)
    """Default ``sorts`` when callers omit them (required for models without ``id``)."""

    read_codec: ModelCodec[M, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Optional row codec override; use :attr:`resolved_read_codec` at runtime."""

    encryption: FieldEncryption | None = attrs.field(default=None)
    """Field-encryption policy (see :class:`FieldEncryption`). For an **external index**
    (Meilisearch) the encrypted fields are sealed in the index document and decrypted on read;
    for **in-place search** (Postgres/Mongo over an encrypted document table) they are
    decrypted out of the search results. Must be the **same policy** as the underlying
    ``DocumentSpec.encryption`` so in-place search reproduces the document write's AAD and
    decrypts its ciphertext. ``None`` (default) = no field encryption."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.default_sort is not None:
            validate_sort_fields(
                self.default_sort,
                read_fields=read_fields_for_model(self.model_type),
                spec_name=self.name,
                model=self.model_type,
            )

        _validate_search_encryption(
            spec_name=self.name,
            model_type=self.model_type,
            encryption=self.encryption,
            default_sort=self.default_sort,
        )

        if len(self.fields) != len(set(self.fields)):
            raise exc.configuration("Search fields must be unique.")

        if not self.default_weights:
            return

        for f, w in self.default_weights.items():
            if f not in self.fields:
                raise exc.configuration(
                    f"Default weight for unknown search field '{f}'."
                )

            if w < 0 or w > 1:
                raise exc.configuration(
                    f"Default weight for search field '{f}' should be between 0.0 and 1.0."
                )

        if not all(f in self.default_weights for f in self.fields):
            raise exc.configuration(
                "Default weights must be provided for all search fields."
            )

    # ....................... #

    @property
    def resolved_read_codec(self) -> ModelCodec[M, Any]:
        """Row codec (explicit override or :func:`default_model_codec`)."""

        if self.read_codec is not None:
            return self.read_codec

        return default_model_codec(self.model_type)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class HubSearchSpec[M: BaseModel](BaseSpec):
    """Hub (junction) search (homogeneous search)."""

    model_type: type[M]
    """Pydantic read model for hub rows."""

    members: Sequence[SearchSpec[Any]] = attrs.field(
        validator=attrs.validators.min_len(1),
    )
    """At least one :class:`SearchSpec` (hub leg / linked index)."""

    default_member_weights: Mapping[str, float] | None = attrs.field(default=None)
    """Default weights for hub members."""

    snapshot: SearchResultSnapshotSpec | None = attrs.field(default=None)
    """Optional defaults for result-ID snapshotting (outer hub adapter)."""

    default_sort: QuerySortExpression | None = attrs.field(default=None)
    """Default ``sorts`` for hub browse/cursor when callers omit them."""

    read_codec: ModelCodec[M, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Row decode/encode codec; defaults to :class:`PydanticModelCodec`."""

    encryption: FieldEncryption | None = attrs.field(default=None)
    """Field-encryption policy for hub-row fields (see :class:`FieldEncryption`), decrypted
    out of hub search results. Mirror of the hub read model's ``DocumentSpec.encryption``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.default_sort is not None:
            validate_sort_fields(
                self.default_sort,
                read_fields=read_fields_for_model(self.model_type),
                spec_name=self.name,
                model=self.model_type,
            )

        _validate_search_encryption(
            spec_name=self.name,
            model_type=self.model_type,
            encryption=self.encryption,
            default_sort=self.default_sort,
        )

        names = [member.name for member in self.members]

        if len(names) != len(set(names)):
            raise exc.configuration(
                "Each hub search member must use a SearchSpec with a distinct name."
            )

        if self.default_member_weights:
            for member in self.members:
                if member.name not in self.default_member_weights:
                    raise exc.configuration(
                        f"Default weight for unknown search field '{member.name}'."
                    )

                w = self.default_member_weights[member.name]

                if w < 0 or w > 1:
                    raise exc.configuration(
                        f"Default weight for search field '{member.name}' should be between 0.0 and 1.0."
                    )

    # ....................... #

    @property
    def resolved_read_codec(self) -> ModelCodec[M, Any]:
        """Row codec (explicit override or :func:`default_model_codec`)."""

        if self.read_codec is not None:
            return self.read_codec

        return default_model_codec(self.model_type)


# ....................... #

FederatedSearchMemberSpec: TypeAlias = SearchSpec[Any] | HubSearchSpec[Any]
"""A federated leg: single-index :class:`SearchSpec` or nested :class:`HubSearchSpec`."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class FederatedSearchSpec[X: BaseModel](BaseSpec):
    """Federated search specification (heterogeneous search)."""

    members: Sequence[FederatedSearchMemberSpec] = attrs.field(
        validator=attrs.validators.min_len(2),
    )
    """At least two members, each a :class:`SearchSpec` or :class:`HubSearchSpec`."""

    snapshot: SearchResultSnapshotSpec | None = attrs.field(default=None)
    """Optional defaults for result-ID snapshotting (outer federated adapter)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        names = [member.name for member in self.members]

        if len(names) != len(set(names)):
            raise exc.configuration(
                "Each federated search member must use a distinct name "
                "(the SearchSpec or HubSearchSpec name)."
            )
