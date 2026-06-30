from datetime import timedelta
from typing import Any, Mapping, Sequence, TypeAlias

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.serialization import (
    ModelCodec,
    model_codec_for,
    stored_field_names_for,
)

from ..base import BaseSpec
from ..conformity import (
    ReadConformity,
    derive_lenient_read_fields,
    validate_lenient_read_fields,
    validate_materialized_computed,
)
from ..crypto import FieldEncryption
from ..querying import QuerySortExpression
from ..querying.sort_resolution import read_fields_for_model, validate_sort_fields

# ----------------------- #


def _sealed_fields(encryption: FieldEncryption | None) -> frozenset[str]:
    """Fields whose stored value is ciphertext — cannot be aggregated or highlighted."""

    if encryption is None:
        return frozenset()

    return encryption.encrypted | encryption.searchable


# ....................... #


def _validate_search_lenient_read_fields(
    *,
    spec_name: object,
    model_type: type[BaseModel],
    lenient_read_fields: frozenset[str],
    fields: Sequence[str],
) -> None:
    """Validate a search spec's lenient read fields (shared rules + index-field overlap)."""

    if not lenient_read_fields:
        return

    if overlap := lenient_read_fields & frozenset(fields):
        raise exc.configuration(
            f"Search spec {spec_name!r}: field(s) {sorted(overlap)} are indexed "
            "(searchable) and cannot be lenient — an indexed field needs a real column.",
        )

    validate_lenient_read_fields(
        model_type=model_type,
        lenient=lenient_read_fields,
        spec_name=spec_name,
    )


# ....................... #


def _validate_search_materialized(
    *,
    spec_name: object,
    model_type: type[BaseModel],
    materialized: frozenset[str],
    lenient_read_fields: frozenset[str],
) -> None:
    """Validate a search spec's materialized fields (computed on model, disjoint from lenient)."""

    if not materialized:
        return

    validate_materialized_computed(
        model_type, materialized, spec_name=spec_name, label="read"
    )

    if overlap := materialized & lenient_read_fields:
        raise exc.configuration(
            f"Search spec {spec_name!r}: field(s) {sorted(overlap)} cannot be both "
            "materialized (stored) and lenient (not stored).",
        )


# ....................... #


def _validate_search_encryption(
    *,
    spec_name: object,
    model_type: type[BaseModel],
    encryption: FieldEncryption | None,
    default_sort: QuerySortExpression | None,
    lenient_read_fields: frozenset[str] = frozenset(),
) -> None:
    """Reject typo'd sealed field names and sealed fields used as ``default_sort`` keys."""

    if encryption is None:
        return

    # Lenient fields are not stored in search results, so they cannot be sealed.
    encryption.validate_fields_exist(
        stored_field_names_for(model_type) - lenient_read_fields, spec_name=spec_name
    )

    if default_sort is not None and (
        forbidden := encryption.forbidden_sort_fields(default_sort)
    ):
        raise exc.configuration(
            f"Search spec {spec_name!r} default_sort uses field-encrypted field(s) "
            f"{forbidden}: sealed fields have no order at rest and cannot be sort keys."
        )


# ....................... #


def _validate_search_facetable_highlightable(
    *,
    spec_name: object,
    model_type: type[BaseModel],
    fields: Sequence[str],
    facetable_fields: frozenset[str],
    highlightable_fields: frozenset[str] | None,
    lenient_read_fields: frozenset[str],
    materialized: frozenset[str],
    encryption: FieldEncryption | None,
) -> None:
    """Validate a search spec's facet/highlight field declarations.

    A **facetable** field must be a real read column (exists on the model, not lenient,
    not sealed) — a value distribution needs a stored value. A **highlightable** field
    must be a searchable ``fields`` member (only analyzed text can be highlighted) and
    not sealed (ciphertext has no fragments).
    """

    if not facetable_fields and highlightable_fields is None:
        return

    sealed: frozenset[str] = _sealed_fields(encryption)

    if facetable_fields:
        # Materialized computed fields are persisted real columns, so they are facetable too.
        read_fields = read_fields_for_model(model_type) | materialized

        if missing := facetable_fields - read_fields:
            raise exc.configuration(
                f"Search spec {spec_name!r}: facetable field(s) {sorted(missing)} are not "
                "fields on the read model.",
            )

        if lenient := facetable_fields & lenient_read_fields:
            raise exc.configuration(
                f"Search spec {spec_name!r}: facetable field(s) {sorted(lenient)} are lenient "
                "(not stored) and cannot be faceted — faceting needs a real column.",
            )

        if forbidden := facetable_fields & sealed:
            raise exc.configuration(
                f"Search spec {spec_name!r}: facetable field(s) {sorted(forbidden)} are "
                "field-encrypted — a sealed value cannot be aggregated.",
            )

    if highlightable_fields is None:
        return

    if not_searchable := highlightable_fields - frozenset(fields):
        raise exc.configuration(
            f"Search spec {spec_name!r}: highlightable field(s) {sorted(not_searchable)} are "
            "not indexed (searchable) — only searchable fields can be highlighted.",
        )

    if forbidden := highlightable_fields & sealed:
        raise exc.configuration(
            f"Search spec {spec_name!r}: highlightable field(s) {sorted(forbidden)} are "
            "field-encrypted — a sealed value has no highlightable text.",
        )


# ....................... #


def _validate_search_default_sort(
    *,
    spec_name: str,
    model_type: type[BaseModel],
    default_sort: QuerySortExpression | None,
    lenient_read_fields: frozenset[str] = frozenset(),
    materialized: frozenset[str] = frozenset(),
) -> None:
    """Validate a search spec's ``default_sort`` against the read model (nested-path aware).

    Materialized computed fields are persisted columns and so may be sort keys; lenient
    read fields have no column and so cannot be — the accepted field set adds the former
    and removes the latter.
    """

    if default_sort is None:
        return

    validate_sort_fields(
        default_sort,
        read_fields=(read_fields_for_model(model_type) | materialized)
        - lenient_read_fields,
        spec_name=spec_name,
        model=model_type,
        client_facing=False,
    )


# ....................... #


def _seq_to_frozenset(value: Sequence[str] | None) -> frozenset[str] | None:
    return None if value is None else frozenset(value)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchFuzzySpec:
    """Fuzzy matching configuration for a search index (immutable value object)."""

    max_distance_ratio: float = 0.34
    """Maximum edit-distance ratio (``0.0``–``1.0``) for fuzzy matches; higher is more lenient."""

    def __attrs_post_init__(self) -> None:
        if not 0.0 <= self.max_distance_ratio <= 1.0:
            raise exc.configuration(
                "SearchFuzzySpec.max_distance_ratio must be between 0.0 and 1.0.",
            )


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

    default_weights: Mapping[str, float] | None = None
    """Default weights for fields."""

    fuzzy: SearchFuzzySpec | None = None
    """Fuzzy matching configuration."""

    sensitive: bool = False
    """Read model carries credential/secret material (password hashes, token digests);
    generated external surfaces (HTTP route generators, MCP tools/resources) must refuse
    to project it. Defaults to ``False``."""

    snapshot: SearchResultSnapshotSpec | None = None
    """Optional defaults for result-ID snapshotting."""

    default_sort: QuerySortExpression | None = None
    """Default ``sorts`` when callers omit them (required for models without ``id``)."""

    materialized: frozenset[str] = attrs.field(factory=frozenset, converter=frozenset)
    """``@computed_field`` names on the read model that are persisted as real columns on
    the search relation, so search results can be **filtered and sorted by the derived
    value** at the database instead of only recomputing it after decode.

    Mirrors :attr:`~forze.application.contracts.document.DocumentSpec.materialized`; the
    column is typically created by the document side over the same table. Each name must
    be a ``@computed_field`` on :attr:`model_type` and must not also be
    :attr:`lenient_read_fields` (stored vs not-stored). Relational in-place search only;
    inert for an external index. Unlike documents, a missing column is **not** validated
    at startup (search has no schema check) — it fails on first query. Empty by default."""

    read_conformity: ReadConformity = "strict"
    """Storage-conformity level. ``strict`` (default): every returned field must map to a
    column. ``lenient``: auto-derive :attr:`lenient_read_fields` from the read model —
    every defaulted, non-identity, non-indexed (:attr:`fields`), non-:attr:`materialized`
    field (static defaults only) becomes absent-tolerant. Explicit
    :attr:`lenient_read_fields` are always included on top. See
    :attr:`resolved_lenient_read_fields`."""

    lenient_read_fields: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset,
    )
    """Read-model field names permitted to be **absent** from the search relation.

    Mirrors :attr:`~forze.application.contracts.document.DocumentSpec.lenient_read_fields`
    for the search return shape: a lenient field is dropped from the result projection
    and hydrated from its model default. Each name must be a non-computed,
    non-identity read field carrying a default, and must **not** be an indexed
    (searchable) :attr:`fields` member — an indexed field needs a real column. Lenient
    fields are also excluded from sort keys. Empty by default (strict)."""

    facetable_fields: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset,
    )
    """Read-model field names that may be **faceted** (value-distribution aggregated) when
    a caller passes :attr:`~.types.SearchOptions.facets`. Each must be a real
    stored read field (not :attr:`lenient_read_fields`, not field-encrypted); analyzed-text
    :attr:`fields` are poor facet targets but are not rejected. Empty by default — faceting
    is opt-in because it has index/mapping implications (e.g. an external index must declare
    the field filterable/keyword). A facet request for a field outside this set is refused."""

    highlightable_fields: frozenset[str] | None = attrs.field(
        default=None,
        converter=_seq_to_frozenset,
    )
    """Searchable field names that may be **highlighted** (matched fragments returned) when
    a caller passes :attr:`~.types.SearchOptions.highlight`. ``None`` (default)
    means all searchable :attr:`fields`; an explicit set narrows it. Each named field must be
    an indexed :attr:`fields` member (only analyzed text can be highlighted) and not
    field-encrypted."""

    read_codec: ModelCodec[M, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Optional row codec override; use :attr:`resolved_read_codec` at runtime."""

    encryption: FieldEncryption | None = None
    """Field-encryption policy (see :class:`FieldEncryption`). For an **external index**
    (Meilisearch) the encrypted fields are sealed in the index document and decrypted on read;
    for **in-place search** (Postgres/Mongo over an encrypted document table) they are
    decrypted out of the search results. Must be the **same policy** as the underlying
    ``DocumentSpec.encryption`` so in-place search reproduces the document write's AAD and
    decrypts its ciphertext. ``None`` (default) = no field encryption."""

    max_results: int | None = None
    """Server-side cap on offset-search results when a caller passes **no** ``limit``.

    A simple offset search with no pagination ``limit`` otherwise fetches the entire
    matched set into memory — a latent OOM on a large index. When set, an unbounded
    request is fetched at most this many rows (an explicit caller ``limit`` is honoured
    as-is, never raised). Defense in depth; ``None`` (default) keeps the previous
    fetch-everything behaviour. Independent of result-snapshot ``max_ids`` (which already
    bounds the snapshot pool)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_results is not None and self.max_results < 1:
            raise exc.configuration(
                f"SearchSpec {self.name!r}: max_results must be at least 1 when set."
            )

        _validate_search_materialized(
            spec_name=self.name,
            model_type=self.model_type,
            materialized=self.materialized,
            lenient_read_fields=self.lenient_read_fields,
        )

        _validate_search_lenient_read_fields(
            spec_name=self.name,
            model_type=self.model_type,
            lenient_read_fields=self.lenient_read_fields,
            fields=self.fields,
        )

        _validate_search_default_sort(
            spec_name=self.name,
            model_type=self.model_type,
            default_sort=self.default_sort,
            lenient_read_fields=self.resolved_lenient_read_fields,
            materialized=self.materialized,
        )

        _validate_search_encryption(
            spec_name=self.name,
            model_type=self.model_type,
            encryption=self.encryption,
            default_sort=self.default_sort,
            lenient_read_fields=self.resolved_lenient_read_fields,
        )

        _validate_search_facetable_highlightable(
            spec_name=self.name,
            model_type=self.model_type,
            fields=self.fields,
            facetable_fields=self.facetable_fields,
            highlightable_fields=self.highlightable_fields,
            lenient_read_fields=self.resolved_lenient_read_fields,
            materialized=self.materialized,
            encryption=self.encryption,
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

        if any(f not in self.default_weights for f in self.fields):
            raise exc.configuration(
                "Default weights must be provided for all search fields."
            )

    # ....................... #

    @property
    def resolved_lenient_read_fields(self) -> frozenset[str]:
        """Effective lenient read fields: explicit plus, under ``read_conformity``
        ``"lenient"``, the auto-derived eligible fields (indexed :attr:`fields` and
        :attr:`materialized` columns excluded). This is what the search backend and sort
        validation read."""

        if self.read_conformity == "lenient":
            return self.lenient_read_fields | derive_lenient_read_fields(
                self.model_type, exclude=frozenset(self.fields) | self.materialized
            )

        return self.lenient_read_fields

    # ....................... #

    @property
    def resolved_read_codec(self) -> ModelCodec[M, Any]:
        """Row codec (explicit override, or a default codec that persists
        :attr:`materialized` computed fields as columns)."""

        if self.read_codec is not None:
            return self.read_codec

        return model_codec_for(self.model_type, materialized=self.materialized)

    # ....................... #

    @property
    def resolved_highlightable_fields(self) -> frozenset[str]:
        """Effective highlightable fields: the explicit :attr:`highlightable_fields`, or —
        when ``None`` — all searchable :attr:`fields` minus field-encrypted ones (ciphertext
        has no highlightable text). What the search backend reads to bound a highlight request.
        """

        if self.highlightable_fields is None:
            return frozenset(self.fields) - _sealed_fields(self.encryption)

        return self.highlightable_fields


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

    default_member_weights: Mapping[str, float] | None = None
    """Default weights for hub members."""

    snapshot: SearchResultSnapshotSpec | None = None
    """Optional defaults for result-ID snapshotting (outer hub adapter)."""

    default_sort: QuerySortExpression | None = None
    """Default ``sorts`` for hub browse/cursor when callers omit them."""

    materialized: frozenset[str] = attrs.field(factory=frozenset, converter=frozenset)
    """``@computed_field`` names on the hub-row model persisted as real hub columns, so
    hub results can be filtered/sorted by the derived value. Mirror of
    :attr:`SearchSpec.materialized`; relational in-place only, not startup-validated."""

    read_conformity: ReadConformity = "strict"
    """Storage-conformity level for hub-row fields. ``strict`` (default): every returned
    field must map to a hub column. ``lenient``: auto-derive :attr:`lenient_read_fields`
    (every defaulted, non-identity, non-:attr:`materialized` hub-row field; static defaults
    only). Explicit :attr:`lenient_read_fields` are always included on top."""

    lenient_read_fields: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset,
    )
    """Hub-row fields permitted to be **absent** from the hub relation: dropped from the
    result projection and hydrated from their default. Mirror of
    :attr:`SearchSpec.lenient_read_fields` (a hub has no index ``fields`` of its own).
    Each must be a non-computed, non-identity hub-row field carrying a default. Empty by
    default (strict)."""

    read_codec: ModelCodec[M, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Row decode/encode codec; defaults to :class:`PydanticModelCodec`."""

    encryption: FieldEncryption | None = None
    """Field-encryption policy for hub-row fields (see :class:`FieldEncryption`), decrypted
    out of hub search results. Mirror of the hub read model's ``DocumentSpec.encryption``."""

    facetable_fields: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset,
    )
    """Hub-row field names that may be **faceted** when a caller passes
    :attr:`~.types.SearchOptions.facets`. A hub facet distribution is computed
    over the merged hub rows (homogeneous model), so it is a flat :class:`FacetResults` like
    single-index. Each must be a real, non-lenient, non-encrypted hub-row field. Empty = opt-out."""

    highlightable_fields: frozenset[str] | None = attrs.field(default=None)
    """Hub-row field names that may be **highlighted**. ``None`` = the union of
    all member legs' searchable :attr:`SearchSpec.fields`. Each named field must be searchable
    on at least one member leg and not field-encrypted. (Highlighting a merged hub row is
    backend-dependent — see the adapter docs.)"""

    # ....................... #

    @property
    def _member_searchable_fields(self) -> frozenset[str]:
        """Union of every member leg's searchable ``fields`` (the hub's highlightable base)."""

        fields: frozenset[str] = frozenset()

        for member in self.members:
            fields = fields | frozenset(member.fields)

        return fields

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _validate_search_materialized(
            spec_name=self.name,
            model_type=self.model_type,
            materialized=self.materialized,
            lenient_read_fields=self.lenient_read_fields,
        )

        validate_lenient_read_fields(
            model_type=self.model_type,
            lenient=self.lenient_read_fields,
            spec_name=self.name,
        )

        _validate_search_default_sort(
            spec_name=self.name,
            model_type=self.model_type,
            default_sort=self.default_sort,
            lenient_read_fields=self.resolved_lenient_read_fields,
            materialized=self.materialized,
        )

        _validate_search_encryption(
            spec_name=self.name,
            model_type=self.model_type,
            encryption=self.encryption,
            default_sort=self.default_sort,
            lenient_read_fields=self.resolved_lenient_read_fields,
        )

        _validate_search_facetable_highlightable(
            spec_name=self.name,
            model_type=self.model_type,
            fields=tuple(self._member_searchable_fields),
            facetable_fields=self.facetable_fields,
            highlightable_fields=self.highlightable_fields,
            lenient_read_fields=self.resolved_lenient_read_fields,
            materialized=self.materialized,
            encryption=self.encryption,
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
    def resolved_lenient_read_fields(self) -> frozenset[str]:
        """Effective lenient hub-row fields: explicit plus, under ``read_conformity``
        ``"lenient"``, the auto-derived eligible fields (:attr:`materialized` excluded).
        """

        if self.read_conformity == "lenient":
            return self.lenient_read_fields | derive_lenient_read_fields(
                self.model_type, exclude=self.materialized
            )

        return self.lenient_read_fields

    # ....................... #

    @property
    def resolved_read_codec(self) -> ModelCodec[M, Any]:
        """Row codec (explicit override, or a default codec that persists
        :attr:`materialized` computed fields as columns)."""

        if self.read_codec is not None:
            return self.read_codec

        return model_codec_for(self.model_type, materialized=self.materialized)

    # ....................... #

    @property
    def resolved_highlightable_fields(self) -> frozenset[str]:
        """Effective highlightable hub-row fields: the explicit
        :attr:`highlightable_fields`, or — when ``None`` — the union of all member legs'
        searchable :attr:`SearchSpec.fields` minus field-encrypted ones."""

        if self.highlightable_fields is None:
            return self._member_searchable_fields - _sealed_fields(self.encryption)

        return self.highlightable_fields


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

    snapshot: SearchResultSnapshotSpec | None = None
    """Optional defaults for result-ID snapshotting (outer federated adapter)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        names = [member.name for member in self.members]

        if len(names) != len(set(names)):
            raise exc.configuration(
                "Each federated search member must use a distinct name "
                "(the SearchSpec or HubSearchSpec name)."
            )
