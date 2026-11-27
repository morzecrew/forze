"""The tables of framework-owned documents, derived from their specs (RFC 0080 §5.7).

Each framework spec renders, and the render passes the startup validator's column, type and
nullability checks, so the derivation cannot drift from what validation accepts. The type
table, the defaults, the indexes and the refusals are each pinned here.
"""

from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal, TypeVar
from uuid import UUID

import attrs
import pytest
from pydantic import BaseModel, EmailStr, Field, computed_field

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.document import DocumentIndex, DocumentSpec
from forze.application.contracts.guarantees import NonOverlapping, Reference, UniqueTogether
from forze.application.contracts.inventory import SpecRegistry, SpecSource
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.base.exceptions import CoreException
from forze.base.primitives import Int64
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_postgres.execution.deps.configs import (
    PostgresDocumentConfig,
    PostgresInboxConfig,
    PostgresReadOnlyDocumentConfig,
)
from forze_postgres.execution.deps.document_binding import (
    postgres_document_schema_spec_for_binding,
)
from forze_postgres.execution.deps.module import PostgresDepsModule
from forze_postgres.execution.deps.owned import owned_relations, resolve_owned_relations
from forze_postgres.kernel.catalog.introspect.types import IndexShapeInfo, PostgresType
from forze_postgres.kernel.catalog.introspect.utils import normalize_pg_type
from forze_postgres.kernel.catalog.owned import (
    BoundOwnedRelation,
    CatalogRelation,
    OwnedColumn,
    OwnedIndex,
    OwnedRelationShape,
    diff_relation,
    document_shape,
    render_sql,
)
from forze_postgres.kernel.catalog.owned.documents import column_type
from forze_postgres.kernel.catalog.owned.matching import key_column
from forze_postgres.kernel.catalog.owned.quoting import unquote_ident
from forze_postgres.kernel.catalog.validation.validate_schema import (
    PostgresDocumentSchemaSpec,
    _write_field_names_union,  # pyright: ignore[reportPrivateUsage]
)
from forze_postgres.kernel.catalog.validation.validate_schema_types import (
    validate_field_nullability,
    validate_field_type_compatibility,
)
from forze_kits.integrations.audit import audit_record_spec
from forze_kits.integrations.progress import job_record_spec
from forze_kits.integrations.realtime import realtime_mailbox_spec
from forze_kits.integrations.realtime import realtime_mailbox_spec
from tests.support.framework_documents import (
    document_config,
    framework_contributions,
    framework_document_specs,
    framework_registry,
)

# ----------------------- #

_CLIENT: Any = object()


def _schema_spec(
    spec: DocumentSpec[Any, Any, Any, Any],
    config: PostgresDocumentConfig | None = None,
) -> PostgresDocumentSchemaSpec:
    name = str(spec.name)

    return postgres_document_schema_spec_for_binding(
        name, spec=spec, config=config or document_config("app", name)
    )


def _catalog(shape: OwnedRelationShape) -> dict[str, PostgresType]:
    """The column types a database created from *shape* reports, as the introspector reads
    them."""

    return {
        column.name: PostgresType(
            base=normalize_pg_type(column.type.removesuffix("[]")),
            is_array=column.type.endswith("[]"),
            not_null=not column.nullable,
        )
        for column in shape.columns
    }


_SEALED = [
    audit_record_spec(encryption=FieldEncryption(encrypted=frozenset({"metadata"}))),
    job_record_spec(encryption=FieldEncryption(encrypted=frozenset({"message", "subject"}))),
    realtime_mailbox_spec(encryption=FieldEncryption(encrypted=frozenset({"payload", "event"}))),
]
"""The framework specs that take a field-encryption policy, each sealing what it allows."""

_SPECS = framework_document_specs()
_IDS = [str(spec.name) for spec in _SPECS]
_ALL = [*_SPECS, *_SEALED]
_ALL_IDS = [*_IDS, *(f"{spec.name}-sealed" for spec in _SEALED)]


# ....................... #


@pytest.mark.parametrize("tenant_aware", [False, True])
@pytest.mark.parametrize("spec", _ALL, ids=_ALL_IDS)
def test_every_framework_table_passes_the_validators_column_checks(
    spec: DocumentSpec[Any, Any, Any, Any], tenant_aware: bool
) -> None:
    schema_spec = _schema_spec(
        spec, document_config("app", str(spec.name), tenant_aware=tenant_aware)
    )
    shape = document_shape(schema_spec)
    catalog = _catalog(shape)

    assert schema_spec.write_domain_model is not None
    assert schema_spec.write_create_model is not None

    required = (
        _write_field_names_union(
            schema_spec.write_domain_model,
            schema_spec.write_create_model,
            schema_spec.write_update_model,
        )
        | schema_spec.materialized
    ) - schema_spec.write_omit_fields

    if tenant_aware:
        required |= {TENANT_ID_FIELD}
        assert catalog[TENANT_ID_FIELD] == PostgresType(base="uuid", is_array=False, not_null=True)

    assert required <= set(catalog)

    for model in (
        schema_spec.write_domain_model,
        schema_spec.write_create_model,
        schema_spec.write_update_model,
    ):
        if model is None:
            continue

        validate_field_type_compatibility(
            model=model,
            column_types=catalog,
            omit_fields=frozenset(),
            label=str(spec.name),
            sealed_fields=schema_spec.sealed_fields,
        )
        validate_field_nullability(
            model=model, column_types=catalog, omit_fields=frozenset(), label=str(spec.name)
        )

    assert shape.primary_key == ("id",)


@pytest.mark.parametrize("spec", _SPECS, ids=_IDS)
def test_a_list_of_scalars_renders_as_an_array(spec: DocumentSpec[Any, Any, Any, Any]) -> None:
    # The validator accepts a bare scalar column for a list too, so it cannot catch this.
    shape = document_shape(_schema_spec(spec))
    domain = spec.write["domain"]  # pyright: ignore[reportOptionalSubscript]

    for column in shape.columns:
        field = domain.model_fields.get(column.name)

        if field is None or column.type == "jsonb":
            continue

        listed = str(field.annotation).startswith(("list[", "tuple["))
        assert column.type.endswith("[]") == listed, column.name


@pytest.mark.parametrize("spec", _SPECS, ids=_IDS)
def test_every_packed_hlc_is_a_bigint(spec: DocumentSpec[Any, Any, Any, Any]) -> None:
    # A packed HlcTimestamp overflows integer: its field carries Int64 (row 32).
    columns = {column.name: column for column in document_shape(_schema_spec(spec)).columns}

    if "hlc" in columns:
        assert columns["hlc"].type == "bigint"


def test_the_stored_file_size_is_a_bigint() -> None:
    files = next(spec for spec in _SPECS if spec.name == "stored_files")
    columns = {column.name: column for column in document_shape(_schema_spec(files)).columns}

    assert columns["size"].type == "bigint"


def test_the_identity_indexes_render_as_required() -> None:
    by_name = {str(spec.name): spec for spec in _SPECS}
    accounts = document_shape(_schema_spec(by_name["authn_password_accounts"]))
    bindings = document_shape(_schema_spec(by_name["authz_principal_role_bindings"]))

    assert [(i.keys, i.unique, i.required, i.nulls_not_distinct) for i in accounts.indexes] == [
        (("username",), True, True, False)
    ]
    assert [(i.keys, i.unique, i.nulls_not_distinct) for i in bindings.indexes] == [
        (("principal_id", "role_id", "scope_tenant_id"), True, True)
    ]



@pytest.mark.parametrize(
    ("name", "keys"),
    [
        ("authn_api_key_accounts", [("key_hash",), ("principal_id",)]),
        ("authn_token_sessions", [("refresh_digest",), ("principal_id",)]),
    ],
)
def test_a_table_read_by_a_lookup_renders_its_index(
    name: str, keys: list[tuple[str, ...]]
) -> None:
    # Every API-key request and token refresh finds its row by these columns.
    shape = document_shape(_schema_spec({str(spec.name): spec for spec in _SPECS}[name]))

    assert [(i.keys, i.unique) for i in shape.indexes] == [(key, False) for key in keys]


def test_the_mailbox_renders_its_replay_and_ack_indexes() -> None:
    shape = document_shape(_schema_spec(realtime_mailbox_spec()))
    keys = [i.keys[-3:] for i in shape.indexes]

    assert ("principal", "hlc", "id") in keys
    # An ack resolves its event to a position by principal and event_id.
    assert any(k[-2:] == ("principal", "event_id") for k in keys)

# ....................... #


class _Status(StrEnum):
    ON = "on"
    OFF = "off"


class _Nested(BaseModel):
    x: int


_T = TypeVar("_T")

type _Tags = list[str]


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (UUID, ("uuid", False)),
        (str, ("text", False)),
        (_Status, ("text", False)),
        (Literal["a", "b"], ("text", False)),
        (EmailStr, ("text", False)),
        (bool, ("boolean", False)),
        (int, ("integer", False)),
        (Int64, ("bigint", False)),
        (float, ("double precision", False)),
        (Decimal, ("numeric", False)),
        (datetime, ("timestamptz", False)),
        (date, ("date", False)),
        (time, ("time", False)),
        (bytes, ("bytea", False)),
        (list[str], ("text[]", False)),
        (tuple[UUID, ...], ("uuid[]", False)),
        (list[Int64], ("bigint[]", False)),
        (list[_Nested], ("jsonb", False)),
        (dict[str, Any], ("jsonb", False)),
        (_Nested, ("jsonb", False)),
        (Any, ("jsonb", False)),
        (str | None, ("text", True)),
        (Int64 | None, ("bigint", True)),
        (list[str] | None, ("text[]", True)),
        (_Tags, ("text[]", False)),
    ],
)
def test_each_annotation_emits_one_column_type(annotation: Any, expected: tuple[str, bool]) -> None:
    assert column_type(annotation) == expected


@pytest.mark.parametrize("annotation", [int | str, _T, set[str], Literal[1, 2]])
def test_an_annotation_outside_the_table_has_no_column_type(annotation: Any) -> None:
    assert column_type(annotation) is None


# ....................... #


class _Fields(BaseModel):
    name: str
    active: bool = True
    count: int = 0
    ratio: float = 0.5
    label: str = "it's"
    status: _Status = _Status.ON
    tags: list[str] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)
    items: list[_Nested] = Field(default_factory=list)
    note: str | None = None
    secret: str = "plain"


class _Doc(_Fields, Document):
    pass


class _Create(BaseDTO):
    name: str


class _Read(_Fields, ReadDocument):
    pass


def _spec(**kwargs: Any) -> DocumentSpec[Any, Any, Any, Any]:
    return DocumentSpec(
        name="things",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create},
        **kwargs,
    )


def _shape(
    spec: DocumentSpec[Any, Any, Any, Any] | None = None,
    **config: Any,
) -> OwnedRelationShape:
    return document_shape(
        _schema_spec(spec or _spec(), PostgresDocumentConfig(**{**_RW, **config}))
    )


_RW: dict[str, Any] = {
    "read": ("app", "things"),
    "write": ("app", "things"),
    "bookkeeping_strategy": "application",
}


def test_the_base_columns_come_first_then_the_fields_in_model_order() -> None:
    names = [column.name for column in _shape(tenant_aware=True).columns]

    assert names[:5] == ["id", "rev", "created_at", "last_update_at", TENANT_ID_FIELD]
    assert names[5:] == [
        "name",
        "active",
        "count",
        "ratio",
        "label",
        "status",
        "tags",
        "extra",
        "items",
        "note",
        "secret",
    ]


def test_a_literal_default_or_an_empty_collection_renders_as_default() -> None:
    columns = {column.name: column for column in _shape().columns}

    assert {name: column.default for name, column in columns.items()} == {
        "id": None,
        "rev": None,
        "created_at": None,
        "last_update_at": None,
        "name": None,
        "active": "true",
        "count": "0",
        "ratio": "0.5",
        "label": "'it''s'",
        "status": "'on'",
        "tags": "'{}'",
        "extra": "'{}'",
        "items": "'[]'",
        "note": None,
        "secret": "'plain'",
    }
    assert [name for name, column in columns.items() if column.nullable] == ["note"]


def test_a_scalar_default_on_a_jsonb_column_is_json() -> None:
    # A jsonb column parses its default as JSON: a bare 'pending' fails the CREATE TABLE.
    class _Loose(Document):
        state: Any = "pending"
        count: Any = 0
        flag: Any = True
        mode: Any = _Status.OFF
        ratio: Any = float("nan")
        when: Any = datetime(2026, 1, 1, tzinfo=UTC)

    class _LooseRead(ReadDocument):
        pass

    loose: DocumentSpec[Any, Any, Any, Any] = DocumentSpec(
        name="loose", read=_LooseRead, write={"domain": _Loose, "create_cmd": BaseDTO}
    )
    columns = {column.name: column for column in _shape(loose).columns}

    names = ("state", "count", "flag", "mode", "ratio", "when")

    assert {name: columns[name].default for name in names} == {
        "state": "'\"pending\"'",
        "count": "'0'",
        "flag": "'true'",
        "mode": "'\"off\"'",
        "ratio": None,
        "when": None,
    }


def test_a_default_that_is_no_literal_renders_none() -> None:
    class _Dated(Document):
        since: datetime = datetime(2026, 1, 1, tzinfo=UTC)

    class _DatedRead(ReadDocument):
        since: datetime = datetime(2026, 1, 1, tzinfo=UTC)

    dated: DocumentSpec[Any, Any, Any, Any] = DocumentSpec(
        name="dated", read=_DatedRead, write={"domain": _Dated, "create_cmd": BaseDTO}
    )
    (since,) = (column for column in _shape(dated).columns if column.name == "since")

    assert (since.type, since.nullable, since.default) == ("timestamptz", False, None)


def test_a_materialized_field_is_a_column_typed_by_its_return() -> None:
    class _Named(Document):
        first: str

        @computed_field  # type: ignore[prop-decorator]
        @property
        def full(self) -> str | None:
            return self.first

        @computed_field  # type: ignore[prop-decorator]
        @property
        def either(self) -> int | str:
            return self.first

    class _NamedRead(ReadDocument):
        first: str

        @computed_field  # type: ignore[prop-decorator]
        @property
        def full(self) -> str | None:
            return self.first

        @computed_field  # type: ignore[prop-decorator]
        @property
        def either(self) -> int | str:
            return self.first

    def named(materialized: frozenset[str]) -> DocumentSpec[Any, Any, Any, Any]:
        return DocumentSpec(
            name="named",
            read=_NamedRead,
            write={"domain": _Named, "create_cmd": BaseDTO},
            materialized=materialized,
        )

    (full,) = (c for c in _shape(named(frozenset({"full"}))).columns if c.name == "full")

    assert (full.type, full.nullable) == ("text", True)

    with pytest.raises(CoreException, match="materialized field 'either'"):
        _shape(named(frozenset({"either"})))


def test_a_read_only_binding_has_no_table_to_print() -> None:
    read_only = attrs.evolve(
        _schema_spec(_spec(), PostgresDocumentConfig(**_RW)), write_relation=None
    )

    with pytest.raises(CoreException) as caught:
        document_shape(read_only)

    assert caught.value.code == "postgres_document_render_refused"


@pytest.mark.parametrize("read_only", [False, True])
def test_a_binding_carries_the_sealed_fields_to_validation(read_only: bool) -> None:
    # A read-only binding reads the same ciphertext a writer stored.
    sealed = audit_record_spec(encryption=FieldEncryption(encrypted=frozenset({"metadata"})))
    config: PostgresReadOnlyDocumentConfig = (
        PostgresReadOnlyDocumentConfig(read=("app", "audit_events"))
        if read_only
        else document_config("app", "audit_events")
    )
    schema_spec = postgres_document_schema_spec_for_binding(
        "audit_events", spec=sealed, config=config
    )

    assert schema_spec.sealed_fields == frozenset({"metadata"})


def test_a_sealed_field_in_an_array_column_is_refused_at_startup() -> None:
    # Ciphertext is one string: an array column cannot hold it.
    catalog = {"tags": PostgresType(base="text", is_array=True, not_null=True)}

    with pytest.raises(CoreException, match="array element type"):
        validate_field_type_compatibility(
            model=_Doc,
            column_types=catalog,
            omit_fields=frozenset(),
            label="things",
            sealed_fields=frozenset({"tags"}),
        )


def test_a_sealed_field_is_text_whatever_its_annotation() -> None:
    # Ciphertext has no plaintext type, so an annotation the type table refuses is still text.
    class _Mixed(Document):
        either: int | str | None = None

    class _MixedRead(ReadDocument):
        either: int | str | None = None

    def mixed(sealed: bool) -> DocumentSpec[Any, Any, Any, Any]:
        return DocumentSpec(
            name="mixed",
            read=_MixedRead,
            write={"domain": _Mixed, "create_cmd": BaseDTO},
            encryption=FieldEncryption(encrypted=frozenset({"either"})) if sealed else None,
        )

    (either,) = (column for column in _shape(mixed(True)).columns if column.name == "either")

    assert (either.type, either.nullable, either.default) == ("text", True, None)

    with pytest.raises(CoreException, match="'either'"):
        _shape(mixed(False))


def test_a_sealed_field_is_text_with_no_default() -> None:
    sealed = _spec(encryption=FieldEncryption(encrypted=frozenset({"secret", "extra"})))
    columns = {column.name: column for column in _shape(sealed).columns}

    assert (columns["secret"].type, columns["secret"].default) == ("text", None)
    assert (columns["extra"].type, columns["extra"].default) == ("text", None)


def test_a_uniqueness_renders_as_a_required_unique_index_led_by_the_tenant() -> None:
    spec = _spec(guarantees=(UniqueTogether(fields=("name",)), UniqueTogether(fields=("note",))))
    indexes = _shape(spec, tenant_aware=True).indexes

    assert [(i.keys, i.unique, i.required, i.nulls_not_distinct) for i in indexes] == [
        ((TENANT_ID_FIELD, "name"), True, True, False),
        # A nullable key counts NULL as one value, as the guarantee does.
        ((TENANT_ID_FIELD, "note"), True, True, True),
    ]


def test_a_uniqueness_that_skips_nulls_is_a_plain_unique_index() -> None:
    spec = _spec(guarantees=(UniqueTogether(fields=("note",), skip_null=True),))

    assert [(i.keys, i.nulls_not_distinct) for i in _shape(spec).indexes] == [(("note",), False)]


def test_a_required_index_and_a_conflict_target_render_as_required_indexes() -> None:
    spec = _spec(
        required_indexes=(
            DocumentIndex(fields=("name",), unique=True),
            DocumentIndex(fields=("count",)),
        ),
    )
    indexes = _shape(spec, conflict_target=("name", "label")).indexes

    assert [(i.keys, i.unique, i.required) for i in indexes] == [
        (("name",), True, True),
        (("count",), False, True),
        (("name", "label"), True, True),
    ]


def test_a_uniqueness_and_a_required_index_over_one_key_render_once() -> None:
    spec = _spec(
        guarantees=(UniqueTogether(fields=("name",)),),
        required_indexes=(DocumentIndex(fields=("name",), unique=True),),
    )

    assert [i.keys for i in _shape(spec).indexes] == [("name",)]


def test_database_bookkeeping_prints_a_note_above_the_table() -> None:
    shape = _shape(bookkeeping_strategy="database")
    relation = BoundOwnedRelation(route="things", schema="app", name="things", shape=shape)

    assert len(shape.notes) == 1
    assert 'bookkeeping_strategy="database"' in render_sql([relation], version="0.25.0")


@pytest.mark.parametrize(
    "guarantee",
    [
        UniqueTogether(fields=("name",), where={"$values": {"active": True}}),
        UniqueTogether(fields=("name",), holds="commit"),
        NonOverlapping(key=("name",), period=("created_at", "last_update_at")),
        Reference(fields=("name",), references=("name",), spec="other", on_delete="restrict"),
    ],
    ids=["filtered", "at-commit", "non-overlapping", "reference"],
)
def test_a_guarantee_a_plain_unique_index_cannot_keep_is_refused(guarantee: Any) -> None:
    # Handed to the renderer directly: the spec's own checks are not what is under test.
    schema_spec = attrs.evolve(
        _schema_spec(_spec(), PostgresDocumentConfig(**_RW)), guarantees=(guarantee,)
    )

    with pytest.raises(CoreException) as caught:
        document_shape(schema_spec)

    assert caught.value.code == "postgres_document_render_refused"


def test_history_and_an_unmapped_annotation_are_refused() -> None:
    class _Odd(Document):
        either: int | str

    class _OddRead(ReadDocument):
        either: int | str

    odd: DocumentSpec[Any, Any, Any, Any] = DocumentSpec(
        name="odd", read=_OddRead, write={"domain": _Odd, "create_cmd": BaseDTO}
    )

    with pytest.raises(CoreException, match="'either'"):
        _shape(odd)

    with pytest.raises(CoreException) as caught:
        _shape(_spec(history_enabled=True), history=("app", "things_history"))

    assert caught.value.code == "postgres_document_render_refused"


# ....................... #


def _deps(**routes: PostgresDocumentConfig) -> PostgresDepsModule:
    return PostgresDepsModule(client=_CLIENT, rw_documents=routes)


def test_only_framework_routes_bound_read_write_are_owned() -> None:
    # A kit registers the application's own spec as KIT: its table stays the application's.
    registry = (
        SpecRegistry().register(_spec(), source=SpecSource.KIT).merge(framework_contributions())
    )
    deps = PostgresDepsModule(
        client=_CLIENT,
        rw_documents={
            "things": PostgresDocumentConfig(**_RW),
            "jobs": document_config("app", "jobs", tenant_aware=True),
        },
        ro_documents={"audit_events": PostgresReadOnlyDocumentConfig(read=("app", "audit"))},
    )

    relations = owned_relations(deps, specs=registry)

    assert [(r.route, r.name) for r in relations.documents] == [("jobs", "jobs")]
    assert relations.bound == ()
    assert [r.route for r in relations.tables] == ["jobs"]
    assert owned_relations(deps).documents == ()


async def test_a_tenant_scoped_document_route_is_skipped_then_bound_per_tenant() -> None:
    tenant = UUID(int=7)

    def per_tenant(tenant_id: UUID | None) -> tuple[str, str]:
        return ("app", f"jobs_{tenant_id.hex[:4] if tenant_id else 'none'}")

    deps = _deps(
        jobs=PostgresDocumentConfig(
            read=per_tenant, write=per_tenant, bookkeeping_strategy="application"
        )
    )
    registry = framework_registry()

    assert owned_relations(deps, specs=registry).skipped == ("jobs",)

    relations = await resolve_owned_relations(deps, tenants=[tenant], specs=registry)

    assert relations.skipped == ()
    assert [(r.route, r.name) for r in relations.documents] == [
        (f"jobs[{tenant}]", f"jobs_{tenant.hex[:4]}")
    ]


async def test_tenants_whose_resolver_names_one_table_bind_one_document_table() -> None:
    def one_table(tenant_id: UUID | None) -> tuple[str, str]:
        return ("app", "jobs")

    deps = _deps(
        jobs=PostgresDocumentConfig(
            read=one_table, write=one_table, bookkeeping_strategy="application"
        )
    )
    tenants = [UUID(int=1), UUID(int=2)]

    relations = await resolve_owned_relations(deps, tenants=tenants, specs=framework_registry())

    # The joined label names no single route; the table is still a document table.
    assert relations.bound == ()
    assert [(r.route, r.name) for r in relations.documents] == [
        (f"jobs[{tenants[0]}], jobs[{tenants[1]}]", "jobs")
    ]


async def test_without_tenants_a_tenant_scoped_document_route_is_skipped() -> None:
    def per_tenant(tenant_id: UUID | None) -> tuple[str, str]:
        return ("app", "jobs")

    deps = _deps(
        jobs=PostgresDocumentConfig(
            read=per_tenant, write=per_tenant, bookkeeping_strategy="application"
        ),
        audit_events=document_config("app", "audit_events"),
    )

    relations = await resolve_owned_relations(deps, tenants=[], specs=framework_registry())

    assert relations.skipped == ("jobs",)
    assert [r.route for r in relations.documents] == ["audit_events"]


def test_a_document_table_shared_with_a_store_table_is_refused() -> None:
    deps = PostgresDepsModule(
        client=_CLIENT,
        rw_documents={"jobs": document_config("app", "shared")},
        inboxes={"orders": PostgresInboxConfig(relation=("app", "shared"))},
    )

    with pytest.raises(CoreException) as caught:
        owned_relations(deps, specs=framework_registry())

    assert caught.value.code == "postgres_owned_relation_shared"


# ....................... #


def _index(
    keys: tuple[str, ...],
    *,
    name: str,
    unique: bool = True,
    primary: bool = False,
    nulls_not_distinct: bool = False,
) -> IndexShapeInfo:
    return IndexShapeInfo(
        name=name,
        schema="app",
        keys=keys,
        descending=tuple(False for _ in keys),
        nulls_first=tuple(False for _ in keys),
        included=0,
        method="btree",
        unique=unique,
        primary=primary,
        nulls_not_distinct=nulls_not_distinct,
        predicate=None,
        valid=True,
        backs_constraint=primary,
        owns_constraint=primary,
        referenced_by_foreign_key=False,
        default_opclasses=True,
        default_collations=True,
        immediate=True,
    )


def _role_bindings() -> BoundOwnedRelation:
    spec = next(s for s in _SPECS if s.name == "authz_principal_role_bindings")
    shape = document_shape(_schema_spec(spec))

    return BoundOwnedRelation(route=str(spec.name), schema="app", name="bindings", shape=shape)


def _role_catalog(*, nulls_not_distinct: bool) -> CatalogRelation:
    relation = _role_bindings()
    keys = ("principal_id", "role_id", "scope_tenant_id")

    return CatalogRelation(
        kind="table",
        columns=_catalog(relation.shape),
        primary_key=("id",),
        indexes=(
            _index(("id",), name="pkey", primary=True),
            _index(keys, name="bindings_key", nulls_not_distinct=nulls_not_distinct),
        ),
    )


def test_a_table_created_from_the_render_has_nothing_to_apply() -> None:
    delta = diff_relation(_role_bindings(), _role_catalog(nulls_not_distinct=True))

    assert delta.blocks == ()


def test_a_unique_index_that_lets_nulls_repeat_does_not_keep_the_declared_one() -> None:
    delta = diff_relation(_role_bindings(), _role_catalog(nulls_not_distinct=False))
    statements = [block.statement for block in delta.blocks if block.statement is not None]

    assert statements == [
        (
            "CREATE UNIQUE INDEX CONCURRENTLY ON app.bindings "
            "(principal_id, role_id, scope_tenant_id) NULLS NOT DISTINCT;"
        )
    ]
    # The declared index itself counts NULL as one value: nothing reads that as a conflict.
    assert not [block for block in delta.blocks if block.finding == "conflict_target"]


@pytest.mark.parametrize("tenant_aware", [False, True])
@pytest.mark.parametrize("spec", _SPECS, ids=_IDS)
def test_every_framework_table_created_from_its_render_diffs_clean(
    spec: DocumentSpec[Any, Any, Any, Any], tenant_aware: bool
) -> None:
    name = str(spec.name)
    shape = document_shape(
        _schema_spec(spec, document_config("app", name, tenant_aware=tenant_aware))
    )
    relation = BoundOwnedRelation(route=name, schema="app", name=name, shape=shape)
    catalog = CatalogRelation(
        kind="table",
        columns=_catalog(shape),
        primary_key=("id",),
        indexes=(
            _index(("id",), name="pkey", primary=True),
            *(
                _index(
                    index.keys,
                    name=f"idx_{position}",
                    unique=index.unique,
                    nulls_not_distinct=index.nulls_not_distinct,
                )
                for position, index in enumerate(shape.indexes)
            ),
        ),
    )

    assert diff_relation(relation, catalog).blocks == ()


@pytest.mark.parametrize(
    ("key", "name"),
    [
        ('"order"', "order"),
        ('"a""b"', 'a"b'),
        ("status", "status"),
        ("created_at DESC", "created_at DESC"),
    ],
)
def test_a_quoted_key_reads_back_as_its_name(key: str, name: str) -> None:
    assert unquote_ident(key) == name



@pytest.mark.parametrize(
    ("key", "column"),
    [("name DESC", "name"), ('"order" desc nulls last', "order"), ("lower(name)", "lower(name)")],
)
def test_a_declared_key_names_its_column_without_its_order(key: str, column: str) -> None:
    assert key_column(key) == column


def test_a_unique_index_counting_nulls_breaks_a_descending_nullable_target() -> None:
    # The target's key carries a direction; its column is still nullable, so a NULLS NOT
    # DISTINCT index over it refuses the second row without a name the adapter writes.
    shape = OwnedRelationShape(
        owner="Store",
        since="0.25.0",
        columns=(
            OwnedColumn(name="id", type="uuid", nullable=False),
            OwnedColumn(name="name", type="text", nullable=True),
        ),
        primary_key=("id",),
        indexes=(OwnedIndex(keys=("name DESC",), unique=True, required=True, comment="c"),),
    )
    relation = BoundOwnedRelation(route="x", schema="app", name="x", shape=shape)
    descending = attrs.evolve(
        _index(("name",), name="kept"), descending=(True,), nulls_first=(True,)
    )
    catalog = CatalogRelation(
        kind="table",
        columns=_catalog(shape),
        primary_key=("id",),
        indexes=(
            _index(("id",), name="pkey", primary=True),
            descending,
            _index(("name",), name="counts_nulls", nulls_not_distinct=True),
        ),
    )

    findings = [block.finding for block in diff_relation(relation, catalog).blocks]

    assert findings == ["conflict_target"]
