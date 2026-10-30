"""Extended validate_schema coverage for write and history relations."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from pydantic import BaseModel, computed_field

from forze.application.contracts.guarantees import Reference, UniqueTogether
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.base.exceptions import CoreException
from forze_postgres.kernel.catalog.introspect import (
    ForeignKeyInfo,
    PostgresIntrospector,
    PostgresType,
    UniqueIndexInfo,
)
from forze_postgres.kernel.catalog.validation.validate_schema import (
    PostgresDocumentSchemaSpec,
    _require_reference_mechanisms,  # pyright: ignore[reportPrivateUsage]
    validate_postgres_document_schemas,
)


class _Read(BaseModel):
    id: str
    name: str


class _Domain(BaseModel):
    id: str
    name: str
    rev: int = 1


class _Create(BaseModel):
    name: str


class _Update(BaseModel):
    name: str | None = None


def _col(name: str) -> PostgresType:
    return PostgresType(base="text", is_array=False, not_null=True)


@pytest.mark.asyncio
async def test_validate_write_and_history_relations() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    read_cols = {"id": _col("id"), "name": _col("name")}
    write_cols = {
        "id": _col("id"),
        "name": _col("name"),
        "rev": PostgresType(base="int4", is_array=False, not_null=True),
    }
    async def _types(*, schema: str, relation: str) -> dict[str, PostgresType]:
        if relation == "write_tbl":
            return write_cols
        return read_cols

    intro.get_column_types = AsyncMock(side_effect=_types)

    await validate_postgres_document_schemas(
        intro,
        [
            PostgresDocumentSchemaSpec(
                name="doc",
                read_model=_Read,
                read_relation=("public", "read_v"),
                write_domain_model=_Domain,
                write_create_model=_Create,
                write_update_model=_Update,
                write_relation=("public", "write_tbl"),
            ),
        ],
    )
    assert intro.get_column_types.await_count >= 2


class _DomainWithComputed(BaseModel):
    id: str
    name: str
    rev: int = 1

    @computed_field
    @property
    def label(self) -> str:
        return f"name:{self.name}"


@pytest.mark.asyncio
async def test_validate_write_domain_computed_field_does_not_require_column() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    cols = {
        "id": _col("id"),
        "name": _col("name"),
        "rev": PostgresType(base="int4", is_array=False, not_null=True),
    }
    intro.get_column_types = AsyncMock(return_value=cols)

    await validate_postgres_document_schemas(
        intro,
        [
            PostgresDocumentSchemaSpec(
                name="doc",
                read_model=_Read,
                read_relation=("public", "t"),
                write_domain_model=_DomainWithComputed,
                write_create_model=_Create,
                write_relation=("public", "t"),
            ),
        ],
    )


@pytest.mark.asyncio
async def test_validate_materialized_field_requires_column() -> None:
    # A materialized computed field IS persisted, so its column must exist —
    # a missing column fails at startup rather than on the first write.
    intro = MagicMock(spec=PostgresIntrospector)
    cols = {
        "id": _col("id"),
        "name": _col("name"),
        "rev": PostgresType(base="int4", is_array=False, not_null=True),
    }
    intro.get_column_types = AsyncMock(return_value=cols)  # no 'label' column

    with pytest.raises(CoreException, match="label"):
        await validate_postgres_document_schemas(
            intro,
            [
                PostgresDocumentSchemaSpec(
                    name="doc",
                    read_model=_DomainWithComputed,
                    read_relation=("public", "t"),
                    write_domain_model=_DomainWithComputed,
                    write_create_model=_Create,
                    write_relation=("public", "t"),
                    materialized=frozenset({"label"}),
                ),
            ],
        )


@pytest.mark.asyncio
async def test_validate_materialized_field_passes_when_column_present() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    cols = {
        "id": _col("id"),
        "name": _col("name"),
        "rev": PostgresType(base="int4", is_array=False, not_null=True),
        "label": _col("label"),
    }
    intro.get_column_types = AsyncMock(return_value=cols)

    await validate_postgres_document_schemas(
        intro,
        [
            PostgresDocumentSchemaSpec(
                name="doc",
                read_model=_DomainWithComputed,
                read_relation=("public", "t"),
                write_domain_model=_DomainWithComputed,
                write_create_model=_Create,
                write_relation=("public", "t"),
                materialized=frozenset({"label"}),
            ),
        ],
    )


class _ReadWithExtra(BaseModel):
    id: str
    name: str
    nickname: str = "anon"  # declared in code, not (yet) a column


@pytest.mark.asyncio
async def test_validate_read_requires_undeclared_field_by_default() -> None:
    # Strict (default): a read field with no column fails startup validation.
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(
        return_value={"id": _col("id"), "name": _col("name")},  # no 'nickname'
    )

    with pytest.raises(CoreException, match="nickname"):
        await validate_postgres_document_schemas(
            intro,
            [
                PostgresDocumentSchemaSpec(
                    name="doc",
                    read_model=_ReadWithExtra,
                    read_relation=("public", "t"),
                ),
            ],
        )


@pytest.mark.asyncio
async def test_validate_read_tolerates_lenient_field_without_column() -> None:
    # read_omit_fields (wired from DocumentSpec.lenient_read_fields): the missing
    # column is tolerated and not type/nullability-checked.
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(
        return_value={"id": _col("id"), "name": _col("name")},  # no 'nickname'
    )

    await validate_postgres_document_schemas(
        intro,
        [
            PostgresDocumentSchemaSpec(
                name="doc",
                read_model=_ReadWithExtra,
                read_relation=("public", "t"),
                read_omit_fields=frozenset({"nickname"}),
            ),
        ],
    )


@pytest.mark.asyncio
async def test_validate_tenant_aware_write_requires_tenant_column() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(
        return_value={
            "id": _col("id"),
            "name": _col("name"),
            "rev": PostgresType(base="int4", is_array=False, not_null=True),
        },
    )

    with pytest.raises(CoreException, match=TENANT_ID_FIELD):
        await validate_postgres_document_schemas(
            intro,
            [
                PostgresDocumentSchemaSpec(
                    name="doc",
                    read_model=_Read,
                    read_relation=("public", "t"),
                    tenant_aware=True,
                    write_domain_model=_Domain,
                    write_create_model=_Create,
                    write_relation=("public", "t"),
                ),
            ],
        )


# ....................... #


class _ScopedDomain(_Domain):
    tenant_id: UUID | None = None


def _spec_over(domain: type[BaseModel], **kwargs: Any) -> PostgresDocumentSchemaSpec:
    return PostgresDocumentSchemaSpec(
        name="doc",
        read_model=_Read,
        read_relation=("public", "t"),
        write_domain_model=domain,
        write_create_model=_Create,
        write_relation=("public", "t"),
        **kwargs,
    )


_NAME_KEY = UniqueIndexInfo(
    columns=frozenset({"name"}), predicate=None, nulls_not_distinct=False, name="t_name_key"
)


async def _warnings(
    spec: PostgresDocumentSchemaSpec,
    *,
    tenant_column: bool,
    indexes: tuple[UniqueIndexInfo, ...] = (_NAME_KEY,),
) -> list[str]:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_primary_key_columns = AsyncMock(return_value=["id"])
    columns = {
        "id": _col("id"),
        "name": _col("name"),
        "rev": PostgresType(base="int4", is_array=False, not_null=True),
    }

    if tenant_column:
        columns[TENANT_ID_FIELD] = PostgresType(base="uuid", is_array=False, not_null=False)

    intro.get_column_types = AsyncMock(return_value=columns)
    intro.unique_indexes = AsyncMock(return_value=indexes)
    intro.exclusion_constraints = AsyncMock(return_value=())

    with patch("forze_postgres.kernel.catalog.validation.validate_schema.logger") as log:
        await validate_postgres_document_schemas(intro, [spec])

    return [call.args[0] % call.args[1:] for call in log.warning.call_args_list]


@pytest.mark.asyncio
async def test_an_undeclared_index_is_reported_only_for_a_spec_the_app_owns() -> None:
    reported = await _warnings(_spec_over(_Domain), tenant_column=False)

    assert len(reported) == 1
    assert "t_name_key" in reported[0]
    assert await _warnings(_spec_over(_Domain, warn_undeclared=False), tenant_column=False) == []


@pytest.mark.asyncio
async def test_a_tenant_column_the_model_maps_is_not_an_unused_one() -> None:
    # A session minted for one tenant stores the tenant as data on a tenant-unaware table.
    mapped = _spec_over(_ScopedDomain, warn_undeclared=False)

    assert await _warnings(mapped, tenant_column=True) == []

    reported = await _warnings(_spec_over(_Domain, warn_undeclared=False), tenant_column=True)

    assert len(reported) == 1
    assert "tenant_aware=False" in reported[0]


@pytest.mark.asyncio
async def test_a_conflict_target_over_other_columns_is_reported() -> None:
    # Upserts resolve on name, and a duplicate create is refused there: the mock knows neither.
    reported = await _warnings(_spec_over(_Domain, conflict_target=("name",)), tenant_column=False)

    assert len(reported) == 1
    assert "t_name_key" in reported[0]


@pytest.mark.asyncio
async def test_an_index_over_the_primary_key_is_not_reported() -> None:
    # Unique wherever the key is: it refuses no write the key would not.
    over_key = UniqueIndexInfo(
        columns=frozenset({"id", "name"}),
        predicate=None,
        nulls_not_distinct=False,
        name="t_id_name_key",
    )

    assert await _warnings(_spec_over(_Domain), tenant_column=False, indexes=(over_key,)) == []


@pytest.mark.asyncio
async def test_the_suggestion_carries_skip_null_and_holds_commit() -> None:
    # Over a nullable column, and checked at commit: the declaration has to say both.
    nullable_deferred = UniqueIndexInfo(
        columns=frozenset({TENANT_ID_FIELD}),
        predicate=None,
        nulls_not_distinct=False,
        name="t_tenant_key",
        initially_deferred=True,
    )

    reported = await _warnings(
        _spec_over(_ScopedDomain), tenant_column=True, indexes=(nullable_deferred,)
    )

    assert len(reported) == 1
    assert "skip_null=True" in reported[0]
    assert 'holds="commit"' in reported[0]


# ....................... #


def _nullable() -> PostgresType:
    return PostgresType(base="text", is_array=False, not_null=False)


def _read_only(name: str, relation: str) -> PostgresDocumentSchemaSpec:
    return PostgresDocumentSchemaSpec(
        name=name, read_model=_Read, read_relation=("public", relation)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["view", "materialized_view"])
async def test_a_view_column_read_as_nullable_does_not_refuse_a_required_field(kind: str) -> None:
    # Postgres records NOT NULL on tables only: every view column reads nullable.
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(return_value={"id": _nullable(), "name": _nullable()})
    intro.get_relation = AsyncMock(return_value=kind)

    await validate_postgres_document_schemas(intro, [_read_only("doc", "v")])


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["table", "partitioned_table"])
async def test_a_nullable_table_column_still_refuses_a_required_field(kind: str) -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(return_value={"id": _col("id"), "name": _nullable()})
    intro.get_relation = AsyncMock(return_value=kind)

    with pytest.raises(CoreException, match="nullable column") as caught:
        await validate_postgres_document_schemas(intro, [_read_only("doc", "t")])

    assert caught.value.code == "postgres_schema_validation_failed"


@pytest.mark.asyncio
async def test_every_failing_document_is_reported_in_one_error() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    columns = {
        "t_ok": {"id": _col("id"), "name": _col("name")},
        "t_nullable": {"id": _col("id"), "name": _nullable()},
        "t_missing": {"id": _col("id")},
    }

    async def _types(*, schema: str, relation: str) -> dict[str, PostgresType]:
        return columns[relation]

    intro.get_column_types = AsyncMock(side_effect=_types)
    intro.get_relation = AsyncMock(return_value="table")
    specs = [
        _read_only("nullable", "t_nullable"),
        _read_only("ok", "t_ok"),
        _read_only("missing", "t_missing"),
    ]

    with pytest.raises(CoreException) as caught:
        await validate_postgres_document_schemas(intro, specs)

    assert caught.value.code == "postgres_schema_documents_refused"
    assert caught.value.details is not None
    failures = caught.value.details["failures"]
    assert [f["document"] for f in failures] == ["nullable", "missing"]
    assert failures[0]["code"] == "postgres_schema_validation_failed"
    assert failures[0]["details"]["field"] == "name"
    assert "missing columns" in failures[1]["summary"]
    assert "nullable: " in caught.value.summary
    assert "missing: " in caught.value.summary


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        CoreException.infrastructure("Database connection error."),
        CoreException.internal("No tenant.", code="introspection_partition_required"),
    ],
)
async def test_an_error_of_no_one_document_stops_the_validation_at_once(
    error: CoreException,
) -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(side_effect=error)

    with pytest.raises(CoreException) as caught:
        await validate_postgres_document_schemas(
            intro, [_read_only("a", "t"), _read_only("b", "t")]
        )

    assert caught.value is error
    assert intro.get_column_types.await_count == 1


def _stop_error(kind: str) -> CoreException:
    if kind == "partition":
        return CoreException.internal("No tenant.", code="introspection_partition_required")

    return CoreException.infrastructure("Database connection error.")


@pytest.mark.asyncio
@pytest.mark.parametrize("stop", ["partition", "infrastructure"])
@pytest.mark.parametrize("refused", [1, 2])
async def test_refusals_found_before_an_error_of_no_one_document_are_raised(
    stop: str,
    refused: int,
) -> None:
    # The hook skips on a missing partition, so the refusal must not be lost to it.
    error = _stop_error(stop)
    intro = MagicMock(spec=PostgresIntrospector)

    async def _types(*, schema: str, relation: str) -> dict[str, PostgresType]:
        if relation == "t_nullable":
            return {"id": _col("id"), "name": _nullable()}

        raise error

    intro.get_column_types = AsyncMock(side_effect=_types)
    intro.get_relation = AsyncMock(return_value="table")
    specs = [_read_only(f"nullable{i}", "t_nullable") for i in range(refused)]
    specs += [_read_only("stops", "t"), _read_only("never", "t_nullable")]

    with pytest.raises(CoreException) as caught:
        await validate_postgres_document_schemas(intro, specs)

    assert caught.value.__cause__ is error
    assert caught.value.details is not None

    if refused == 1:
        assert caught.value.code == "postgres_schema_validation_failed"
        assert caught.value.details["field"] == "name"

    else:
        assert caught.value.code == "postgres_schema_documents_refused"
        assert [f["document"] for f in caught.value.details["failures"]] == [
            "nullable0",
            "nullable1",
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize("side", ["write", "history"])
async def test_a_write_or_history_view_keeps_the_nullability_rule(side: str) -> None:
    # Only a read relation is let off: a write through a view needs every required value.
    intro = MagicMock(spec=PostgresIntrospector)
    not_null = {
        base: PostgresType(base=base, is_array=False, not_null=True)
        for base in ("uuid", "int4", "timestamptz", "jsonb")
    }
    columns = {
        "v": {"id": _nullable(), "name": _nullable()},
        "w": {"id": _col("id"), "name": _nullable(), "rev": not_null["int4"]},
        "h": {
            "source": _nullable(),
            "id": not_null["uuid"],
            "rev": not_null["int4"],
            "created_at": not_null["timestamptz"],
            "data": not_null["jsonb"],
        },
    }

    async def _types(*, schema: str, relation: str) -> dict[str, PostgresType]:
        return columns[relation]

    intro.get_column_types = AsyncMock(side_effect=_types)
    intro.get_relation = AsyncMock(return_value="view")
    relations: dict[str, Any] = (
        {
            "write_domain_model": _Domain,
            "write_create_model": _Create,
            "write_relation": ("public", "w"),
        }
        if side == "write"
        else {"history_enabled": True, "history_relation": ("public", "h")}
    )
    spec = PostgresDocumentSchemaSpec(
        name="doc", read_model=_Read, read_relation=("public", "v"), **relations
    )

    with pytest.raises(CoreException, match="nullable column") as caught:
        await validate_postgres_document_schemas(intro, [spec])

    assert caught.value.details is not None
    assert caught.value.details["label"].startswith(f"doc {side}")


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [None, "w", "h"])
async def test_a_tenant_aware_write_and_history_check_their_tenant_columns(
    missing: str | None,
) -> None:
    tenant = PostgresType(base="uuid", is_array=False, not_null=True)
    not_null = {
        base: PostgresType(base=base, is_array=False, not_null=True)
        for base in ("uuid", "int4", "timestamptz", "jsonb")
    }
    columns: dict[str, dict[str, PostgresType]] = {
        "v": {"id": _col("id"), "name": _col("name")},
        "w": {"id": _col("id"), "name": _col("name"), "rev": not_null["int4"]},
        "h": {
            "source": _col("source"),
            "id": not_null["uuid"],
            "rev": not_null["int4"],
            "created_at": not_null["timestamptz"],
            "data": not_null["jsonb"],
        },
    }

    for relation, cols in columns.items():
        if relation != missing:
            cols[TENANT_ID_FIELD] = tenant

    async def _types(*, schema: str, relation: str) -> dict[str, PostgresType]:
        return columns[relation]

    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(side_effect=_types)
    intro.get_relation = AsyncMock(return_value="table")
    spec = PostgresDocumentSchemaSpec(
        name="doc",
        read_model=_Read,
        read_relation=("public", "v"),
        tenant_aware=True,
        write_domain_model=_Domain,
        write_create_model=_Create,
        write_relation=("public", "w"),
        history_enabled=True,
        history_relation=("public", "h"),
    )

    if missing is None:
        await validate_postgres_document_schemas(intro, [spec])

        return

    with pytest.raises(CoreException, match=TENANT_ID_FIELD) as caught:
        await validate_postgres_document_schemas(intro, [spec])

    side = "write" if missing == "w" else "history"
    assert caught.value.details is not None
    assert caught.value.details["document"] == f"doc {side}"


# ....................... #

_ONE_PER_NAME = UniqueTogether(fields=("name",))


async def _validate_tenant_aware(
    *guarantees: UniqueTogether,
    indexes: tuple[UniqueIndexInfo, ...],
) -> list[str]:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_primary_key_columns = AsyncMock(return_value=["id"])
    intro.get_column_types = AsyncMock(
        return_value={
            "id": _col("id"),
            "name": _col("name"),
            "rev": PostgresType(base="int4", is_array=False, not_null=True),
            TENANT_ID_FIELD: PostgresType(base="uuid", is_array=False, not_null=True),
        }
    )
    intro.unique_indexes = AsyncMock(return_value=indexes)
    intro.exclusion_constraints = AsyncMock(return_value=())
    spec = _spec_over(_Domain, tenant_aware=True, guarantees=guarantees)

    with patch("forze_postgres.kernel.catalog.validation.validate_schema.logger") as log:
        await validate_postgres_document_schemas(intro, [spec])

    return [call.args[0] % call.args[1:] for call in log.warning.call_args_list]


def _index(*columns: str) -> UniqueIndexInfo:
    return UniqueIndexInfo(
        columns=frozenset(columns), predicate=None, nulls_not_distinct=False, name="t_key"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("columns", [("name",), (TENANT_ID_FIELD, "name")])
async def test_a_tenant_aware_guarantee_is_kept_by_an_index_per_tenant_or_over_all(
    columns: tuple[str, ...],
) -> None:
    # Per tenant is what the in-memory store keeps; across tenants is stricter, as before.
    assert await _validate_tenant_aware(_ONE_PER_NAME, indexes=(_index(*columns),)) == []


@pytest.mark.asyncio
async def test_a_tenant_aware_refusal_names_the_index_per_tenant() -> None:
    with pytest.raises(CoreException) as caught:
        await _validate_tenant_aware(_ONE_PER_NAME, indexes=())

    assert f"ADD UNIQUE ({TENANT_ID_FIELD}, name)" in str(caught.value)


@pytest.mark.asyncio
async def test_a_tenant_aware_suggestion_leaves_the_tenant_column_out() -> None:
    # The spec cannot name the column, and its guarantee is kept per tenant without it.
    reported = await _validate_tenant_aware(indexes=(_index(TENANT_ID_FIELD, "name"),))

    assert len(reported) == 1
    assert "UniqueTogether(fields=('name',))" in reported[0]


@pytest.mark.asyncio
async def test_set_null_beside_the_tenant_column_before_postgres_15_names_no_column_list() -> None:
    # `ON DELETE SET NULL (field)` is a syntax error there, so the hint is the single-column key.
    intro = MagicMock(spec=PostgresIntrospector)
    intro.foreign_keys = AsyncMock(
        return_value=(
            ForeignKeyInfo(
                name="t_stage",
                columns=((TENANT_ID_FIELD, TENANT_ID_FIELD), ("stage_id", "id")),
                referenced=("public", "stages"),
                on_delete="n",
                set_null_columns=None,
            ),
        )
    )
    intro.get_column_types = AsyncMock(return_value={"stage_id": _nullable()})
    spec = _spec_over(
        _Domain,
        guarantees=(Reference(field="stage_id", spec="stages", on_delete="set_null"),),
        bound_relations={"stages": ("public", "stages")},
    )

    with pytest.raises(CoreException) as caught:
        await _require_reference_mechanisms(intro, spec)

    summary = caught.value.summary
    assert "needs Postgres 15 or later" in summary
    assert "SET NULL (stage_id)" not in summary
    assert "ALTER TABLE public.t DROP CONSTRAINT t_stage;" in summary
    assert "ADD FOREIGN KEY (stage_id) REFERENCES public.stages (id) ON DELETE SET NULL;" in summary
