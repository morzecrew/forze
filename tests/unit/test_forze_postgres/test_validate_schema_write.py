"""Extended validate_schema coverage for write and history relations."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from pydantic import BaseModel, computed_field

from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.base.exceptions import CoreException
from forze_postgres.kernel.catalog.introspect import (
    PostgresIntrospector,
    PostgresType,
    UniqueIndexInfo,
)
from forze_postgres.kernel.catalog.validation.validate_schema import (
    PostgresDocumentSchemaSpec,
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
