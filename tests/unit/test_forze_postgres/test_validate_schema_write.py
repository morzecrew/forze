"""Extended validate_schema coverage for write and history relations."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel, computed_field

from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.base.exceptions import CoreException
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector, PostgresType
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
