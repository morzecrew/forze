"""Unit tests for Postgres catalog warmup and document schema validation."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.execution import ExecutionContext, Deps
from forze.base.errors import CoreError

from forze_postgres.execution.catalog_warmup import (
    postgres_catalog_warmup_lifecycle_step,
    warm_postgres_catalog,
)
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.document_schema import (
    postgres_document_schema_validation_lifecycle_step,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.validate_schema import (
    PostgresDocumentSchemaSpec,
    validate_postgres_document_schemas,
)


@pytest.mark.asyncio
async def test_warm_postgres_catalog_single_search() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(return_value={})
    intro.get_index_info = AsyncMock(
        return_value=MagicMock(
            schema="public",
            name="ix",
            amname="gin",
            engine="fts",
            indexdef="",
            expr="x",
            columns=(),
            has_tsvector_col=True,
        ),
    )

    ctx = ExecutionContext(
        deps=Deps.plain({PostgresIntrospectorDepKey: intro}),
    )

    await warm_postgres_catalog(
        ctx,
        searches={
            "s": {
                "engine": "fts",
                "index": ("public", "ix"),
                "read": ("public", "v"),
                "fts_groups": {"A": ("title",)},
            },
        },
    )

    intro.get_column_types.assert_awaited_once_with(schema="public", relation="v")
    intro.get_index_info.assert_awaited_once_with(index="ix", schema="public")


@pytest.mark.asyncio
async def test_warm_postgres_catalog_vector_skips_index_info() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(return_value={})

    ctx = ExecutionContext(
        deps=Deps.plain({PostgresIntrospectorDepKey: intro}),
    )

    await warm_postgres_catalog(
        ctx,
        searches={
            "s": {
                "engine": "vector",
                "index": ("public", "ix"),
                "read": ("public", "v"),
                "heap": ("public", "h"),
                "vector_column": "emb",
                "embedding_dimensions": 3,
                "embeddings_name": "e",
            },
        },
    )

    assert intro.get_column_types.await_count == 2
    intro.get_index_info.assert_not_called()


@pytest.mark.asyncio
async def test_warm_postgres_catalog_skips_on_partition_error() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(
        side_effect=CoreError(
            "partition",
            code="introspection_partition_required",
        ),
    )

    ctx = ExecutionContext(
        deps=Deps.plain({PostgresIntrospectorDepKey: intro}),
    )

    await warm_postgres_catalog(
        ctx,
        searches={
            "s": {
                "engine": "pgroonga",
                "index": ("public", "ix"),
                "read": ("public", "v"),
            },
        },
    )


@pytest.mark.asyncio
async def test_catalog_warmup_lifecycle_step_builds() -> None:
    step = postgres_catalog_warmup_lifecycle_step(searches=None)
    assert step.name == "postgres_catalog_warmup"


class _Read(BaseModel):
    a: int
    b: str = "x"


@pytest.mark.asyncio
async def test_validate_postgres_document_schemas_read_only() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(
        return_value={
            "a": MagicMock(),
            "b": MagicMock(),
        },
    )

    await validate_postgres_document_schemas(
        intro,
        [
            PostgresDocumentSchemaSpec(
                name="doc",
                read_model=_Read,
                read_relation=("public", "r"),
            ),
        ],
    )

    intro.get_column_types.assert_awaited_once_with(schema="public", relation="r")


@pytest.mark.asyncio
async def test_validate_postgres_document_schemas_missing_column() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(return_value={"a": MagicMock()})

    with pytest.raises(CoreError, match="missing columns"):
        await validate_postgres_document_schemas(
            intro,
            [
                PostgresDocumentSchemaSpec(
                    name="doc",
                    read_model=_Read,
                    read_relation=("public", "r"),
                ),
            ],
        )


@pytest.mark.asyncio
async def test_schema_validation_lifecycle_step_builds() -> None:
    step = postgres_document_schema_validation_lifecycle_step(specs=())
    assert step.name == "postgres_document_schema_validate"
