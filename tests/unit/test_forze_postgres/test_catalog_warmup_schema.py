"""Unit tests for Postgres catalog warmup and document schema validation."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.base.exceptions import CoreException, exc
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.catalog_warmup import (
    postgres_catalog_warmup_lifecycle_step,
    warm_postgres_catalog,
)
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.document_schema import (
    postgres_document_schema_validation_lifecycle_step,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector, PostgresType
from forze_postgres.kernel.catalog.validation.validate_schema import (
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
        side_effect=exc.internal(
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
async def test_warm_postgres_catalog_hub_members() -> None:
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
        hub_searches={
            "hub": {
                "hub": ("public", "hub_v"),
                "members": {
                    "m": {
                        "engine": "fts",
                        "index": ("public", "ix"),
                        "read": ("public", "v"),
                        "fts_groups": {"A": ("title",)},
                    },
                },
            },
        },
    )

    intro.get_column_types.assert_any_await(schema="public", relation="hub_v")
    assert intro.get_column_types.await_count >= 2


@pytest.mark.asyncio
async def test_warm_postgres_catalog_federated_embedded_hub() -> None:
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
        federated_searches={
            "fed": {
                "members": {
                    "hub_member": {
                        "hub": ("public", "hub_v"),
                        "members": {
                            "m": {
                                "engine": "fts",
                                "index": ("public", "ix"),
                                "read": ("public", "v"),
                                "fts_groups": {"A": ("title",)},
                            },
                        },
                    },
                },
            },
        },
    )

    assert intro.get_column_types.await_count >= 2


@pytest.mark.asyncio
async def test_catalog_warmup_lifecycle_step_runs_hook() -> None:
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
    step = postgres_catalog_warmup_lifecycle_step(
        searches={
            "s": {
                "engine": "fts",
                "index": ("public", "ix"),
                "read": ("public", "v"),
                "fts_groups": {"A": ("title",)},
            },
        },
    )
    assert step.id == "postgres_catalog_warmup"
    await step.startup(ctx)
    intro.get_column_types.assert_awaited()


class _Read(BaseModel):
    a: int
    b: str = "x"


@pytest.mark.asyncio
async def test_validate_postgres_document_schemas_read_only() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(
        return_value={
            "a": PostgresType(base="int4", is_array=False, not_null=True),
            "b": PostgresType(base="text", is_array=False, not_null=False),
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

    intro.get_column_types.assert_awaited()
    assert intro.get_column_types.await_count == 2
    intro.get_column_types.assert_any_await(schema="public", relation="r")


@pytest.mark.asyncio
async def test_validate_postgres_document_schemas_missing_column() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(
        return_value={"a": PostgresType(base="int4", is_array=False, not_null=True)},
    )

    with pytest.raises(CoreException, match="missing columns"):
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
    assert step.id == "postgres_document_schema_validate"
