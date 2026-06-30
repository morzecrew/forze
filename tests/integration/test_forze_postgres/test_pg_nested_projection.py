"""Integration tests for dot-path field projection on JSONB (nested Pydantic fields).

Projecting a dotted path selects the whole JSONB root column and reshapes the requested
leaves out of it in Python, so ``meta.score`` returns the nested ``{"meta": {"score": ...}}``
shape — identical to the mock oracle and Mongo.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.scenarios.document_nested_filters import (
    NestedFilterMeta as Meta,
    NestedFilterRowCreate as RowCreate,
    NestedFilterRowDoc as RowDoc,
    NestedFilterRowRead as RowRead,
    NestedFilterRowUpdate as RowUpdate,
)


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: doc,
                DocumentCommandDepKey: doc,
            }
        )
    )


def _spec() -> DocumentSpec:
    return DocumentSpec(
        name="nested_pg_proj_ns",
        read=RowRead,
        write={"domain": RowDoc, "create_cmd": RowCreate, "update_cmd": RowUpdate},
    )


async def _create_table(pg_client: PostgresClient, t: str) -> None:
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            meta jsonb NOT NULL
        );
        """
    )


@pytest.mark.asyncio
async def test_project_nested_jsonb_leaf_reshapes(pg_client: PostgresClient) -> None:
    t = f"nest_proj_{uuid4().hex[:12]}"
    await _create_table(pg_client, t)
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="a", meta=Meta(score=10, tag="x")))

    out = await query.project({"$values": {"title": "a"}}, ["meta.score"])

    assert out == {"meta": {"score": 10}}


@pytest.mark.asyncio
async def test_project_sibling_leaves_merge_and_mix_with_top(
    pg_client: PostgresClient,
) -> None:
    t = f"nest_proj_{uuid4().hex[:12]}"
    await _create_table(pg_client, t)
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="a", meta=Meta(score=10, tag="x")))

    out = await query.project(
        {"$values": {"title": "a"}}, ["title", "meta.score", "meta.tag"]
    )

    assert out == {"title": "a", "meta": {"score": 10, "tag": "x"}}


@pytest.mark.asyncio
async def test_project_many_nested_over_rows(pg_client: PostgresClient) -> None:
    t = f"nest_proj_{uuid4().hex[:12]}"
    await _create_table(pg_client, t)
    ctx = _ctx(pg_client, t)
    spec = _spec()
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="a", meta=Meta(score=10)))
    await cmd.create(RowCreate(title="b", meta=Meta(score=20)))

    page = await query.project_many(["meta.score"], sorts={"meta.score": "asc"})

    assert list(page.hits) == [{"meta": {"score": 10}}, {"meta": {"score": 20}}]
