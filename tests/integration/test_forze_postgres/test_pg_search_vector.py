"""Integration tests for pgvector :class:`PostgresVectorSearchAdapterV2` (KNN + filters)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderDepKey, EmbeddingsSpec
from forze.application.contracts.query import QueryFilterExpression
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.adapters.search import PostgresVectorSearchAdapterV2
from forze_postgres.adapters.search._vector_sql import vector_param_literal
from forze_postgres.execution.deps.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient
from forze_mock import MockHashEmbeddingsProvider

# ----------------------- #


class VecDoc(BaseModel):
    id: UUID
    label: str


def _embeddings_factory(
    _ctx: ExecutionContext,
    spec: EmbeddingsSpec,
) -> MockHashEmbeddingsProvider:
    return MockHashEmbeddingsProvider(dimensions=spec.dimensions)


def _vector_search_context(
    pg_client: PostgresClient,
    *,
    table: str,
    index_name: str,
    vector_distance: str = "l2",
) -> ExecutionContext:
    return ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config={
                        "index": ("public", index_name),
                        "read": ("public", table),
                        "heap": ("public", table),
                        "engine": "vector",
                        "vector_column": "emb",
                        "vector_distance": vector_distance,
                        "embeddings_name": "vec_test",
                        "embedding_dimensions": 3,
                    }
                ),
                EmbeddingsProviderDepKey: _embeddings_factory,
            }
        )
    )


async def _ensure_vector_extension(pg_client: PostgresClient) -> None:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS vector")


# ....................... #


@pytest.mark.asyncio
async def test_vector_l2_knn_orders_by_nearest(pgvector_client: PostgresClient) -> None:
    """L2: query embedding is closest to the row built from the same label string (mock)."""
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_l2_{suffix}"
    index_name = f"idx_vec_l2_{suffix}"
    a_id, b_id = uuid4(), uuid4()

    await pgvector_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            label text NOT NULL,
            emb vector(3) NOT NULL
        );
        """
    )

    prov = MockHashEmbeddingsProvider(dimensions=3)
    v_alpha = await prov.embed_one("alpha")
    v_beta = await prov.embed_one("beta")
    la = vector_param_literal(v_alpha)
    lb = vector_param_literal(v_beta)

    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb)
        VALUES (%(a)s, 'a', '{la}'::vector), (%(b)s, 'b', '{lb}'::vector)
        """,
        {"a": a_id, "b": b_id},
    )

    spec = SearchSpec(
        name="vector_test",
        model_type=VecDoc,
        fields=["id", "label"],
    )
    ctx = _vector_search_context(
        pgvector_client, table=table, index_name=index_name, vector_distance="l2"
    )
    port = ctx.search_query(spec)
    assert isinstance(port, PostgresVectorSearchAdapterV2)
    _ = port.index_qname, PostgresQualifiedName("public", index_name)

    hits, total = await port.search("alpha")
    assert total == 2
    assert hits[0].id == a_id
    assert hits[1].id == b_id

    disj, n_disj = await port.search(["alpha", "beta"])
    assert n_disj == 2
    assert {row.id for row in disj} == {a_id, b_id}
    assert disj[0].id == a_id


@pytest.mark.asyncio
async def test_vector_l2_with_hnsw_index(pgvector_client: PostgresClient) -> None:
    """ANN index does not change KNN results for a tiny table."""
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_hnsw_{suffix}"
    index_name = f"idx_hnsw_{suffix}"

    await pgvector_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            label text NOT NULL,
            emb vector(3) NOT NULL
        );
        CREATE INDEX {index_name}_ann ON {table} USING hnsw (emb vector_l2_ops);
        """
    )

    prov = MockHashEmbeddingsProvider(dimensions=3)
    x, y = uuid4(), uuid4()
    vx = await prov.embed_one("xkey")
    vy = await prov.embed_one("ykey")
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES
        (%(x)s, 'x', '{vector_param_literal(vx)}'::vector),
        (%(y)s, 'y', '{vector_param_literal(vy)}'::vector)
        """,
        {"x": x, "y": y},
    )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(
        pgvector_client, table=table, index_name=index_name, vector_distance="l2"
    )
    port = ctx.search_query(spec)
    out, n = await port.search("xkey")
    assert n == 2
    assert out[0].id == x
    assert out[1].id == y


@pytest.mark.asyncio
async def test_vector_cosine_knn_orders_by_nearest(pgvector_client: PostgresClient) -> None:
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_cos_{suffix}"
    index_name = f"idx_vec_cos_{suffix}"
    a_id, b_id = uuid4(), uuid4()

    await pgvector_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            label text NOT NULL,
            emb vector(3) NOT NULL
        );
        """
    )

    prov = MockHashEmbeddingsProvider(dimensions=3)
    v_one = await prov.embed_one("one")
    v_two = await prov.embed_one("two")
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES
        (%(a)s, 'a', '{vector_param_literal(v_one)}'::vector),
        (%(b)s, 'b', '{vector_param_literal(v_two)}'::vector)
        """,
        {"a": a_id, "b": b_id},
    )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(
        pgvector_client, table=table, index_name=index_name, vector_distance="cosine"
    )
    port = ctx.search_query(spec)
    rows, total = await port.search("one")
    assert total == 2
    assert rows[0].id == a_id


@pytest.mark.asyncio
async def test_vector_inner_product_knn_orders_by_nearest(
    pgvector_client: PostgresClient,
) -> None:
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_ip_{suffix}"
    index_name = f"idx_vec_ip_{suffix}"
    a_id, b_id = uuid4(), uuid4()

    await pgvector_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            label text NOT NULL,
            emb vector(3) NOT NULL
        );
        """
    )

    prov = MockHashEmbeddingsProvider(dimensions=3)
    p_a = await prov.embed_one("ip_a")
    p_b = await prov.embed_one("ip_b")
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES
        (%(a)s, 'a', '{vector_param_literal(p_a)}'::vector),
        (%(b)s, 'b', '{vector_param_literal(p_b)}'::vector)
        """,
        {"a": a_id, "b": b_id},
    )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(
        pgvector_client,
        table=table,
        index_name=index_name,
        vector_distance="inner_product",
    )
    port = ctx.search_query(spec)
    rows, total = await port.search("ip_a")
    assert total == 2
    assert rows[0].id == a_id


@pytest.mark.asyncio
async def test_vector_empty_query_zero_rank_includes_all_filtered_rows(
    pgvector_client: PostgresClient,
) -> None:
    """Whitespace-only query skips embedding; every joined row gets rank 0 (tie)."""
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_empty_{suffix}"
    index_name = f"idx_vec_e_{suffix}"
    a_id, b_id = uuid4(), uuid4()

    await pgvector_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            label text NOT NULL,
            emb vector(3) NOT NULL
        );
        """
    )
    prov = MockHashEmbeddingsProvider(dimensions=3)
    z = vector_param_literal(await prov.embed_one("z"))
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES
        (%(a)s, '1', '{z}'::vector), (%(b)s, '2', '{z}'::vector)
        """,
        {"a": a_id, "b": b_id},
    )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)
    _rows, total = await port.search("   ")
    assert total == 2


@pytest.mark.asyncio
async def test_vector_respects_eq_filter_on_projection(pgvector_client: PostgresClient) -> None:
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_flt_{suffix}"
    index_name = f"idx_vec_flt_{suffix}"

    await pgvector_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            label text NOT NULL,
            emb vector(3) NOT NULL
        );
        """
    )
    prov = MockHashEmbeddingsProvider(dimensions=3)
    keep = uuid4()
    drop = uuid4()
    v_keep = await prov.embed_one("filter_me")
    v_drop = await prov.embed_one("other")
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES
        (%(k)s, 'keep', '{vector_param_literal(v_keep)}'::vector),
        (%(d)s, 'drop', '{vector_param_literal(v_drop)}'::vector)
        """,
        {"k": keep, "d": drop},
    )

    spec = SearchSpec(
        name="vector_test",
        model_type=VecDoc,
        fields=["id", "label"],
    )
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)
    flt: QueryFilterExpression = {"$fields": {"label": "keep"}}
    rows, n = await port.search("filter_me", filters=flt)
    assert n == 1
    assert rows[0].id == keep
    assert rows[0].label == "keep"
