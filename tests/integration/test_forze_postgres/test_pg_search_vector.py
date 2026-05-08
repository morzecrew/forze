"""Integration tests for pgvector :class:`PostgresVectorSearchAdapterV2` (KNN + filters)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import CursorPage, Page
from forze.application.contracts.embeddings import (
    EmbeddingsProviderDepKey,
    EmbeddingsSpec,
)
from forze.application.contracts.query import QueryFilterExpression
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_mock import MockHashEmbeddingsProvider
from forze_postgres.adapters.search import PostgresVectorSearchAdapter
from forze_postgres.adapters.search._vector_sql import vector_param_literal
from forze_postgres.execution.deps.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient

# ----------------------- #


class VecDoc(BaseModel):
    id: UUID
    label: str


class VecDocView(BaseModel):
    """Alternate read model with the same columns as :class:`VecDoc` (``return_type`` tests)."""

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
    assert isinstance(port, PostgresVectorSearchAdapter)
    _ = port.index_qname, PostgresQualifiedName("public", index_name)

    __p = await port.search("alpha", return_count=True)
    hits = __p.hits
    total = __p.count
    assert total == 2
    assert hits[0].id == a_id
    assert hits[1].id == b_id

    __p = await port.search(["alpha", "beta"], return_count=True)
    disj = __p.hits
    n_disj = __p.count
    assert n_disj == 2
    assert {row.id for row in disj} == {a_id, b_id}
    assert disj[0].id == a_id

    __p = await port.search(
        ["alpha", "beta"], options={"phrase_combine": "all"}, return_count=True
    )
    conj = __p.hits
    n_conj = __p.count
    assert n_conj == 2
    assert {row.id for row in conj} == {a_id, b_id}


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
    __p = await port.search("xkey", return_count=True)
    out = __p.hits
    n = __p.count
    assert n == 2
    assert out[0].id == x
    assert out[1].id == y


@pytest.mark.asyncio
async def test_vector_cosine_knn_orders_by_nearest(
    pgvector_client: PostgresClient,
) -> None:
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
    __p = await port.search("one", return_count=True)
    rows = __p.hits
    total = __p.count
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
    __p = await port.search("ip_a", return_count=True)
    rows = __p.hits
    total = __p.count
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
    __p = await port.search("   ", return_count=True)
    _rows = __p.hits
    total = __p.count
    assert total == 2


@pytest.mark.asyncio
async def test_vector_respects_eq_filter_on_projection(
    pgvector_client: PostgresClient,
) -> None:
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
    __p = await port.search("filter_me", filters=flt, return_count=True)
    rows = __p.hits
    n = __p.count
    assert n == 1
    assert rows[0].id == keep
    assert rows[0].label == "keep"


@pytest.mark.asyncio
async def test_vector_return_count_no_matches_short_circuit(
    pgvector_client: PostgresClient,
) -> None:
    """``return_count=True`` with an empty filtered CTE returns a zero page without listing rows."""
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_zc_{suffix}"
    index_name = f"idx_vec_zc_{suffix}"

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
    one_id = uuid4()
    v = vector_param_literal(await prov.embed_one("lonely"))
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES (%(i)s, 'only', '{v}'::vector)
        """,
        {"i": one_id},
    )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)
    impossible: QueryFilterExpression = {"$fields": {"label": "nope"}}
    page = await port.search("lonely", filters=impossible, return_count=True)
    assert isinstance(page, Page)
    assert page.count == 0
    assert page.hits == []


@pytest.mark.asyncio
async def test_vector_sorts_add_secondary_order_on_tied_rank(
    pgvector_client: PostgresClient,
) -> None:
    """Whitespace query ties vector rank; ``sorts`` adds a projection ``ORDER BY`` tie-breaker."""
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_sort_{suffix}"
    index_name = f"idx_vec_sort_{suffix}"
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
    z = vector_param_literal(await prov.embed_one("tie"))
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES
        (%(b)s, 'b', '{z}'::vector), (%(a)s, 'a', '{z}'::vector)
        """,
        {"a": a_id, "b": b_id},
    )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)
    page = await port.search(" ", sorts={"label": "asc"}, return_count=True)
    assert page.count == 2
    assert [h.label for h in page.hits] == ["a", "b"]


@pytest.mark.asyncio
async def test_vector_return_fields_and_pagination(
    pgvector_client: PostgresClient,
) -> None:
    """Tied ranks + ``sorts`` yields stable order for ``LIMIT`` / ``OFFSET`` and projections."""
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_rf_{suffix}"
    index_name = f"idx_vec_rf_{suffix}"
    ids = [uuid4() for _ in range(3)]
    labels = ["a", "b", "c"]

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
    z = vector_param_literal(await prov.embed_one("tie"))
    for u, lb in zip(ids, labels, strict=True):
        await pgvector_client.execute(
            f"""
            INSERT INTO {table} (id, label, emb) VALUES (%(id)s, %(lb)s, '{z}'::vector)
            """,
            {"id": u, "lb": lb},
        )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)

    counted = await port.search(
        " ",
        sorts={"label": "asc"},
        pagination={"limit": 2, "offset": 0},
        return_fields=["id", "label"],
        return_count=True,
    )
    assert counted.count == 3
    assert len(counted.hits) == 2
    assert all(set(r.keys()) == {"id", "label"} for r in counted.hits)
    assert [r["label"] for r in counted.hits] == ["a", "b"]

    page2 = await port.search(
        " ",
        sorts={"label": "asc"},
        pagination={"limit": 2, "offset": 2},
        return_fields=["label"],
        return_count=False,
    )
    assert not isinstance(page2, Page)
    assert len(page2.hits) == 1
    assert page2.hits[0] == {"label": "c"}


@pytest.mark.asyncio
async def test_vector_search_return_type_override(
    pgvector_client: PostgresClient,
) -> None:
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_rt_{suffix}"
    index_name = f"idx_vec_rt_{suffix}"
    row_id = uuid4()

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
    v = vector_param_literal(await prov.embed_one("typed"))
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES (%(i)s, 'x', '{v}'::vector)
        """,
        {"i": row_id},
    )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)
    page = await port.search("typed", return_type=VecDocView, return_count=True)
    assert page.count == 1
    assert isinstance(page.hits[0], VecDocView)
    assert page.hits[0].id == row_id


@pytest.mark.asyncio
async def test_vector_search_return_type_countless_page(
    pgvector_client: PostgresClient,
) -> None:
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_rt0_{suffix}"
    index_name = f"idx_vec_rt0_{suffix}"
    row_id = uuid4()

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
    v = vector_param_literal(await prov.embed_one("rt0"))
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES (%(i)s, 'y', '{v}'::vector)
        """,
        {"i": row_id},
    )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)
    page = await port.search("rt0", return_type=VecDocView, return_count=False)
    assert not isinstance(page, Page)
    assert len(page.hits) == 1
    assert isinstance(page.hits[0], VecDocView)


@pytest.mark.asyncio
async def test_vector_search_default_model_countless_page(
    pgvector_client: PostgresClient,
) -> None:
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_nc_{suffix}"
    index_name = f"idx_vec_nc_{suffix}"

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
    z = vector_param_literal(await prov.embed_one("nc"))
    u = uuid4()
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES (%(i)s, 'only', '{z}'::vector)
        """,
        {"i": u},
    )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)
    page = await port.search("nc", return_count=False)
    assert not isinstance(page, Page)
    assert len(page.hits) == 1
    assert isinstance(page.hits[0], VecDoc)


@pytest.mark.asyncio
async def test_vector_search_with_cursor_ranked_chains(
    pgvector_client: PostgresClient,
) -> None:
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_cur_{suffix}"
    index_name = f"idx_vec_cur_{suffix}"

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
    va = vector_param_literal(await prov.embed_one("near-a"))
    vb = vector_param_literal(await prov.embed_one("near-b"))
    vc = vector_param_literal(await prov.embed_one("near-c"))
    id_a, id_b, id_c = uuid4(), uuid4(), uuid4()
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES
        (%(a)s, 'a', '{va}'::vector),
        (%(b)s, 'b', '{vb}'::vector),
        (%(c)s, 'c', '{vc}'::vector)
        """,
        {"a": id_a, "b": id_b, "c": id_c},
    )

    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)

    p1: CursorPage = await port.search_with_cursor(
        "near-a",
        sorts={"label": "asc"},
        return_fields=["id", "label"],
        cursor={"limit": 1},
    )
    assert len(p1.hits) == 1
    assert set(p1.hits[0].keys()) == {"id", "label"}
    assert p1.has_more is True
    assert p1.next_cursor is not None

    p2 = await port.search_with_cursor(
        "near-a",
        sorts={"label": "asc"},
        return_fields=["id", "label"],
        cursor={"limit": 5, "after": p1.next_cursor},
    )
    assert len(p2.hits) == 2
    assert {p1.hits[0]["id"], *{r["id"] for r in p2.hits}} == {id_a, id_b, id_c}

    b0 = await port.search_with_cursor(
        "",
        sorts={"label": "asc"},
        return_fields=["id", "label"],
        cursor={"limit": 2},
    )
    assert len(b0.hits) == 2
    assert b0.has_more is True


@pytest.mark.asyncio
async def test_vector_search_three_term_query_multi_embed(
    pgvector_client: PostgresClient,
) -> None:
    """Three sub-queries use ``embed`` (not ``embed_one``) and multi-rank KNN SQL."""
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_3q_{suffix}"
    index_name = f"idx_vec_3q_{suffix}"

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
    v0 = await prov.embed_one("q0")
    v1 = await prov.embed_one("q1")
    await prov.embed_one("q2")
    v_sum = [v0[i] + v1[i] for i in range(3)]
    a_id, b_id, c_id = uuid4(), uuid4(), uuid4()
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES
        (%(a)s, 'one', '{vector_param_literal(v0)}'::vector),
        (%(b)s, 'two', '{vector_param_literal(v1)}'::vector),
        (%(c)s, 'sum', '{vector_param_literal(v_sum)}'::vector);
        """,
        {"a": a_id, "b": b_id, "c": c_id},
    )
    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)
    p = await port.search(
        ["q0", "q1", "q2"],
        options={"phrase_combine": "all"},
        return_count=True,
    )
    assert p.count == 3
    assert {r.label for r in p.hits} == {"one", "two", "sum"}


@pytest.mark.asyncio
async def test_vector_search_with_cursor_browse_before(
    pgvector_client: PostgresClient,
) -> None:
    """No-query keyset: ``after`` + ``before`` on the same sort order (tie-break on ``id``)."""
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_bef_{suffix}"
    index_name = f"idx_vec_bef_{suffix}"
    a_id, b_id, c_id = uuid4(), uuid4(), uuid4()

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
    z = vector_param_literal(await prov.embed_one("browse-tie"))
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES
        (%(c)s, 'c', '{z}'::vector),
        (%(a)s, 'a', '{z}'::vector),
        (%(b)s, 'b', '{z}'::vector);
        """,
        {"a": a_id, "b": b_id, "c": c_id},
    )
    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)
    p0: CursorPage = await port.search_with_cursor(
        "",
        sorts={"label": "asc"},
        return_fields=["id", "label"],
        cursor={"limit": 1},
    )
    assert len(p0.hits) == 1
    assert p0.hits[0]["label"] == "a"
    assert p0.has_more is True
    assert p0.next_cursor is not None

    p1 = await port.search_with_cursor(
        "",
        sorts={"label": "asc"},
        return_fields=["id", "label"],
        cursor={"limit": 1, "after": p0.next_cursor},
    )
    assert len(p1.hits) == 1
    assert p1.hits[0]["label"] == "b"
    assert p1.next_cursor is not None

    p_back: CursorPage = await port.search_with_cursor(
        "",
        sorts={"label": "asc"},
        return_fields=["id", "label"],
        cursor={"limit": 2, "before": p1.next_cursor},
    )
    assert len(p_back.hits) >= 1
    assert p_back.hits[0]["label"] == "a"


@pytest.mark.asyncio
async def test_vector_search_with_cursor_ranked_return_type_and_before(
    pgvector_client: PostgresClient,
) -> None:
    """KNN + secondary sort: ``return_type`` on cursor rows and a backward ``before`` page."""
    await _ensure_vector_extension(pgvector_client)

    suffix = uuid4().hex[:12]
    table = f"vec_rtb_{suffix}"
    index_name = f"idx_vec_rtb_{suffix}"
    a_id, b_id, c_id = uuid4(), uuid4(), uuid4()

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
    va = vector_param_literal(await prov.embed_one("knn-a"))
    vb = vector_param_literal(await prov.embed_one("knn-b"))
    vc = vector_param_literal(await prov.embed_one("knn-c"))
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb) VALUES
        (%(a)s, 'a', '{va}'::vector),
        (%(b)s, 'b', '{vb}'::vector),
        (%(c)s, 'c', '{vc}'::vector);
        """,
        {"a": a_id, "b": b_id, "c": c_id},
    )
    spec = SearchSpec(name="vector_test", model_type=VecDoc, fields=["id", "label"])
    ctx = _vector_search_context(pgvector_client, table=table, index_name=index_name)
    port = ctx.search_query(spec)

    p0: CursorPage = await port.search_with_cursor(
        "knn-a",
        sorts={"label": "asc"},
        return_type=VecDocView,
        cursor={"limit": 1},
    )
    assert len(p0.hits) == 1
    assert isinstance(p0.hits[0], VecDocView)
    assert p0.hits[0].id in {a_id, b_id, c_id}
    assert p0.has_more is True
    assert p0.next_cursor is not None

    p1 = await port.search_with_cursor(
        "knn-a",
        sorts={"label": "asc"},
        return_type=VecDocView,
        cursor={"limit": 1, "after": p0.next_cursor},
    )
    assert len(p1.hits) == 1
    assert p1.next_cursor is not None

    p_back: CursorPage = await port.search_with_cursor(
        "knn-a",
        sorts={"label": "asc"},
        return_type=VecDocView,
        cursor={"limit": 2, "before": p1.next_cursor},
    )
    assert len(p_back.hits) >= 1
    assert p_back.hits[0].id == p0.hits[0].id
