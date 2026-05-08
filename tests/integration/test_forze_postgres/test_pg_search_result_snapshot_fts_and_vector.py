"""FTS and vector v2 search with Valkey result-ID snapshot materialization and re-read (integration)."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.embeddings import (
    EmbeddingsProviderDepKey,
    EmbeddingsSpec,
)
from forze.application.contracts.search import (
    SearchQueryDepKey,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_mock import MockHashEmbeddingsProvider
from forze_postgres.adapters.search import (
    PostgresFTSSearchAdapter,
    PostgresVectorSearchAdapter,
)
from forze_postgres.adapters.search._vector_sql import vector_param_literal
from forze_postgres.execution.deps.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient
from forze_redis.execution.deps.deps import ConfigurableRedisSearchResultSnapshot
from forze_redis.execution.deps.keys import RedisClientDepKey
from forze_redis.kernel.platform.client import RedisClient


class FtsRow(BaseModel):
    id: UUID
    title: str
    content: str


class VecRow(BaseModel):
    id: UUID
    label: str


def _embeddings_factory(
    _ctx: ExecutionContext,
    spec: EmbeddingsSpec,
) -> MockHashEmbeddingsProvider:
    return MockHashEmbeddingsProvider(dimensions=spec.dimensions)


def _exec_fts(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    *,
    table: str,
    index_name: str,
) -> ExecutionContext:
    ns = f"it:rss:fts:{uuid4().hex[:10]}"
    return ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config={"namespace": ns},
                ),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config={
                        "index": ("public", index_name),
                        "read": ("public", table),
                        "heap": ("public", table),
                        "engine": "fts",
                        "fts_groups": {
                            "A": ("title",),
                            "B": ("content",),
                        },
                    }
                ),
            }
        )
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fts_v2_result_snapshot_reread(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    suffix = uuid4().hex[:12]
    table = f"fts_rss_{suffix}"
    index_name = f"idx_fts_rss_{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        CREATE INDEX {index_name}
        ON {table}
        USING gin (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content, '')));
        """
    )
    rid = uuid4()
    await pg_client.execute(
        f"""
        INSERT INTO {table} (id, title, content)
        VALUES (%(id)s, 'snap fts', 'keyword token');
        """,
        {"id": rid},
    )
    spec = SearchSpec(
        name="fts_rss",
        model_type=FtsRow,
        fields=["title", "content"],
        snapshot=SearchResultSnapshotSpec(
            name="snap",
            enabled=True,
            ttl=timedelta(minutes=5),
            max_ids=1_000,
        ),
    )
    ctx = _exec_fts(pg_client, redis_client, table=table, index_name=index_name)
    adapter = ctx.search_query(spec)
    assert isinstance(adapter, PostgresFTSSearchAdapter)

    p1 = await adapter.search(
        "keyword", return_count=True, pagination={"limit": 5, "offset": 0}
    )
    assert p1.snapshot is not None
    assert p1.count == 1
    p2 = await adapter.search(
        "keyword",
        return_count=True,
        pagination={"limit": 5, "offset": 0},
        snapshot={"id": p1.snapshot.id, "fingerprint": p1.snapshot.fingerprint},
    )
    assert p2.hits[0].title == p1.hits[0].title
    assert p2.count == 1
    assert p2.snapshot is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_vector_v2_result_snapshot_reread(
    pgvector_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    await pgvector_client.execute("CREATE EXTENSION IF NOT EXISTS vector")
    suffix = uuid4().hex[:12]
    table = f"vec_rss_{suffix}"
    index_name = f"idx_vec_rss_{suffix}"
    await pgvector_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            label text NOT NULL,
            emb vector(3) NOT NULL
        );
        CREATE INDEX {index_name} ON {table} USING hnsw (emb vector_l2_ops);
        """
    )
    prov = MockHashEmbeddingsProvider(dimensions=3)
    v = await prov.embed_one("vecq")
    await pgvector_client.execute(
        f"""
        INSERT INTO {table} (id, label, emb)
        VALUES (%(id)s, 'row1', '{vector_param_literal(v)}'::vector);
        """,
        {"id": uuid4()},
    )
    ns = f"it:rss:vec:{uuid4().hex[:10]}"
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pgvector_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(
                    client=pgvector_client
                ),
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config={"namespace": ns},
                ),
                EmbeddingsProviderDepKey: _embeddings_factory,
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config={
                        "index": ("public", index_name),
                        "read": ("public", table),
                        "heap": ("public", table),
                        "engine": "vector",
                        "vector_column": "emb",
                        "vector_distance": "l2",
                        "embeddings_name": "vec_rss",
                        "embedding_dimensions": 3,
                    }
                ),
            }
        )
    )
    spec = SearchSpec(
        name="vec_rss",
        model_type=VecRow,
        fields=["id", "label"],
        snapshot=SearchResultSnapshotSpec(
            name="snap",
            enabled=True,
            ttl=timedelta(minutes=5),
        ),
    )
    adapter = ctx.search_query(spec)
    assert isinstance(adapter, PostgresVectorSearchAdapter)
    p1 = await adapter.search(
        "vecq", return_count=True, pagination={"limit": 3, "offset": 0}
    )
    assert p1.snapshot is not None
    p2 = await adapter.search(
        "vecq",
        return_count=True,
        pagination={"limit": 3, "offset": 0},
        snapshot={"id": p1.snapshot.id, "fingerprint": p1.snapshot.fingerprint},
    )
    assert p2.hits[0].label == p1.hits[0].label
    assert p2.count == 1
