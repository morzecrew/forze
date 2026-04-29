"""PGroonga v2 search with Valkey result-ID snapshot materialization and re-read (integration)."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression
from forze.application.contracts.search import (
    SearchQueryDepKey,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.adapters.search import PostgresPGroongaSearchAdapterV2
from forze_postgres.execution.deps.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient
from forze_redis.execution.deps.deps import ConfigurableRedisSearchResultSnapshot
from forze_redis.execution.deps.keys import RedisClientDepKey
from forze_redis.kernel.platform.client import RedisClient


class PgRow(BaseModel):
    id: UUID
    title: str
    content: str


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_v2_result_snapshot_reread(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    suffix = uuid4().hex[:12]
    table = f"pgr_rss_{suffix}"
    index_name = f"idx_pgr_rss_{suffix}"
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
        ON {table} USING pgroonga ((ARRAY[title, content]));
        """,
    )
    rid = uuid4()
    await pg_client.execute(
        f"""
        INSERT INTO {table} (id, title, content)
        VALUES (%(id)s, 'pgr snap', 'hello pgroonga world');
        """,
        {"id": rid},
    )
    ns = f"it:rss:pgr:{uuid4().hex[:10]}"
    ctx = ExecutionContext(
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
                        "engine": "pgroonga",
                    }
                ),
            }
        )
    )
    spec = SearchSpec(
        name="pgr_rss",
        model_type=PgRow,
        fields=["title", "content"],
        snapshot=SearchResultSnapshotSpec(
            name="snap",
            enabled=True,
            ttl=timedelta(minutes=5),
        ),
    )
    adapter = ctx.search_query(spec)
    assert isinstance(adapter, PostgresPGroongaSearchAdapterV2)
    p1 = await adapter.search("hello", return_count=True, pagination={"limit": 5, "offset": 0})
    assert p1.snapshot is not None
    p2 = await adapter.search(
        "hello",
        return_count=True,
        pagination={"limit": 5, "offset": 0},
        snapshot={"id": p1.snapshot.id, "fingerprint": p1.snapshot.fingerprint},
    )
    assert p2.hits[0].title == p1.hits[0].title
    assert p2.count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_v2_filter_only_empty_query_snapshot_reread(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    """No search terms, structured filter only: still materialize and re-read the snapshot."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    suffix = uuid4().hex[:12]
    table = f"pgr_filt_{suffix}"
    index_name = f"idx_pgr_filt_{suffix}"
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
        ON {table} USING pgroonga ((ARRAY[title, content]));
        """,
    )
    await pg_client.execute(
        f"""
        INSERT INTO {table} (id, title, content) VALUES
        (%(a)s, 'match-me', 'body'),
        (%(b)s, 'other', 'body');
        """,
        {"a": uuid4(), "b": uuid4()},
    )
    ns = f"it:rss:pgrf:{uuid4().hex[:10]}"
    ctx = ExecutionContext(
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
                        "engine": "pgroonga",
                    }
                ),
            }
        )
    )
    spec = SearchSpec(
        name="pgr_filt",
        model_type=PgRow,
        fields=["title", "content"],
        snapshot=SearchResultSnapshotSpec(
            name="snap",
            enabled=True,
            ttl=timedelta(minutes=5),
        ),
    )
    adapter = ctx.search_query(spec)
    flt: QueryFilterExpression = {"$fields": {"title": "match-me"}}
    p1 = await adapter.search(
        "",
        filters=flt,
        return_count=True,
        pagination={"limit": 3, "offset": 0},
    )
    assert p1.count == 1
    assert p1.snapshot is not None
    p2 = await adapter.search(
        "",
        filters=flt,
        return_count=True,
        pagination={"limit": 3, "offset": 0},
        snapshot={"id": p1.snapshot.id, "fingerprint": p1.snapshot.fingerprint},
    )
    assert p2.hits[0].title == "match-me"
