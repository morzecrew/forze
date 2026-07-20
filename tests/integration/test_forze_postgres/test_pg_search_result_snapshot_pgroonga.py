"""PGroonga v2 search with Valkey result-ID snapshot materialization and re-read (integration)."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.search import (
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.execution import Deps
from forze_postgres.adapters.search import PostgresPGroongaSearchAdapter
from forze_postgres.execution.deps import (
    ConfigurablePostgresHubSearch,
    ConfigurablePostgresSearch,
)
from forze_postgres.execution.deps.configs import (
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresSearchConfig,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_redis.execution.deps import ConfigurableRedisSearchResultSnapshot
from forze_redis.execution.deps.configs import RedisSearchResultSnapshotConfig
from forze_redis.execution.deps.keys import RedisClientDepKey
from forze_redis.kernel.client import RedisClient
from tests.support.execution_context import context_from_deps


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
    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config=RedisSearchResultSnapshotConfig(namespace=ns),
                ),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index_name),
                        read=("public", table),
                        heap=("public", table),
                        engine="pgroonga",
                    )
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
    adapter = ctx.search.query(spec)
    assert isinstance(adapter, PostgresPGroongaSearchAdapter)
    p1 = await adapter.search_page(
        "hello",
        pagination={"limit": 5, "offset": 0},
    )
    assert p1.snapshot is not None
    p2 = await adapter.search_page(
        "hello",
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
    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config=RedisSearchResultSnapshotConfig(namespace=ns),
                ),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index_name),
                        read=("public", table),
                        heap=("public", table),
                        engine="pgroonga",
                    )
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
    adapter = ctx.search.query(spec)
    flt: QueryFilterExpression = {"$values": {"title": "match-me"}}
    p1 = await adapter.search_page(
        "",
        filters=flt,
        pagination={"limit": 3, "offset": 0},
    )
    assert p1.count == 1
    assert p1.snapshot is not None
    p2 = await adapter.search_page(
        "",
        filters=flt,
        pagination={"limit": 3, "offset": 0},
        snapshot={"id": p1.snapshot.id, "fingerprint": p1.snapshot.fingerprint},
    )
    assert p2.hits[0].title == "match-me"


class _KindRow(BaseModel):
    name: str = ""
    display_name: str = ""


class _DocHub(BaseModel):
    id: UUID
    kind_id: UUID
    body: str


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_result_snapshot_reread_thin_per_window(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    """Hub snapshot write streams the thin id pipeline per window, hydrates by id,
    then replays identically."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    suffix = uuid4().hex[:12]
    kinds = f"hub_rss_kinds_{suffix}"
    hub = f"hub_rss_docs_{suffix}"
    index_name = f"idx_{kinds}_pg"
    await pg_client.execute(
        f"""
        CREATE TABLE {kinds} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE {hub} (
            id uuid PRIMARY KEY,
            kind_id uuid NOT NULL REFERENCES {kinds} (id),
            body text NOT NULL
        );
        CREATE INDEX {index_name} ON {kinds} USING pgroonga ((ARRAY[name, display_name]));
        """
    )
    k1 = uuid4()
    await pg_client.execute(
        f"INSERT INTO {kinds} (id, name, display_name) VALUES (%(id)s, %(n)s, %(d)s)",
        {"id": k1, "n": "alpha kind", "d": "Alpha"},
    )
    doc_ids = [uuid4() for _ in range(5)]
    for i, did in enumerate(doc_ids):
        await pg_client.execute(
            f"INSERT INTO {hub} (id, kind_id, body) VALUES (%(id)s, %(k)s, %(b)s)",
            {"id": did, "k": k1, "b": f"heavy body {i}"},
        )

    ns = f"it:rss:hub:{uuid4().hex[:10]}"
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config=RedisSearchResultSnapshotConfig(namespace=ns),
                ),
                HubSearchQueryDepKey: ConfigurablePostgresHubSearch(
                    config=PostgresHubSearchConfig(
                        hub=("public", hub),
                        members={
                            "kind_txt": PostgresHubSearchMemberConfig(
                                index=("public", index_name),
                                read=("public", kinds),
                                hub_fk="kind_id",
                                engine="pgroonga",
                            ),
                        },
                    )
                ),
            }
        )
    )
    hub_spec = HubSearchSpec(
        name="hub_rss",
        model_type=_DocHub,
        members=(
            SearchSpec(name="kind_txt", model_type=_KindRow, fields=["name", "display_name"]),
        ),
        snapshot=SearchResultSnapshotSpec(
            name="snap",
            enabled=True,
            ttl=timedelta(minutes=5),
            chunk_size=2,
        ),
    )
    adapter = ctx.search.hub(hub_spec)

    p1 = await adapter.search_page("alpha", pagination={"limit": 3, "offset": 0})
    assert p1.snapshot is not None
    assert p1.count == 5
    assert len(p1.hits) == 3
    assert all(h.body.startswith("heavy body") for h in p1.hits)

    p2 = await adapter.search_page(
        "alpha",
        pagination={"limit": 3, "offset": 0},
        snapshot={"id": p1.snapshot.id, "fingerprint": p1.snapshot.fingerprint},
    )
    assert [h.id for h in p2.hits] == [h.id for h in p1.hits]
    assert p2.count == 5
    assert p2.hits[0].body == p1.hits[0].body
