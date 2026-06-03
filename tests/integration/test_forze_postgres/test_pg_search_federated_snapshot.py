"""Postgres federated search with Redis result snapshots (integration)."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, Page
from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    FederatedSearchSpec,
    SearchQueryDepKey,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.execution import Deps
from forze_postgres.execution.deps import (
    ConfigurablePostgresFederatedSearch,
    ConfigurablePostgresSearch,
)
from forze_postgres.execution.deps.configs import (
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLegSearch,
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


class FedHit(BaseModel):
    id: UUID
    label: str


def _mem(name: str) -> SearchSpec[FedHit]:
    return SearchSpec(name=name, model_type=FedHit, fields=["label"])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_snapshot_reread_search_count_none(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    """Snapshot re-read with ``search_count=none`` returns hits without a total."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:10]
    ta = f"fed_snap_a_{suffix}"
    tb = f"fed_snap_b_{suffix}"
    ia = f"idx_fed_a_{suffix}"
    ib = f"idx_fed_b_{suffix}"
    token = "fedshared"

    for table, idx in ((ta, ia), (tb, ib)):
        await pg_client.execute(
            f"""
            CREATE TABLE {table} (
                id uuid PRIMARY KEY,
                label text NOT NULL
            );
            CREATE INDEX {idx} ON {table}
            USING pgroonga (label);
            """
        )

    id_a, id_b = uuid4(), uuid4()
    await pg_client.execute(
        f"INSERT INTO {ta} (id, label) VALUES (%(id)s, %(lbl)s)",
        {"id": id_a, "lbl": f"{token} leg-a"},
    )
    await pg_client.execute(
        f"INSERT INTO {tb} (id, label) VALUES (%(id)s, %(lbl)s)",
        {"id": id_b, "lbl": f"{token} leg-b"},
    )

    leg_a, leg_b = f"a_{suffix}", f"b_{suffix}"
    ns = f"it:fed:snap:{uuid4().hex[:10]}"
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config=RedisSearchResultSnapshotConfig(namespace=ns),
                ),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", ia),
                        read=("public", ta),
                        engine="pgroonga",
                    )
                ),
                FederatedSearchQueryDepKey: ConfigurablePostgresFederatedSearch(
                    config=PostgresFederatedSearchConfig(
                        members={
                            leg_a: PostgresFederatedSearchLegSearch(
                                search=PostgresSearchConfig(
                                    index=("public", ia),
                                    read=("public", ta),
                                    engine="pgroonga",
                                ),
                            ),
                            leg_b: PostgresFederatedSearchLegSearch(
                                search=PostgresSearchConfig(
                                    index=("public", ib),
                                    read=("public", tb),
                                    engine="pgroonga",
                                ),
                            ),
                        },
                    ),
                ),
            }
        )
    )

    fed_spec = FederatedSearchSpec(
        name=f"fed_snap_{suffix}",
        members=(_mem(leg_a), _mem(leg_b)),
        snapshot=SearchResultSnapshotSpec(
            name="snap",
            enabled=True,
            ttl=timedelta(minutes=5),
        ),
    )
    fed = ctx.search.federated(fed_spec)

    first = await fed.search_page(
        token,
        pagination={"limit": 10, "offset": 0},
    )
    assert isinstance(first, Page)
    assert first.count == 2
    assert first.snapshot is not None
    assert len(first.hits) == 2

    second = await fed.search_page(
        token,
        pagination={"limit": 10, "offset": 0},
        options={"search_count": "none"},
        snapshot={
            "id": first.snapshot.id,
            "fingerprint": first.snapshot.fingerprint,
        },
    )
    assert isinstance(second, CountlessPage)
    assert not isinstance(second, Page)
    assert len(second.hits) == 2
    assert {row.hit.id for row in second.hits} == {id_a, id_b}
