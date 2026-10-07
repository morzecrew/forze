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
from forze.application.contracts.querying import UNSUPPORTED_QUERY_FEATURE_CODE
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze_postgres.execution.deps import (
    ConfigurablePostgresFederatedSearch,
    ConfigurablePostgresSearch,
)
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PgroongaEngine,
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
        thin_merge=False,  # the full-record snapshot format
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_thin_snapshot_write_and_replay(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    """A ``thin_merge`` federated snapshot stores (member, id) keys and replays by re-fetch."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:10]
    ta, tb = f"fed_tsnap_a_{suffix}", f"fed_tsnap_b_{suffix}"
    ia, ib = f"idx_tsnap_a_{suffix}", f"idx_tsnap_b_{suffix}"
    token = "tsnaptok"

    for table, idx in ((ta, ia), (tb, ib)):
        await pg_client.execute(
            f"""
            CREATE TABLE {table} (
                id uuid PRIMARY KEY,
                label text NOT NULL
            );
            CREATE INDEX {idx} ON {table} USING pgroonga (label);
            """
        )

    shared, only_b = uuid4(), uuid4()
    inserts = (
        (ta, shared, f"{token} a"),
        (tb, shared, f"{token} shared"),
        (tb, only_b, f"{token} b"),
    )
    for table, rid, lbl in inserts:
        await pg_client.execute(
            f"INSERT INTO {table} (id, label) VALUES (%(id)s, %(lbl)s)",
            {"id": rid, "lbl": lbl},
        )

    leg_a, leg_b = f"a_{suffix}", f"b_{suffix}"
    ns = f"it:fed:tsnap:{uuid4().hex[:10]}"
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config=RedisSearchResultSnapshotConfig(namespace=ns),
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

    spec = FederatedSearchSpec(
        name=f"fed_tsnap_{suffix}",
        members=(_mem(leg_a), _mem(leg_b)),
        snapshot=SearchResultSnapshotSpec(
            name="snap", enabled=True, ttl=timedelta(minutes=5)
        ),
        thin_merge=True,
    )
    fed = ctx.search.federated(spec)

    first = await fed.search_page(token, pagination={"limit": 10})
    assert isinstance(first, Page)
    assert first.snapshot is not None
    # shared is a distinct federated identity per member: (a,shared)+(b,shared)+(b,only_b).
    assert first.count == 3
    first_ids = sorted((h.member, str(h.hit.id)) for h in first.hits)

    replay = await fed.search_page(
        token,
        pagination={"limit": 10},
        snapshot={
            "id": first.snapshot.id,
            "fingerprint": first.snapshot.fingerprint,
        },
    )
    # Replay re-fetches the frozen identities by id from the legs.
    assert sorted((h.member, str(h.hit.id)) for h in replay.hits) == first_ids


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_thin_merge_matches_full(pg_client: PostgresClient) -> None:
    """``thin_merge=True`` returns the same federated hits as the full-fetch path."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:10]
    ta, tb = f"fed_thin_a_{suffix}", f"fed_thin_b_{suffix}"
    ia, ib = f"idx_thin_a_{suffix}", f"idx_thin_b_{suffix}"
    token = "thintok"

    for table, idx in ((ta, ia), (tb, ib)):
        await pg_client.execute(
            f"""
            CREATE TABLE {table} (
                id uuid PRIMARY KEY,
                label text NOT NULL
            );
            CREATE INDEX {idx} ON {table} USING pgroonga (label);
            """
        )

    shared, only_a, only_b = uuid4(), uuid4(), uuid4()
    rows_by_table = (
        (ta, ((shared, f"{token} shared"), (only_a, f"{token} a"))),
        (tb, ((shared, f"{token} shared"), (only_b, f"{token} b"))),
    )
    for table, rows in rows_by_table:
        for rid, lbl in rows:
            await pg_client.execute(
                f"INSERT INTO {table} (id, label) VALUES (%(id)s, %(lbl)s)",
                {"id": rid, "lbl": lbl},
            )

    leg_a, leg_b = f"a_{suffix}", f"b_{suffix}"
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
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

    members = (_mem(leg_a), _mem(leg_b))
    full_spec = FederatedSearchSpec(name=f"fed_full_{suffix}", members=members, thin_merge=False)
    thin_spec = FederatedSearchSpec(
        name=f"fed_thinm_{suffix}", members=members, thin_merge=True
    )

    full = await ctx.search.federated(full_spec).search_page(
        token, pagination={"limit": 10}
    )
    thin = await ctx.search.federated(thin_spec).search_page(
        token, pagination={"limit": 10}
    )

    def idents(page: object) -> list[tuple[str, str]]:
        return [(h.member, str(h.hit.id)) for h in page.hits]  # type: ignore[attr-defined]

    assert idents(thin) == idents(full)
    assert thin.count == full.count == 4
    # The shared id is a distinct federated identity per member, preserved by the thin path.
    assert (leg_a, str(shared)) in idents(thin)
    assert (leg_b, str(shared)) in idents(thin)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_thin_merge_matches_full_with_sort(
    pg_client: PostgresClient,
) -> None:
    """A secondary ``sort`` produces the same ORDER thin vs. full (thin projects the field).

    Each leg contributes one rank-1 hit, so the two fuse to an equal RRF score and the
    ``label`` sort alone decides the order — a thin path that failed to project ``label``
    would fall back to the score tie-break and diverge from the full path.
    """
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:10]
    ta, tb = f"fed_sort_a_{suffix}", f"fed_sort_b_{suffix}"
    ia, ib = f"idx_sort_a_{suffix}", f"idx_sort_b_{suffix}"
    token = "sorttok"

    for table, idx in ((ta, ia), (tb, ib)):
        await pg_client.execute(
            f"""
            CREATE TABLE {table} (
                id uuid PRIMARY KEY,
                label text NOT NULL
            );
            CREATE INDEX {idx} ON {table} USING pgroonga (label);
            """
        )

    id_a, id_b = uuid4(), uuid4()
    # Distinct labels, one matching doc per leg → equal rank → the sort drives order.
    await pg_client.execute(
        f"INSERT INTO {ta} (id, label) VALUES (%(id)s, %(lbl)s)",
        {"id": id_a, "lbl": f"{token} zzz"},
    )
    await pg_client.execute(
        f"INSERT INTO {tb} (id, label) VALUES (%(id)s, %(lbl)s)",
        {"id": id_b, "lbl": f"{token} aaa"},
    )

    leg_a, leg_b = f"a_{suffix}", f"b_{suffix}"
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
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

    members = (_mem(leg_a), _mem(leg_b))
    full_spec = FederatedSearchSpec(
        name=f"fed_sfull_{suffix}", members=members, thin_merge=False
    )
    thin_spec = FederatedSearchSpec(
        name=f"fed_sthin_{suffix}", members=members, thin_merge=True
    )

    def ordered(page: object) -> list[tuple[str, str]]:
        return [(h.member, str(h.hit.id)) for h in page.hits]  # type: ignore[attr-defined]

    for direction in ("asc", "desc"):
        full = await ctx.search.federated(full_spec).search_page(
            token, pagination={"limit": 10}, sorts={"label": direction}
        )
        thin = await ctx.search.federated(thin_spec).search_page(
            token, pagination={"limit": 10}, sorts={"label": direction}
        )
        assert ordered(thin) == ordered(full), direction

    # The label sort actually reorders (i.e. the projected value is read, not None):
    # asc yields "aaa" (leg_b) first, desc yields "zzz" (leg_a) first.
    asc = await ctx.search.federated(thin_spec).search_page(
        token, pagination={"limit": 10}, sorts={"label": "asc"}
    )
    desc = await ctx.search.federated(thin_spec).search_page(
        token, pagination={"limit": 10}, sorts={"label": "desc"}
    )
    assert ordered(asc) == [(leg_b, str(id_b)), (leg_a, str(id_a))]
    assert ordered(desc) == [(leg_a, str(id_a)), (leg_b, str(id_b))]



class _Numbered(BaseModel):
    id: UUID
    label: str
    n: int


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("a_engine", ["pgroonga_index_first", "pgroonga_filter_first", "fts"])
async def test_federated_thin_pages_match_full_pages_in_order(
    pg_client: PostgresClient, a_engine: str
) -> None:
    """Every page of a thin merge holds the full merge's rows, in its order, deep pages too."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    suffix = uuid4().hex[:8]
    tables: dict[str, str] = {}

    for member in ("a", "b"):
        table = f"fed_tvf_{member}_{suffix}"
        await pg_client.execute(
            f"CREATE TABLE {table} (id uuid PRIMARY KEY, label text NOT NULL, n int NOT NULL)"
        )
        # Ranks repeat every 7 rows, so relevance ties span members and pages.
        await pg_client.execute(
            f"INSERT INTO {table} SELECT gen_random_uuid(), "
            "repeat('tok ', (g % 7) + 1) || 'x' || g, g FROM generate_series(1, 200) g"
        )
        await pg_client.execute(
            f"CREATE INDEX {table}_pg ON {table} USING pgroonga ((ARRAY[label]))"
        )
        await pg_client.execute(
            f"CREATE INDEX {table}_fts ON {table} "
            "USING gin (to_tsvector('english', coalesce(label, '')))"
        )
        tables[member] = table

    def leg(member: str, engine: str) -> PostgresFederatedSearchLegSearch:
        table = tables[member]

        if engine == "fts":
            return PostgresFederatedSearchLegSearch(
                search=PostgresSearchConfig(
                    index=("public", f"{table}_fts"),
                    read=("public", table),
                    engine=FtsEngine(groups={"A": ("label",)}),
                )
            )

        plan = "index_first" if engine.endswith("index_first") else "filter_first"

        return PostgresFederatedSearchLegSearch(
            search=PostgresSearchConfig(
                index=("public", f"{table}_pg"),
                read=("public", table),
                engine=PgroongaEngine(plan=plan),
                candidate_limit=1,
            )
        )

    name_a, name_b = f"a{suffix}", f"b{suffix}"
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                FederatedSearchQueryDepKey: ConfigurablePostgresFederatedSearch(
                    config=PostgresFederatedSearchConfig(
                        members={
                            name_a: leg("a", a_engine),
                            name_b: leg("b", "pgroonga_filter_first"),
                        }
                    )
                ),
            }
        )
    )
    members = (
        SearchSpec(name=name_a, model_type=_Numbered, fields=["label"]),
        SearchSpec(name=name_b, model_type=_Numbered, fields=["label"]),
    )
    full = ctx.search.federated(
        FederatedSearchSpec(name=f"full{suffix}", members=members, thin_merge=False)
    )
    thin = ctx.search.federated(FederatedSearchSpec(name=f"thin{suffix}", members=members))

    for sorts in (None, {"n": "desc"}):
        for offset in (0, 20, 60, 100, 140, 180, 390):
            window = {"limit": 10, "offset": offset}
            want = await full.search_page("tok", pagination=window, sorts=sorts)
            got = await thin.search_page("tok", pagination=window, sorts=sorts)

            assert [(h.member, h.hit.id) for h in got.hits] == [
                (h.member, h.hit.id) for h in want.hits
            ], (sorts, offset)
            assert got.count == want.count == 400


@pytest.mark.asyncio
async def test_federated_search_refuses_to_aggregate(pg_client: PostgresClient) -> None:
    """Members' rows differ in shape, so a federation has no one matched set to measure.

    The refusal comes before any statement, so the members need no tables.
    """

    suffix = uuid4().hex[:10]
    legs = (f"a_{suffix}", f"b_{suffix}")
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                FederatedSearchQueryDepKey: ConfigurablePostgresFederatedSearch(
                    config=PostgresFederatedSearchConfig(
                        members={
                            leg: PostgresFederatedSearchLegSearch(
                                search=PostgresSearchConfig(
                                    index=("public", f"idx_{leg}"),
                                    read=("public", leg),
                                    engine="pgroonga",
                                ),
                            )
                            for leg in legs
                        },
                    ),
                ),
            }
        )
    )
    port = ctx.search.federated(
        FederatedSearchSpec(name=f"fed_agg_{suffix}", members=tuple(_mem(leg) for leg in legs))
    )

    with pytest.raises(CoreException) as refused:
        await port.aggregate_search({"$computed": {"n": {"$count": None}}}, "token")

    assert refused.value.code == UNSUPPORTED_QUERY_FEATURE_CODE
