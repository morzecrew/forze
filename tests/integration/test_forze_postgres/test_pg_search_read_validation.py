"""Integration tests for Postgres search ``read_validation`` (strict vs trusted)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, ValidationError, field_validator

from forze.application.contracts.search import (
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

ReadValidation = Literal["strict", "trusted"]

# ----------------------- #


class SearchableModel(BaseModel):
    id: UUID
    title: str
    content: str


class _StrictTitleModel(BaseModel):
    id: UUID
    title: str
    content: str

    @field_validator("title")
    @classmethod
    def title_must_be_long(cls, value: str) -> str:
        if len(value) < 20:
            msg = "title too short for strict validation"
            raise ValueError(msg)

        return value


class _HubLegTxt(BaseModel):
    name: str = ""
    display_name: str = ""


class _SameHeapHubRow(BaseModel):
    id: UUID
    name: str
    display_name: str


# ....................... #


def _search_ctx(
    pg_client: PostgresClient,
    *,
    table: str,
    index: str,
    read_validation: ReadValidation = "strict",
):
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index),
                        read=("public", table),
                        engine="pgroonga",
                        read_validation=read_validation,
                    )
                ),
            }
        )
    )


def _hub_member(**kwargs: object) -> PostgresHubSearchMemberConfig:
    if "engine" not in kwargs:
        kwargs["engine"] = "pgroonga"
    return PostgresHubSearchMemberConfig(**kwargs)  # type: ignore[misc]


def _hub_config(
    *,
    hub: tuple[str, str],
    members: dict[object, PostgresHubSearchMemberConfig],
    execution: str = "sql",
    read_validation: ReadValidation = "strict",
) -> PostgresHubSearchConfig:
    return PostgresHubSearchConfig(
        hub=hub,
        members=members,
        execution=execution,  # type: ignore[arg-type]
        read_validation=read_validation,
    )


async def _setup_pgroonga_items(
    pg_client: PostgresClient,
    suffix: str,
) -> tuple[str, str, list[dict[str, object]]]:
    table = f"rv_items_{suffix}"
    index = f"idx_rv_items_{suffix}"
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        CREATE INDEX {index}
        ON {table} USING pgroonga ((ARRAY[title, content]));
        """
    )
    docs = [
        {
            "id": uuid4(),
            "title": "Forze Framework",
            "content": "Hexagonal architecture framework in python",
        },
        {
            "id": uuid4(),
            "title": "Postgres Guide",
            "content": "How to use postgres with python",
        },
        {
            "id": uuid4(),
            "title": "Python Tips",
            "content": "Advanced python development",
        },
    ]
    for doc in docs:
        await pg_client.execute(
            f"""
            INSERT INTO {table} (id, title, content)
            VALUES (%(id)s, %(title)s, %(content)s)
            """,
            doc,
        )
    return table, index, docs


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_search_trusted_matches_strict_hits(
    pg_client: PostgresClient,
) -> None:
    suffix = uuid4().hex[:10]
    table, index, docs = await _setup_pgroonga_items(pg_client, suffix)
    spec = SearchSpec(
        name=f"rv_strict_{suffix}",
        model_type=SearchableModel,
        fields=["title", "content"],
    )

    strict_ctx = _search_ctx(
        pg_client, table=table, index=index, read_validation="strict"
    )
    trusted_ctx = _search_ctx(
        pg_client, table=table, index=index, read_validation="trusted"
    )

    strict_page = await strict_ctx.search.query(spec).search_page("python")
    trusted_page = await trusted_ctx.search.query(spec).search_page("python")

    assert strict_page.count == trusted_page.count == 3
    assert {h.id for h in strict_page.hits} == {h.id for h in trusted_page.hits}
    assert {h.id for h in strict_page.hits} == {d["id"] for d in docs}

    strict_one = await strict_ctx.search.query(spec).search_page("hexagonal")
    trusted_one = await trusted_ctx.search.query(spec).search_page("hexagonal")
    assert strict_one.count == trusted_one.count == 1
    assert strict_one.hits[0].title == trusted_one.hits[0].title == "Forze Framework"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_search_trusted_runs_field_validator(
    pg_client: PostgresClient,
) -> None:
    """Trusted decode trusts columns only — values are still validated.

    Rows violating the read model raise on both paths; ``trusted`` no longer
    constructs invalid model instances (it used to skip field validators).
    """

    suffix = uuid4().hex[:10]
    table, index, _docs = await _setup_pgroonga_items(pg_client, suffix)
    spec = SearchSpec(
        name=f"rv_validator_{suffix}",
        model_type=_StrictTitleModel,
        fields=["title", "content"],
    )

    strict_ctx = _search_ctx(
        pg_client, table=table, index=index, read_validation="strict"
    )
    trusted_ctx = _search_ctx(
        pg_client, table=table, index=index, read_validation="trusted"
    )

    with pytest.raises(ValidationError):
        await strict_ctx.search.query(spec).search_page("python")

    with pytest.raises(ValidationError):
        await trusted_ctx.search.query(spec).search_page("python")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pgroonga_snapshot_trusted_reread(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    suffix = uuid4().hex[:10]
    table, index, _docs = await _setup_pgroonga_items(pg_client, suffix)
    ns = f"it:rv:snap:{uuid4().hex[:10]}"
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
                        index=("public", index),
                        read=("public", table),
                        heap=("public", table),
                        engine="pgroonga",
                        read_validation="trusted",
                    )
                ),
            }
        )
    )
    spec = SearchSpec(
        name=f"rv_snap_{suffix}",
        model_type=SearchableModel,
        fields=["title", "content"],
        snapshot=SearchResultSnapshotSpec(
            name="snap",
            enabled=True,
            ttl=timedelta(minutes=5),
        ),
    )
    adapter = ctx.search.query(spec)
    assert isinstance(adapter, PostgresPGroongaSearchAdapter)
    assert adapter.read_validation == "trusted"

    first = await adapter.search_page(
        "python",
        pagination={"limit": 5, "offset": 0},
    )
    assert first.snapshot is not None
    assert first.count == 3

    second = await adapter.search_page(
        "python",
        pagination={"limit": 5, "offset": 0},
        snapshot={"id": first.snapshot.id, "fingerprint": first.snapshot.fingerprint},
    )
    assert second.count == first.count
    assert {h.id for h in second.hits} == {h.id for h in first.hits}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_parallel_search_trusted(
    pg_client: PostgresClient,
) -> None:
    """Parallel hub leg merge with ``read_validation=trusted`` materializes hits."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    suffix = uuid4().hex[:8]
    ht = f"hub_rv_par_{suffix}"
    token = f"rv-token-{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {ht} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE INDEX idx_{suffix}_rvpar ON {ht}
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )
    for i in range(10):
        await pg_client.execute(
            f"""
            INSERT INTO {ht} (id, name, display_name)
            VALUES (%(id)s, %(n)s, %(d)s)
            """,
            {"id": uuid4(), "n": f"{token}-{i}", "d": "display"},
        )

    leg_n = f"rvpar_{suffix}"
    doc_leg = SearchSpec(
        name=leg_n,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    hub_spec = HubSearchSpec(
        name=f"hub_rvpar_{suffix}",
        model_type=_SameHeapHubRow,
        members=(doc_leg,),
    )
    strict_cfg = _hub_config(
        hub=("public", ht),
        members={
            leg_n: _hub_member(
                index=("public", f"idx_{suffix}_rvpar"),
                read=("public", ht),
                hub_fk="id",
                same_heap_as_hub=True,
                engine="pgroonga",
            ),
        },
        execution="parallel",
        read_validation="strict",
    )
    trusted_cfg = _hub_config(
        hub=("public", ht),
        members={
            leg_n: _hub_member(
                index=("public", f"idx_{suffix}_rvpar"),
                read=("public", ht),
                hub_fk="id",
                same_heap_as_hub=True,
                engine="pgroonga",
            ),
        },
        execution="parallel",
        read_validation="trusted",
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    strict_adapter = ConfigurablePostgresHubSearch(config=strict_cfg)(ctx, hub_spec)
    trusted_adapter = ConfigurablePostgresHubSearch(config=trusted_cfg)(ctx, hub_spec)
    assert trusted_adapter.read_validation == "trusted"

    kwargs = {
        "pagination": {"limit": 4},
        "options": {"search_count": "exact"},
    }
    strict_page = await strict_adapter.search_page(token, **kwargs)
    trusted_page = await trusted_adapter.search_page(token, **kwargs)

    assert strict_page.count == trusted_page.count == 10
    assert len(strict_page.hits) == len(trusted_page.hits) == 4
    assert {h.id for h in strict_page.hits} == {h.id for h in trusted_page.hits}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_parallel_cursor_uuid_sort(
    pg_client: PostgresClient,
) -> None:
    """Parallel hub cursor pagination compares UUID sort keys against wire tokens."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    suffix = uuid4().hex[:8]
    ht = f"hub_rv_cur_{suffix}"
    query_token = f"rv-cur-{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {ht} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE INDEX idx_{suffix}_rvcur ON {ht}
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )
    for i in range(8):
        await pg_client.execute(
            f"""
            INSERT INTO {ht} (id, name, display_name)
            VALUES (%(id)s, %(n)s, 'display')
            """,
            {"id": uuid4(), "n": f"{query_token}-{i}"},
        )

    leg_n = f"rvcur_{suffix}"
    doc_leg = SearchSpec(
        name=leg_n,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    hub_spec = HubSearchSpec(
        name=f"hub_rvcur_{suffix}",
        model_type=_SameHeapHubRow,
        members=(doc_leg,),
    )
    hub_cfg = _hub_config(
        hub=("public", ht),
        members={
            leg_n: _hub_member(
                index=("public", f"idx_{suffix}_rvcur"),
                read=("public", ht),
                hub_fk="id",
                same_heap_as_hub=True,
                engine="pgroonga",
            ),
        },
        execution="parallel",
        read_validation="trusted",
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    collected: list[Any] = []
    next_c: str | None = None
    for _ in range(10):
        cur: dict[str, Any] = {"limit": 3}
        if next_c is not None:
            cur["after"] = next_c
        page = await adapter.project_search_cursor(
            ["id", "name", "display_name"],
            query_token,
            sorts={"id": "desc"},
            cursor=cur,
        )
        assert len(page.hits) > 0
        collected.extend(h["id"] for h in page.hits)
        if not page.has_more:
            break
        assert page.next_cursor is not None
        next_c = page.next_cursor

    assert len(collected) == 8
    assert len({str(x) for x in collected}) == 8
