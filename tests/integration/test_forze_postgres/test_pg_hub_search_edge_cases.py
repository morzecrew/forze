"""Hub search edge cases: browse without sorts, multi-leg OR field maps."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, Page
from forze.application.contracts.search import HubSearchSpec, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps.configs import PostgresHubSearchConfig
from forze_postgres.execution.deps.deps import ConfigurablePostgresHubSearch
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient


class _HubRow(BaseModel):
    id: UUID
    name: str
    display_name: str


class _LegRow(BaseModel):
    name: str = ""
    display_name: str = ""


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_browse_empty_query_without_sorts(pg_client: PostgresClient) -> None:
    """Offset browse on hub heap when query is empty and no ``sorts`` are provided."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    suffix = uuid4().hex[:8]
    ht = f"hub_nosort_{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {ht} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE INDEX idx_{suffix}_ns ON {ht}
        USING pgroonga ((ARRAY[name, display_name]));
        """,
    )
    a, b = uuid4(), uuid4()
    await pg_client.execute(
        f"""
        INSERT INTO {ht} (id, name, display_name) VALUES
        (%(a)s, 'z-last', 'Z'), (%(b)s, 'a-first', 'A')
        """,
        {"a": a, "b": b},
    )

    leg_n = f"leg_{suffix}"
    leg = SearchSpec(name=leg_n, model_type=_LegRow, fields=["name", "display_name"])
    hub_spec = HubSearchSpec(
        name=f"hub_nosort_{suffix}",
        model_type=_HubRow,
        members=(leg,),
    )
    hub_cfg: PostgresHubSearchConfig = {
        "hub": ("public", ht),
        "members": {
            leg_n: {
                "index": ("public", f"idx_{suffix}_ns"),
                "read": ("public", ht),
                "hub_fk": "id",
                "same_heap_as_hub": True,
            },
        },
    }

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            },
        ),
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    page = await adapter.search_page(
        "",
        pagination={"limit": 10, "offset": 0},
    )
    assert isinstance(page, Page)
    assert page.count == 2
    names = {h.name for h in page.hits}
    assert names == {"z-last", "a-first"}

    countless = await adapter.select_search(_HubRow, "")
    assert isinstance(countless, CountlessPage)
    assert len(countless.hits) == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_two_leg_or_with_distinct_field_maps(
    pg_client: PostgresClient,
) -> None:
    """OR hub across two heaps with different PGroonga column layouts."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    suffix = uuid4().hex[:10]
    hub_t = f"hub_or_{suffix}"
    leg_a = f"leg_a_{suffix}"
    leg_b = f"leg_b_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {hub_t} (
            id uuid PRIMARY KEY,
            leg_a_id uuid NOT NULL,
            leg_b_id uuid NOT NULL
        );
        CREATE TABLE {leg_a} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            body text NOT NULL
        );
        CREATE TABLE {leg_b} (
            id uuid PRIMARY KEY,
            headline text NOT NULL,
            summary text NOT NULL
        );
        CREATE INDEX idx_a_{suffix} ON {leg_a}
        USING pgroonga ((ARRAY[title, body]));
        CREATE INDEX idx_b_{suffix} ON {leg_b}
        USING pgroonga ((ARRAY[headline, summary]));
        """,
    )

    hub_id = uuid4()
    a_id, b_id = uuid4(), uuid4()
    await pg_client.execute(
        f"INSERT INTO {hub_t} (id, leg_a_id, leg_b_id) VALUES (%(h)s, %(a)s, %(b)s)",
        {"h": hub_id, "a": a_id, "b": b_id},
    )
    await pg_client.execute(
        f"INSERT INTO {leg_a} (id, title, body) VALUES (%(a)s, 'alpha match', 'body')",
        {"a": a_id},
    )
    await pg_client.execute(
        f"INSERT INTO {leg_b} (id, headline, summary) VALUES (%(b)s, 'other', 'beta match')",
        {"b": b_id},
    )

    class HubLink(BaseModel):
        id: UUID
        leg_a_id: UUID
        leg_b_id: UUID

    class LegA(BaseModel):
        title: str = ""
        body: str = ""

    class LegB(BaseModel):
        headline: str = ""
        summary: str = ""

    leg_a_spec = SearchSpec(
        name=f"la_{suffix}",
        model_type=LegA,
        fields=["title", "body"],
    )
    leg_b_spec = SearchSpec(
        name=f"lb_{suffix}",
        model_type=LegB,
        fields=["headline", "summary"],
    )
    hub_spec = HubSearchSpec(
        name=f"hub_or_spec_{suffix}",
        model_type=HubLink,
        members=(leg_a_spec, leg_b_spec),
    )
    hub_cfg: PostgresHubSearchConfig = {
        "hub": ("public", hub_t),
        "members": {
            leg_a_spec.name: {
                "index": ("public", f"idx_a_{suffix}"),
                "read": ("public", leg_a),
                "hub_fk": "leg_a_id",
            },
            leg_b_spec.name: {
                "index": ("public", f"idx_b_{suffix}"),
                "read": ("public", leg_b),
                "hub_fk": "leg_b_id",
            },
        },
    }

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            },
        ),
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    alpha = await adapter.search_page("alpha")
    assert alpha.count == 1
    assert alpha.hits[0].id == hub_id

    beta = await adapter.search_page("beta")
    assert beta.count == 1
    assert beta.hits[0].id == hub_id
