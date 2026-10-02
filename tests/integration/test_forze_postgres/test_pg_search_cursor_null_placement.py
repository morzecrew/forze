"""Which null placement a Postgres search cursor refuses, and whose mistake it is.

The cursor seeks with a null as the smallest value, so it refuses another placement. Only on
the keys it keeps: a key after the ``id`` orders nothing and is dropped. And a placement in
the spec's ``default_sort`` is the spec author's, not the caller's who sent no sort.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import HubSearchSpec, SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.execution.deps import ConfigurablePostgresHubSearch, ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresSearchConfig,
)
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_FTS = FtsEngine(groups={"A": ("label",)})
_LAST = {"dir": "asc", "nulls": "last"}


class _Row(BaseModel):
    id: UUID
    label: str
    m: int | None = None


class _Leg(BaseModel):
    label: str


async def _port(client: PostgresClient, *, hub: bool, default_sort: Any = None) -> Any:
    table = f"cur_nulls_{uuid4().hex[:10]}"
    await client.execute(
        f"CREATE TABLE {table} (id uuid PRIMARY KEY, label text NOT NULL, m int); "
        f"CREATE INDEX {table}_fts ON {table} USING gin (to_tsvector('english', label)); "
        f"INSERT INTO {table} SELECT gen_random_uuid(), 'alpha', NULLIF(g % 3, 0) "
        "FROM generate_series(1, 6) g;"
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", f"{table}_fts"), read=("public", table), engine=_FTS
                    )
                ),
            }
        )
    )

    if not hub:
        return ctx.search.query(
            SearchSpec(name="rows", model_type=_Row, fields=["label"], default_sort=default_sort)
        )

    config = PostgresHubSearchConfig(
        hub=("public", table),
        members={
            "leg": PostgresHubSearchMemberConfig(
                index=("public", f"{table}_fts"), read=("public", table), hub_fk="id", engine=_FTS
            )
        },
    )

    return ConfigurablePostgresHubSearch(config=config)(
        ctx,
        HubSearchSpec(
            name="rows",
            model_type=_Row,
            members=(SearchSpec(name="leg", model_type=_Leg, fields=["label"]),),
            default_sort=default_sort,
        ),
    )


@pytest.mark.parametrize("hub", [False, True], ids=["single", "hub"])
@pytest.mark.parametrize("query", ["alpha", ""])
async def test_a_placement_after_the_id_is_dropped_not_refused(
    pg_client: PostgresClient, hub: bool, query: str
) -> None:
    port = await _port(pg_client, hub=hub)

    page = await port.search_cursor(query, None, {"limit": 10}, {"id": "asc", "m": _LAST})

    assert [hit.id for hit in page.hits] == sorted(hit.id for hit in page.hits)


@pytest.mark.parametrize("hub", [False, True], ids=["single", "hub"])
async def test_a_placement_the_request_names_is_the_callers_error(
    pg_client: PostgresClient, hub: bool
) -> None:
    port = await _port(pg_client, hub=hub)

    with pytest.raises(CoreException) as refused:
        await port.search_cursor("alpha", None, {"limit": 10}, {"m": _LAST})

    assert refused.value.kind is ExceptionKind.PRECONDITION


@pytest.mark.parametrize("hub", [False, True], ids=["single", "hub"])
async def test_a_placement_the_default_sort_carries_is_the_specs_error(
    pg_client: PostgresClient, hub: bool
) -> None:
    port = await _port(pg_client, hub=hub, default_sort={"m": _LAST})

    with pytest.raises(CoreException) as refused:
        await port.search_cursor("alpha", None, {"limit": 10})

    assert refused.value.kind is ExceptionKind.CONFIGURATION


async def test_an_unknown_cursor_sort_field_is_refused_before_the_pipeline(
    pg_client: PostgresClient,
) -> None:
    port = await _port(pg_client, hub=False)

    with pytest.raises(CoreException) as refused:
        await port.search_cursor("alpha", None, {"limit": 10}, {"nope": "asc"})

    assert refused.value.code == "field_not_on_read_model"
