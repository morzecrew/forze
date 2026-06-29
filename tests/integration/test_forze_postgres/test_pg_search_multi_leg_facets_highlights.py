"""Postgres multi-leg facets/highlights (RFC 0006, P6): federated highlights + fail-closed."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    FederatedSearchSpec,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.execution.deps import ConfigurablePostgresFederatedSearch
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
from tests.support.execution_context import context_from_deps

# ----------------------- #


class FedHit(BaseModel):
    id: UUID
    label: str


def _mem(name: str) -> SearchSpec[FedHit]:
    return SearchSpec(name=name, model_type=FedHit, fields=["label"])


async def _bootstrap_federated(
    pg_client: PostgresClient,
) -> tuple[ExecutionContext, FederatedSearchSpec[FedHit]]:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    suffix = uuid4().hex[:10]
    ta, tb = f"fed_hl_a_{suffix}", f"fed_hl_b_{suffix}"
    ia, ib = f"idx_fed_hl_a_{suffix}", f"idx_fed_hl_b_{suffix}"

    for table, idx in ((ta, ia), (tb, ib)):
        await pg_client.execute(
            f"""
            CREATE TABLE {table} (id uuid PRIMARY KEY, label text NOT NULL);
            CREATE INDEX {idx} ON {table} USING pgroonga (label);
            """
        )

    await pg_client.execute(
        f"INSERT INTO {ta} (id, label) VALUES (%(id)s, %(l)s)",
        {"id": uuid4(), "l": "shared book leg-a"},
    )
    await pg_client.execute(
        f"INSERT INTO {tb} (id, label) VALUES (%(id)s, %(l)s)",
        {"id": uuid4(), "l": "shared book leg-b"},
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
    fed_spec = FederatedSearchSpec(
        name=f"fed_hl_{suffix}", members=(_mem(leg_a), _mem(leg_b))
    )
    return ctx, fed_spec


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_highlights_threaded_through_merge(
    pg_client: PostgresClient,
) -> None:
    ctx, fed_spec = await _bootstrap_federated(pg_client)
    fed = ctx.search.federated(fed_spec)

    page = await fed.search_page(
        "book", options={"highlight": {"fields": ["label"]}}
    )

    assert page.count == 2
    assert page.highlights is not None
    assert len(page.highlights) == len(page.hits)
    fragments = [hl["label"][0] for hl in page.highlights if "label" in hl]
    assert fragments
    assert all("<em>" in frag.lower() and "book" in frag.lower() for frag in fragments)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_facets_fail_closed(pg_client: PostgresClient) -> None:
    ctx, fed_spec = await _bootstrap_federated(pg_client)
    fed = ctx.search.federated(fed_spec)

    with pytest.raises(CoreException) as ei:
        await fed.search_page("book", options={"facets": ["label"]})

    assert ei.value.kind is ExceptionKind.PRECONDITION
