"""Unit tests for ``forze_postgres.kernel.gateways.search.pgroonga``."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec, parse_search_spec
from forze_postgres.kernel.gateways import (
    PostgresPGroongaSearchGateway,
    PostgresQualifiedName,
)
from forze_postgres.kernel.introspect import PostgresIndexInfo, PostgresIntrospector
from forze_postgres.kernel.platform import PostgresClient


class SearchDoc(BaseModel):
    id: UUID
    title: str


def _build_gateway() -> tuple[PostgresPGroongaSearchGateway[SearchDoc], MagicMock]:
    client = MagicMock(spec=PostgresClient)
    client.fetch_value = AsyncMock(return_value=1)
    client.fetch_all = AsyncMock(
        return_value=[{"id": UUID("11111111-1111-1111-1111-111111111111")}]
    )

    introspector = MagicMock(spec=PostgresIntrospector)
    introspector.get_index_info = AsyncMock(
        return_value=PostgresIndexInfo(
            schema="public",
            name="idx_docs_pgroonga",
            amname="pgroonga",
            engine="pgroonga",
            indexdef=(
                "CREATE INDEX idx_docs_pgroonga ON public.docs USING pgroonga (title)"
            ),
            expr=None,
        )
    )

    search_spec = parse_search_spec(
        SearchSpec(
            namespace="search_ns",
            model=SearchDoc,
            indexes={
                "public.idx_docs_pgroonga": {
                    "source": "public.docs",
                    "mode": "pgroonga",
                    "fields": [{"path": "title"}],
                }
            },
            default_index="public.idx_docs_pgroonga",
        ),
        raise_if_no_sources=True,
    )

    gateway = PostgresPGroongaSearchGateway(
        qname=PostgresQualifiedName(schema="public", name="docs"),
        client=client,
        model=SearchDoc,
        introspector=introspector,
        search_spec=search_spec,
    )
    return gateway, client


def test_pgroonga_order_uses_explicit_table_alias_for_score() -> None:
    gateway, _ = _build_gateway()

    order = gateway._pgroonga_order(table_alias="docs_alias")
    order_repr = repr(order)

    assert "pgroonga_score(" in order_repr
    assert "Identifier('docs_alias')" in order_repr


@pytest.mark.asyncio
async def test_search_uses_table_alias_in_from_and_pgroonga_score() -> None:
    gateway, client = _build_gateway()

    rows, total = await gateway.search(query="python", return_fields=["id"])

    assert total == 1
    assert rows == [{"id": UUID("11111111-1111-1111-1111-111111111111")}]

    stmt = client.fetch_all.await_args.args[0]
    stmt_repr = repr(stmt)

    # The alias "t" must be present in both FROM and pgroonga_score(...)
    assert "pgroonga_score(" in stmt_repr
    assert stmt_repr.count("Identifier('t')") >= 2
