"""Unit tests for ``forze_postgres.kernel.gateways.search.pgroonga``."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.search.internal.specs import (
    SearchFieldSpecInternal,
    SearchIndexSpecInternal,
    SearchSpecInternal,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName
from forze_postgres.kernel.gateways.search.pgroonga import PostgresPGroongaSearchGateway
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.introspect.types import PostgresIndexInfo
from forze_postgres.kernel.platform import PostgresClient


class SearchDoc(BaseModel):
    id: UUID
    name: str


def _build_gateway() -> tuple[
    PostgresPGroongaSearchGateway[SearchDoc],
    MagicMock,
    MagicMock,
]:
    client = MagicMock(spec=PostgresClient)
    client.fetch_value = AsyncMock(return_value=1)
    client.fetch_all = AsyncMock(
        return_value=[
            {
                "id": UUID("11111111-1111-1111-1111-111111111111"),
                "name": "hello",
            }
        ]
    )

    introspector = MagicMock(spec=PostgresIntrospector)
    introspector.get_index_info = AsyncMock(
        return_value=PostgresIndexInfo(
            schema="public",
            name="docs_search_idx",
            amname="pgroonga",
            engine="pgroonga",
            indexdef="CREATE INDEX docs_search_idx ON public.docs USING pgroonga",
            expr="ARRAY[(COALESCE(name, ''::text))]",
        )
    )

    search_spec = SearchSpecInternal(
        namespace="docs",
        model=SearchDoc,
        indexes={
            "public.docs_search_idx": SearchIndexSpecInternal(
                fields=[SearchFieldSpecInternal(path="name")]
            )
        },
        default_index="public.docs_search_idx",
    )

    gateway = PostgresPGroongaSearchGateway(
        qname=PostgresQualifiedName(schema="public", name="docs"),
        client=client,
        model=SearchDoc,
        introspector=introspector,
        search_spec=search_spec,
    )

    return gateway, client, introspector


def test_pgroonga_order_uses_table_alias_for_default_score() -> None:
    gateway, _, _ = _build_gateway()

    order = gateway._pgroonga_order()
    rendered = repr(order)

    assert "pgroonga_score(" in rendered
    assert "Identifier('t')" in rendered
    assert "tableoid" not in rendered


def test_pgroonga_order_uses_custom_alias() -> None:
    gateway, _, _ = _build_gateway()

    order = gateway._pgroonga_order(table_alias="docs_alias")

    assert "Identifier('docs_alias')" in repr(order)


@pytest.mark.asyncio
async def test_search_uses_table_alias_for_ordered_query() -> None:
    gateway, client, introspector = _build_gateway()

    rows, total = await gateway.search("hello")

    assert total == 1
    assert rows[0].name == "hello"
    introspector.get_index_info.assert_awaited_once_with(
        index="docs_search_idx", schema="public"
    )

    stmt = client.fetch_all.await_args.args[0]
    rendered = repr(stmt)

    assert "pgroonga_score(" in rendered
    # The alias is used both in FROM and in pgroonga_score(alias).
    assert rendered.count("Identifier('t')") >= 2
