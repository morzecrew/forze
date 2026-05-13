"""Tests for implicit ``LIMIT`` on :class:`~forze_postgres.kernel.gateways.read.PostgresReadGateway`."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.domain.models import Document
from forze_postgres.kernel.gateways import PostgresQualifiedName, PostgresReadGateway
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform import PostgresClient


class _Row(Document):
    title: str


@pytest.mark.asyncio
async def test_find_many_applies_implicit_limit_when_omitted() -> None:
    client = MagicMock(spec=PostgresClient)
    client.fetch_all = AsyncMock(return_value=[])
    client.gather_concurrency_semaphore = MagicMock(
        return_value=asyncio.Semaphore(8),
    )

    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(return_value={})
    intro.cache_partition_key = None

    gw = PostgresReadGateway(
        source_qname=PostgresQualifiedName("public", "t"),
        client=client,
        model_type=_Row,
        introspector=intro,
        tenant_aware=False,
        find_many_implicit_limit=42,
    )

    await gw.find_many(None, limit=None)

    _stmt, params, *_rest = client.fetch_all.await_args[0]
    assert params[-1] == 42
