"""Unit tests for ``forze_postgres.adapters.document``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.domain.models import ReadDocument
from forze_postgres.adapters.document import PostgresDocumentAdapter
from forze_postgres.kernel.gateways import PostgresReadGateway


def _build_read_doc(pk: UUID, *, rev: int = 1) -> ReadDocument:
    now = datetime.now(tz=UTC)
    return ReadDocument(id=pk, rev=rev, created_at=now, last_update_at=now)


def _build_read_gateway() -> MagicMock:
    gateway = MagicMock(spec=PostgresReadGateway)
    gateway.model = ReadDocument
    gateway.client = object()
    gateway.get = AsyncMock()
    gateway.get_many = AsyncMock()
    return gateway


class TestPostgresDocumentAdapter:
    @pytest.mark.asyncio
    async def test_get_falls_back_to_read_gateway_when_cache_get_fails(self) -> None:
        pk = uuid4()
        expected = _build_read_doc(pk)
        read_gw = _build_read_gateway()
        read_gw.get.return_value = expected

        cache = MagicMock()
        cache.get = AsyncMock(side_effect=RuntimeError("cache unavailable"))
        cache.set_versioned = AsyncMock()

        adapter = PostgresDocumentAdapter(read_gw=read_gw, cache=cache)

        result = await adapter.get(pk, for_update=True)

        assert result == expected
        read_gw.get.assert_awaited_once_with(pk, for_update=True, return_fields=None)
        cache.set_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_ignores_cache_set_failure_after_read(self) -> None:
        pk = uuid4()
        expected = _build_read_doc(pk, rev=3)
        read_gw = _build_read_gateway()
        read_gw.get.return_value = expected

        cache = MagicMock()
        cache.get = AsyncMock(return_value=None)
        cache.set_versioned = AsyncMock(side_effect=RuntimeError("cache unavailable"))

        adapter = PostgresDocumentAdapter(read_gw=read_gw, cache=cache)

        result = await adapter.get(pk)

        assert result == expected
        read_gw.get.assert_awaited_once_with(pk)
        cache.set_versioned.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_many_falls_back_to_read_gateway_when_cache_get_many_fails(
        self,
    ) -> None:
        pks = [uuid4(), uuid4()]
        expected = [_build_read_doc(pk, rev=i + 1) for i, pk in enumerate(pks)]
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = expected

        cache = MagicMock()
        cache.get_many = AsyncMock(side_effect=RuntimeError("cache unavailable"))
        cache.set_many_versioned = AsyncMock()

        adapter = PostgresDocumentAdapter(read_gw=read_gw, cache=cache)

        result = await adapter.get_many(pks)

        assert result == expected
        read_gw.get_many.assert_awaited_once_with(pks, return_fields=None)
        cache.set_many_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_many_ignores_cache_set_many_failure_after_cache_misses(
        self,
    ) -> None:
        pks = [uuid4(), uuid4()]
        expected = [_build_read_doc(pk, rev=i + 1) for i, pk in enumerate(pks)]
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = expected

        cache = MagicMock()
        cache.get_many = AsyncMock(return_value=({}, [str(pk) for pk in pks]))
        cache.set_many_versioned = AsyncMock(
            side_effect=RuntimeError("cache unavailable")
        )

        adapter = PostgresDocumentAdapter(read_gw=read_gw, cache=cache)

        result = await adapter.get_many(pks)

        assert result == expected
        read_gw.get_many.assert_awaited_once_with(pks)
        cache.set_many_versioned.assert_awaited_once()
