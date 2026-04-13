"""Unit tests for ``forze_postgres.kernel.gateways.history``."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.base.errors import CoreError, NotFoundError, ValidationError
from forze.domain.models import Document
from forze_postgres.kernel.gateways import PostgresQualifiedName
from forze_postgres.kernel.gateways.history import PostgresHistoryGateway
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform import PostgresClient


class MyDoc(Document):
    name: str


def _build_gateway(strategy: str = "database") -> tuple[PostgresHistoryGateway[MyDoc], MagicMock]:
    client = MagicMock(spec=PostgresClient)
    client.fetch_one = AsyncMock()
    client.fetch_all = AsyncMock()
    client.execute = AsyncMock()

    introspector = MagicMock(spec=PostgresIntrospector)
    introspector.get_column_types = AsyncMock(return_value={})

    qname = PostgresQualifiedName(schema="public", name="docs_history")
    target_qname = PostgresQualifiedName(schema="public", name="docs")

    gw = PostgresHistoryGateway(
        qname=qname,
        client=client,
        model=MyDoc,
        introspector=introspector,
        target_qname=target_qname,
        strategy=strategy,  # type: ignore
    )
    return gw, client


@pytest.mark.asyncio
async def test_init_invalid_strategy() -> None:
    with pytest.raises(CoreError, match="Invalid history write strategy"):
        _build_gateway(strategy="invalid")


@pytest.mark.asyncio
async def test_read_success() -> None:
    gw, client = _build_gateway()
    pk = UUID("11111111-1111-1111-1111-111111111111")
    rev = 1

    client.fetch_one.return_value = {
        "_data": {
            "id": str(pk),
            "rev": rev,
            "name": "test",
            "created_at": "2025-01-01T00:00:00Z",
            "last_update_at": "2025-01-01T00:00:00Z",
        }
    }

    result = await gw.read(pk, rev)

    assert isinstance(result, MyDoc)
    assert result.id == pk
    assert result.rev == rev
    assert result.name == "test"

    client.fetch_one.assert_called_once()


@pytest.mark.asyncio
async def test_read_not_found() -> None:
    gw, client = _build_gateway()
    client.fetch_one.return_value = None

    pk = UUID("11111111-1111-1111-1111-111111111111")
    with pytest.raises(NotFoundError, match="History not found"):
        await gw.read(pk, 1)


@pytest.mark.asyncio
async def test_read_many_success() -> None:
    gw, client = _build_gateway()
    pk1 = UUID("11111111-1111-1111-1111-111111111111")
    pk2 = UUID("22222222-2222-2222-2222-222222222222")

    client.fetch_all.return_value = [
        {
            "_data": {
                "id": str(pk1),
                "rev": 1,
                "name": "test1",
                "created_at": "2025-01-01T00:00:00Z",
                "last_update_at": "2025-01-01T00:00:00Z",
            }
        },
        {
            "_data": {
                "id": str(pk2),
                "rev": 2,
                "name": "test2",
                "created_at": "2025-01-01T00:00:00Z",
                "last_update_at": "2025-01-01T00:00:00Z",
            }
        }
    ]

    results = await gw.read_many([pk1, pk2], [1, 2])

    assert len(results) == 2
    assert results[0].name == "test1"
    assert results[1].name == "test2"


@pytest.mark.asyncio
async def test_read_many_length_mismatch() -> None:
    gw, _ = _build_gateway()
    with pytest.raises(ValidationError, match="Length of pks and revs must be the same"):
        await gw.read_many([UUID("11111111-1111-1111-1111-111111111111")], [1, 2])


@pytest.mark.asyncio
async def test_write_database_strategy_is_noop() -> None:
    gw, client = _build_gateway(strategy="database")
    doc = MyDoc(id=UUID("11111111-1111-1111-1111-111111111111"), rev=1, name="test")

    await gw.write(doc)
    client.execute.assert_not_called()

    await gw.write_many([doc])
    client.execute.assert_not_called()


@pytest.mark.asyncio
async def test_write_application_strategy() -> None:
    gw, client = _build_gateway(strategy="application")
    doc = MyDoc(id=UUID("11111111-1111-1111-1111-111111111111"), rev=1, name="test")

    await gw.write(doc)
    assert client.execute.call_count == 1

    # Check that it's an INSERT statement
    stmt = client.execute.call_args[0][0]
    assert "INSERT INTO" in str(stmt)


@pytest.mark.asyncio
async def test_write_many_application_strategy() -> None:
    gw, client = _build_gateway(strategy="application")
    doc1 = MyDoc(id=UUID("11111111-1111-1111-1111-111111111111"), rev=1, name="test1")
    doc2 = MyDoc(id=UUID("22222222-2222-2222-2222-222222222222"), rev=2, name="test2")

    await gw.write_many([doc1, doc2], batch_size=1)
    assert client.execute.call_count == 2
