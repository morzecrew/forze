"""Unit tests for :class:`PostgresHistoryGateway`."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.base.errors import CoreError
from forze.base.errors import NotFoundError
from forze.base.errors import ValidationError
from forze.domain.constants import HISTORY_DATA_FIELD
from forze.domain.models import Document
from forze_postgres.kernel.gateways import PostgresHistoryGateway, PostgresQualifiedName
from forze_postgres.kernel.introspect import PostgresIntrospector, PostgresType
from forze_postgres.kernel.platform.client import PostgresClient

pytest.importorskip("psycopg")


class _HDom(Document):
    name: str


def _history_column_types() -> dict[str, PostgresType]:
    return {
        "source": PostgresType(base="text", is_array=False, not_null=True),
        "id": PostgresType(base="uuid", is_array=False, not_null=True),
        "rev": PostgresType(base="int4", is_array=False, not_null=True),
        "created_at": PostgresType(base="timestamptz", is_array=False, not_null=True),
        "data": PostgresType(base="jsonb", is_array=False, not_null=True),
    }


def _make_doc(*, pk: UUID | None = None, rev: int = 1) -> _HDom:
    pk = pk or uuid4()
    now = datetime.now(tz=UTC)
    return _HDom(
        id=pk,
        rev=rev,
        created_at=now,
        last_update_at=now,
        name="one",
    )


def _gw(
    *,
    client: PostgresClient | MagicMock,
    strategy: str,
    introspector: PostgresIntrospector | MagicMock | None = None,
) -> PostgresHistoryGateway[_HDom]:
    intro = introspector or MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(return_value=_history_column_types())

    return PostgresHistoryGateway(
        qname=PostgresQualifiedName(schema="public", name="hist_t"),
        target_qname=PostgresQualifiedName(schema="public", name="main_t"),
        strategy=strategy,  # type: ignore[arg-type]
        client=client,  # type: ignore[arg-type]
        model_type=_HDom,
        introspector=intro,
        tenant_aware=False,
        tenant_provider=lambda: None,
    )


class TestPostgresHistoryGatewayInit:
    """Construction and validation."""

    def test_invalid_strategy_raises(self) -> None:
        """Unknown bookkeeping strategy is rejected."""
        client = MagicMock(spec=PostgresClient)
        intro = MagicMock(spec=PostgresIntrospector)
        intro.get_column_types = AsyncMock(return_value=_history_column_types())

        with pytest.raises(CoreError, match="Invalid bookkeeping strategy"):
            PostgresHistoryGateway(
                qname=PostgresQualifiedName(schema="public", name="h"),
                target_qname=PostgresQualifiedName(schema="public", name="m"),
                strategy="invalid",  # type: ignore[arg-type]
                client=client,
                model_type=_HDom,
                introspector=intro,
            )


class TestPostgresHistoryGatewayRead:
    """read / read_many."""

    @pytest.mark.asyncio
    async def test_read_returns_domain_from_data_column(self) -> None:
        doc = _make_doc()
        client = MagicMock(spec=PostgresClient)
        client.fetch_one = AsyncMock(
            return_value={
                HISTORY_DATA_FIELD: doc.model_dump(mode="json"),
            }
        )

        gw = _gw(client=client, strategy="application")
        out = await gw.read(doc.id, doc.rev)

        assert out.id == doc.id
        assert out.rev == doc.rev
        assert out.name == doc.name
        client.fetch_one.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_read_missing_row_raises_not_found(self) -> None:
        client = MagicMock(spec=PostgresClient)
        client.fetch_one = AsyncMock(return_value=None)
        gw = _gw(client=client, strategy="application")
        pk = uuid4()

        with pytest.raises(NotFoundError, match="History not found"):
            await gw.read(pk, 1)

    @pytest.mark.asyncio
    async def test_read_many_mismatched_lengths_raises(self) -> None:
        gw = _gw(client=MagicMock(spec=PostgresClient), strategy="application")

        with pytest.raises(ValidationError, match="Length of pks and revs"):
            await gw.read_many([uuid4()], [1, 2])

    @pytest.mark.asyncio
    async def test_read_many_fetches_and_validates(self) -> None:
        d1 = _make_doc()
        d2 = _make_doc()
        client = MagicMock(spec=PostgresClient)
        client.fetch_all = AsyncMock(
            return_value=[
                {HISTORY_DATA_FIELD: d1.model_dump(mode="json")},
                {HISTORY_DATA_FIELD: d2.model_dump(mode="json")},
            ]
        )
        gw = _gw(client=client, strategy="application")

        out = await gw.read_many([d1.id, d2.id], [d1.rev, d2.rev])

        assert len(out) == 2
        assert {x.id for x in out} == {d1.id, d2.id}
        client.fetch_all.assert_awaited_once()


class TestPostgresHistoryGatewayWrite:
    """write / write_many and database no-op path."""

    @pytest.mark.asyncio
    async def test_write_database_strategy_skips_execute(self) -> None:
        doc = _make_doc()
        client = MagicMock(spec=PostgresClient)
        client.execute = AsyncMock()
        gw = _gw(client=client, strategy="database")

        await gw.write(doc)

        client.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_write_application_inserts(self) -> None:
        doc = _make_doc()
        client = MagicMock(spec=PostgresClient)
        client.execute = AsyncMock()
        gw = _gw(client=client, strategy="application")

        await gw.write(doc)

        client.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_write_many_database_skips_execute(self) -> None:
        client = MagicMock(spec=PostgresClient)
        client.execute = AsyncMock()
        gw = _gw(client=client, strategy="database")

        await gw.write_many([_make_doc(), _make_doc()])

        client.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_write_many_application_batches(self) -> None:
        client = MagicMock(spec=PostgresClient)
        client.execute = AsyncMock()
        gw = _gw(client=client, strategy="application")

        docs = [_make_doc(), _make_doc(), _make_doc()]
        await gw.write_many(docs, batch_size=2)

        assert client.execute.await_count == 2
