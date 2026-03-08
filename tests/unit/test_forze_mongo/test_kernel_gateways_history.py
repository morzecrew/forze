"""Unit tests for ``forze_mongo.kernel.gateways.history``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.base.errors import CoreError
from forze.domain.models import Document
from forze_mongo.kernel.gateways import MongoHistoryGateway
from forze_mongo.kernel.platform import MongoClient


class MyDoc(Document):
    name: str


def _domain_doc(pk: UUID, *, rev: int = 1, name: str = "item") -> MyDoc:
    now = datetime.now(tz=UTC)
    return MyDoc(id=pk, rev=rev, created_at=now, last_update_at=now, name=name)


def _build_client() -> MagicMock:
    client = MagicMock(spec=MongoClient)
    client.collection.return_value = object()
    client.insert_many = AsyncMock()
    client.find_many = AsyncMock()
    return client


class TestMongoHistoryGateway:
    @pytest.mark.asyncio
    async def test_write_many_persists_history_records(self) -> None:
        client = _build_client()
        gw = MongoHistoryGateway(
            source="docs_history",
            target_source="docs",
            strategy="application",
            client=client,
            model=MyDoc,
        )
        doc = _domain_doc(uuid4(), rev=2, name="beta")

        await gw.write_many([doc])

        payload = client.insert_many.await_args.args[1][0]
        assert payload["source"] == "docs"
        assert payload["id"] == str(doc.id)
        assert payload["rev"] == 2
        assert payload["data"]["id"] == str(doc.id)

    @pytest.mark.asyncio
    async def test_read_many_loads_documents_by_pk_and_rev(self) -> None:
        pk = uuid4()
        now = datetime.now(tz=UTC).isoformat()
        client = _build_client()
        client.find_many.return_value = [
            {
                "source": "docs",
                "id": str(pk),
                "rev": 1,
                "data": {
                    "id": str(pk),
                    "rev": 1,
                    "created_at": now,
                    "last_update_at": now,
                    "name": "alpha",
                },
            }
        ]
        gw = MongoHistoryGateway(
            source="docs_history",
            target_source="docs",
            strategy="application",
            client=client,
            model=MyDoc,
        )

        result = await gw.read_many([pk], [1])

        assert len(result) == 1
        assert result[0].id == pk
        assert result[0].rev == 1

    def test_init_rejects_non_application_history_strategy(self) -> None:
        client = _build_client()

        with pytest.raises(CoreError, match="Invalid history write strategy"):
            MongoHistoryGateway(
                source="docs_history",
                target_source="docs",
                strategy="database",  # pyright: ignore[reportArgumentType]
                client=client,
                model=MyDoc,
            )
