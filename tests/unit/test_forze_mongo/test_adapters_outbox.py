"""Unit tests for :class:`MongoOutboxStore` document/claim mapping (mocked client)."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    IntegrationEvent,
    OutboxSpec,
    StagedOutboxEntry,
)
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze_mongo.adapters.outbox import MongoOutboxStore, _claim_from_doc
from forze_mongo.execution.deps.configs import MongoOutboxConfig

# ----------------------- #


class _Payload(BaseModel):
    label: str


def _store(client: AsyncMock) -> MongoOutboxStore[_Payload]:
    return MongoOutboxStore(
        client=client,
        spec=OutboxSpec(name="events", codec=PydanticModelCodec(_Payload)),
        config=MongoOutboxConfig(collection=("app", "outbox")),
    )


def _entry(*, ordering_key: str | None) -> StagedOutboxEntry:
    return StagedOutboxEntry(
        outbox_route="events",
        event=IntegrationEvent(
            event_type="demo.created",
            payload=_Payload(label="x"),
            event_id=uuid4(),
            ordering_key=ordering_key,
        ),
        payload_json={"label": "x"},
    )


# ----------------------- #


@pytest.mark.asyncio
async def test_persist_rows_writes_ordering_key_field() -> None:
    client = AsyncMock()
    client.find_many = AsyncMock(return_value=[])  # no existing event ids
    client.insert_many = AsyncMock(side_effect=lambda _c, docs, **_kw: list(docs))

    keyed = _entry(ordering_key="agg-1")
    unkeyed = _entry(ordering_key=None)

    assert await _store(client).persist_rows([keyed, unkeyed]) == 2

    documents = client.insert_many.await_args.args[1]
    by_event_id = {doc["event_id"]: doc for doc in documents}
    assert by_event_id[str(keyed.event.event_id)]["ordering_key"] == "agg-1"
    assert by_event_id[str(unkeyed.event.event_id)]["ordering_key"] is None


@pytest.mark.asyncio
async def test_claim_pending_returns_ordering_key_from_doc() -> None:
    client = AsyncMock()
    event_id = uuid4()
    claimed_doc = {
        "_id": 1,
        "id": str(uuid4()),
        "outbox_route": "events",
        "event_id": str(event_id),
        "event_type": "demo.created",
        "payload": {"label": "x"},
        "occurred_at": utcnow(),
        "attempts": 0,
        "ordering_key": "agg-1",
    }
    # First find: claim candidates; second find: read back by claim token.
    client.find_many = AsyncMock(side_effect=[[{"_id": 1}], [claimed_doc]])
    client.update_many = AsyncMock(return_value=1)

    [claim] = await _store(client).claim_pending()

    assert claim.ordering_key == "agg-1"
    assert claim.event_id == event_id


def test_claim_from_doc_defaults_ordering_key_to_none() -> None:
    claim = _claim_from_doc(
        {
            "id": str(uuid4()),
            "outbox_route": "events",
            "event_id": str(uuid4()),
            "event_type": "demo.created",
            "payload": {"label": "x"},
        }
    )

    assert claim.ordering_key is None
