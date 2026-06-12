"""Integration tests for the measured Mongo round-trip optimizations.

Covers three behaviors against a real MongoDB:

* ``create`` decodes the inserted payload in memory (no ``find`` read-back)
  and still returns a model identical to a subsequent read, including BSON's
  millisecond datetime truncation.
* ``ensure`` / ``upsert`` insert paths return fully-set models so the document
  adapter's ``hydrate_from_write`` transform works (regression).
* ``claim_pending`` claims a batch in exactly three commands and tolerates
  contention by returning a subset without error.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel
from pymongo import monitoring

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.outbox import (
    IntegrationEvent,
    OutboxSpec,
    OutboxStatus,
    StagedOutboxEntry,
)
from forze.application.execution import Deps
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.adapters.outbox import MongoOutboxStore
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.configs import MongoOutboxConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.execution.deps.utils import doc_write_gw
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps

# ----------------------- #
# Command monitor (registered before any client is created by the fixtures)


class _CommandRecorder(monitoring.CommandListener):
    """Global pymongo command listener; inert unless explicitly enabled."""

    def __init__(self) -> None:
        self.enabled = False
        self.events: list[tuple[str, Any]] = []

    def start(self) -> None:
        self.events = []
        self.enabled = True

    def stop(self) -> None:
        self.enabled = False

    def commands_for(self, collection: str) -> list[str]:
        return [name for name, target in self.events if target == collection]

    def started(self, event: monitoring.CommandStartedEvent) -> None:
        if self.enabled:
            self.events.append(
                (event.command_name, event.command.get(event.command_name))
            )

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        pass

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        pass


_RECORDER = _CommandRecorder()
monitoring.register(_RECORDER)


# ----------------------- #
# Models


class RtDoc(Document):
    name: str
    tags: list[str] = []


class RtRead(ReadDocument):
    name: str
    tags: list[str]


class RtCreate(CreateDocumentCmd):
    name: str


class RtUpdate(BaseDTO):
    name: str | None = None


class _OutboxPayload(BaseModel):
    label: str


# ----------------------- #


def _write_gw(mongo_client: MongoClient, relation: tuple[str, str]):
    ctx = context_from_deps(Deps.plain({MongoClientDepKey: mongo_client}))
    return doc_write_gw(
        ctx,
        write_types=DocumentWriteTypes(
            domain=RtDoc,
            create_cmd=RtCreate,
            update_cmd=RtUpdate,
        ),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_create_skips_read_back_and_matches_subsequent_read(
    mongo_client: MongoClient,
) -> None:
    """``create`` issues no ``find``; its result equals a later read exactly."""

    db_name = (await mongo_client.db()).name
    collection = f"rt_create_{uuid4().hex[:8]}"
    write = _write_gw(mongo_client, (db_name, collection))

    _RECORDER.start()
    created = await write.create(RtCreate(name="no-read-back"))
    _RECORDER.stop()

    commands = _RECORDER.commands_for(collection)
    assert commands == ["insert"], commands  # zero find commands

    loaded = await write.read_gw.get(created.id)
    assert created == loaded  # incl. ms-truncated, naive-UTC datetimes
    assert created.created_at.microsecond % 1000 == 0
    assert created.created_at.tzinfo is None
    assert created.__pydantic_fields_set__ == set(RtDoc.model_fields)

    _RECORDER.start()
    many = await write.create_many([RtCreate(name="m1"), RtCreate(name="m2")])
    _RECORDER.stop()

    assert _RECORDER.commands_for(collection) == ["insert"]
    loaded_many = await write.read_gw.get_many([d.id for d in many])
    assert list(many) == list(loaded_many)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_ensure_and_upsert_insert_paths_hydrate_from_write(
    mongo_client: MongoClient,
) -> None:
    """Adapter ensure/upsert with ``hydrate_from_write=True`` (regression).

    The insert paths used to return the partial ``_from_cdto`` model whose
    default-factory fields were unset; the hydrate transform
    (``exclude={"unset": True}``) then failed read-model validation.
    """

    db_name = (await mongo_client.db()).name
    collection = f"rt_hydrate_{uuid4().hex[:8]}"
    spec = DocumentSpec(
        name="rt_hydrate_ns",
        read=RtRead,
        write={
            "domain": RtDoc,
            "create_cmd": RtCreate,
            "update_cmd": RtUpdate,
        },
    )
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=(db_name, collection),
            write=(db_name, collection),
        )
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    assert getattr(cmd, "hydrate_from_write", None) is True

    ensured_id = uuid4()
    ensured = await cmd.ensure(ensured_id, RtCreate(name="ensured"))
    assert ensured.id == ensured_id
    assert ensured.name == "ensured"
    assert ensured == await query.get(ensured_id)

    upserted_id = uuid4()
    upserted = await cmd.upsert(
        upserted_id,
        RtCreate(name="upsert-insert"),
        RtUpdate(name="not-applied"),
    )
    assert upserted.id == upserted_id
    assert upserted.name == "upsert-insert"
    assert upserted == await query.get(upserted_id)

    # match path still works (delegates to update)
    again = await cmd.upsert(
        upserted_id,
        RtCreate(name="ignored"),
        RtUpdate(name="updated"),
    )
    assert again.name == "updated"
    assert again.rev == 2


# ----------------------- #
# Outbox claim batch


def _staged_rows(route: str, n: int) -> list[StagedOutboxEntry]:
    return [
        StagedOutboxEntry(
            outbox_route=route,
            event=IntegrationEvent(
                event_type="rt.created",
                payload=_OutboxPayload(label=f"row-{i}"),
                event_id=uuid4(),
            ),
            payload_json={"label": f"row-{i}"},
        )
        for i in range(n)
    ]


def _outbox_store(
    mongo_client: MongoClient,
    db_name: str,
    coll_name: str,
    route: str,
) -> MongoOutboxStore[_OutboxPayload]:
    return MongoOutboxStore(
        client=mongo_client,
        spec=OutboxSpec(name=route, codec=PydanticModelCodec(_OutboxPayload)),
        config=MongoOutboxConfig(collection=(db_name, coll_name)),
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_outbox_claim_batch_issues_three_commands(
    mongo_client: MongoClient,
) -> None:
    """One claim batch = find candidates + update_many + find by token."""

    db_name = (await mongo_client.db()).name
    coll_name = f"rt_outbox_{uuid4().hex[:8]}"
    route = "rt_batch"
    store = _outbox_store(mongo_client, db_name, coll_name, route)

    n = 20
    assert await store.persist_rows(_staged_rows(route, n)) == n

    _RECORDER.start()
    claims = await store.claim_pending(limit=n)
    _RECORDER.stop()

    commands = _RECORDER.commands_for(coll_name)
    assert commands == ["find", "update", "find"], commands

    assert len(claims) == n
    assert len({c.id for c in claims}) == n

    coll = await mongo_client.collection(coll_name, db_name=db_name)
    rows = await mongo_client.find_many(coll, {"outbox_route": route})
    tokens = {row["claim_token"] for row in rows}
    assert len(tokens) == 1  # one fresh token per batch
    assert tokens != {None}
    for row in rows:
        assert row["status"] == OutboxStatus.PROCESSING.value
        assert row["processing_at"] is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_outbox_contended_claim_returns_subset_without_error(
    mongo_client: MongoClient,
) -> None:
    """Rows stolen between candidate-find and update are skipped, not errored."""

    db_name = (await mongo_client.db()).name
    coll_name = f"rt_outbox_{uuid4().hex[:8]}"
    route = "rt_contended"
    store = _outbox_store(mongo_client, db_name, coll_name, route)

    n = 10
    assert await store.persist_rows(_staged_rows(route, n)) == n

    stolen_object_ids: list[Any] = []
    original_find_many = MongoClient.find_many
    state = {"stolen": False}

    async def hijacked_find_many(self, coll, flt, **kwargs):  # type: ignore[no-untyped-def]
        res = await original_find_many(self, coll, flt, **kwargs)

        if not state["stolen"] and flt.get("status") == OutboxStatus.PENDING.value:
            state["stolen"] = True
            stolen_object_ids.extend(doc["_id"] for doc in res[:5])
            # a competing relay claims these candidates first
            await coll.update_many(
                {"_id": {"$in": stolen_object_ids}},
                {
                    "$set": {
                        "status": OutboxStatus.PROCESSING.value,
                        "processing_at": utcnow(),
                    }
                },
            )

        return res

    from unittest.mock import patch

    with patch.object(MongoClient, "find_many", hijacked_find_many):
        claims = await store.claim_pending(limit=n)

    assert len(claims) == n - 5  # subset, no error

    coll = await mongo_client.collection(coll_name, db_name=db_name)
    stolen_rows = await mongo_client.find_many(
        coll, {"_id": {"$in": stolen_object_ids}}
    )
    stolen_row_ids = {UUID(str(row["id"])) for row in stolen_rows}
    assert stolen_row_ids.isdisjoint({c.id for c in claims})
    for row in stolen_rows:
        assert "claim_token" not in row  # the contended batch never touched them
