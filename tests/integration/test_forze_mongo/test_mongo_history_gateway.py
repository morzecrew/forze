"""Integration tests for :class:`~forze_mongo.kernel.gateways.history.MongoHistoryGateway`."""

from uuid import UUID, uuid4

import pytest

pytest.importorskip("pymongo")

from forze.base.errors import NotFoundError, ValidationError
from forze.base.primitives import utcnow
from forze.domain.constants import (
    HISTORY_DATA_FIELD,
    HISTORY_SOURCE_FIELD,
    ID_FIELD,
    REV_FIELD,
)
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import Document
from forze_mongo.kernel.gateways.history import MongoHistoryGateway
from forze_mongo.kernel.platform import MongoClient


class HistDoc(Document, SoftDeletionMixin):
    title: str


def _doc(pk: UUID, rev: int, title: str) -> HistDoc:
    now = utcnow()
    return HistDoc(
        id=pk,
        rev=rev,
        created_at=now,
        last_update_at=now,
        title=title,
    )


async def _gw(
    client: MongoClient,
    *,
    hist_coll: str,
    target_coll: str,
) -> MongoHistoryGateway[HistDoc]:
    db_name = (await client.db()).name
    return MongoHistoryGateway(
        database=db_name,
        collection=hist_coll,
        target_database=db_name,
        target_collection=target_coll,
        client=client,
        model_type=HistDoc,
        tenant_aware=False,
    )


@pytest.mark.asyncio
async def test_history_read_not_found(mongo_client: MongoClient) -> None:
    gw = await _gw(
        mongo_client,
        hist_coll=f"h_{uuid4().hex[:8]}",
        target_coll=f"t_{uuid4().hex[:8]}",
    )
    with pytest.raises(NotFoundError, match="History not found"):
        await gw.read(uuid4(), 1)


@pytest.mark.asyncio
async def test_history_read_missing_payload(mongo_client: MongoClient) -> None:
    hist_coll = f"h_{uuid4().hex[:8]}"
    target_coll = f"t_{uuid4().hex[:8]}"
    gw = await _gw(mongo_client, hist_coll=hist_coll, target_coll=target_coll)
    db_name = (await mongo_client.db()).name
    full_target = f"{db_name}.{target_coll}"
    pk = uuid4()
    coll = await mongo_client.collection(hist_coll, db_name=db_name)
    await coll.insert_one(
        {
            HISTORY_SOURCE_FIELD: full_target,
            ID_FIELD: str(pk),
            REV_FIELD: 1,
        },
    )
    with pytest.raises(NotFoundError, match="History payload not found"):
        await gw.read(pk, 1)


@pytest.mark.asyncio
async def test_history_read_many_length_mismatch(mongo_client: MongoClient) -> None:
    gw = await _gw(
        mongo_client,
        hist_coll=f"h_{uuid4().hex[:8]}",
        target_coll=f"t_{uuid4().hex[:8]}",
    )
    with pytest.raises(ValidationError, match="same"):
        await gw.read_many([uuid4()], [])


@pytest.mark.asyncio
async def test_history_read_many_empty(mongo_client: MongoClient) -> None:
    gw = await _gw(
        mongo_client,
        hist_coll=f"h_{uuid4().hex[:8]}",
        target_coll=f"t_{uuid4().hex[:8]}",
    )
    assert await gw.read_many([], []) == []


@pytest.mark.asyncio
async def test_history_read_many_skips_missing_and_bad_payload(
    mongo_client: MongoClient,
) -> None:
    hist_coll = f"h_{uuid4().hex[:8]}"
    target_coll = f"t_{uuid4().hex[:8]}"
    gw = await _gw(mongo_client, hist_coll=hist_coll, target_coll=target_coll)
    db_name = (await mongo_client.db()).name
    full_target = f"{db_name}.{target_coll}"
    coll = await mongo_client.collection(hist_coll, db_name=db_name)

    pk_ok = uuid4()
    pk_bad = uuid4()
    pk_nopayload = uuid4()
    snap = _doc(pk_ok, 1, "ok").model_dump(mode="json")

    await coll.insert_many(
        [
            {
                HISTORY_SOURCE_FIELD: full_target,
                ID_FIELD: str(pk_ok),
                REV_FIELD: 1,
                HISTORY_DATA_FIELD: snap,
            },
            {
                HISTORY_SOURCE_FIELD: full_target,
                ID_FIELD: str(pk_nopayload),
                REV_FIELD: 2,
            },
        ],
    )

    out = await gw.read_many([pk_ok, pk_bad, pk_nopayload], [1, 1, 2])
    assert len(out) == 1
    assert out[0].id == pk_ok
    assert out[0].title == "ok"


@pytest.mark.asyncio
async def test_history_write_read_roundtrip(mongo_client: MongoClient) -> None:
    gw = await _gw(
        mongo_client,
        hist_coll=f"h_{uuid4().hex[:8]}",
        target_coll=f"t_{uuid4().hex[:8]}",
    )
    pk = uuid4()
    doc = _doc(pk, 1, "v1")
    await gw.write(doc)
    loaded = await gw.read(pk, 1)
    assert loaded.title == "v1"
    assert loaded.rev == 1


@pytest.mark.asyncio
async def test_history_write_many_bulk_and_empty_noop(
    mongo_client: MongoClient,
) -> None:
    hist_coll = f"h_{uuid4().hex[:8]}"
    target_coll = f"t_{uuid4().hex[:8]}"
    gw = await _gw(mongo_client, hist_coll=hist_coll, target_coll=target_coll)
    await gw.write_many([])

    pk = uuid4()
    await gw.write_many(
        [
            _doc(pk, 1, "a"),
            _doc(pk, 2, "b"),
        ],
    )
    one = await gw.read(pk, 1)
    two = await gw.read(pk, 2)
    assert one.title == "a"
    assert two.title == "b"
