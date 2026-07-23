"""Integration tests for :class:`~forze_mongo.kernel.gateways.history.MongoHistoryGateway`."""

from uuid import UUID, uuid4

import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("pymongo")

from forze.base.primitives import utcnow
from forze.domain.constants import (
    HISTORY_DATA_FIELD,
    HISTORY_SOURCE_FIELD,
    ID_FIELD,
    REV_FIELD,
)
from forze.domain.models import Document
from forze_kits.domain.soft_deletion import SoftDeletionMixin
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.gateways.history import MongoHistoryGateway
from tests.unit._gateway_codec_helpers import history_codecs_for


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
    domain_codec, history_codec = history_codecs_for(HistDoc)
    return MongoHistoryGateway(
        relation=(db_name, hist_coll),
        target_relation=(db_name, target_coll),
        client=client,
        model_type=HistDoc,
        codec=domain_codec,
        history_codec=history_codec,
        tenant_aware=False,
    )

@pytest.mark.asyncio
async def test_history_read_not_found(mongo_client: MongoClient) -> None:
    gw = await _gw(
        mongo_client,
        hist_coll=f"h_{uuid4().hex[:8]}",
        target_coll=f"t_{uuid4().hex[:8]}",
    )
    with pytest.raises(CoreException, match="History not found"):
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
    with pytest.raises(CoreException, match="History payload not found"):
        await gw.read(pk, 1)

@pytest.mark.asyncio
async def test_history_read_many_length_mismatch(mongo_client: MongoClient) -> None:
    gw = await _gw(
        mongo_client,
        hist_coll=f"h_{uuid4().hex[:8]}",
        target_coll=f"t_{uuid4().hex[:8]}",
    )
    with pytest.raises(CoreException, match="same"):
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


# ....................... #
# Tagged-tier tenant isolation on the history read path


from forze.application.contracts.tenancy import TenantIdentity


async def _tenant_gw(
    client: MongoClient,
    *,
    hist_coll: str,
    target_coll: str,
    tenant: UUID,
) -> MongoHistoryGateway[HistDoc]:
    db_name = (await client.db()).name
    domain_codec, history_codec = history_codecs_for(HistDoc)
    return MongoHistoryGateway(
        relation=(db_name, hist_coll),
        target_relation=(db_name, target_coll),
        client=client,
        model_type=HistDoc,
        codec=domain_codec,
        history_codec=history_codec,
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
    )


@pytest.mark.asyncio
async def test_history_read_is_tenant_scoped(mongo_client: MongoClient) -> None:
    """Under tagged tenancy a snapshot is stamped AND read-filtered by tenant, so tenant
    B cannot read tenant A's history for a guessed (pk, rev). The Firestore twin filtered;
    the Mongo read did not — this is the bypass."""

    hist_coll = f"h_{uuid4().hex[:8]}"
    target_coll = f"t_{uuid4().hex[:8]}"
    a, b = uuid4(), uuid4()

    gw_a = await _tenant_gw(mongo_client, hist_coll=hist_coll, target_coll=target_coll, tenant=a)
    gw_b = await _tenant_gw(mongo_client, hist_coll=hist_coll, target_coll=target_coll, tenant=b)

    pk = uuid4()
    await gw_a.write(_doc(pk, 1, "tenant-a-secret"))

    # tenant A reads its own snapshot
    assert (await gw_a.read(pk, 1)).title == "tenant-a-secret"

    # tenant B, with the exact same (pk, rev), gets nothing — not A's snapshot
    with pytest.raises(CoreException):
        await gw_b.read(pk, 1)

    # read_many is filtered too: B sees none of A's rows
    assert await gw_b.read_many([pk], [1]) == []
    assert [d.title for d in await gw_a.read_many([pk], [1])] == ["tenant-a-secret"]


@pytest.mark.asyncio
async def test_history_read_isolates_legacy_records_without_tenant_id(
    mongo_client: MongoClient,
) -> None:
    """A tenant-aware history read is strict: a pre-upgrade "legacy" revision that carries
    no ``tenant_id`` is unowned, so it must be invisible to *every* tenant — a tolerant
    ``$exists: false`` branch would hand tenant A's snapshot to tenant B for a guessed
    ``(pk, rev)``. The lost pre-upgrade OCC continuity is intentional and self-healing
    (an ordinary update re-stamps the current revision)."""

    from forze.application.contracts.tenancy import TENANT_ID_FIELD

    hist_coll = f"h_{uuid4().hex[:8]}"
    target_coll = f"t_{uuid4().hex[:8]}"
    a, b = uuid4(), uuid4()
    gw_a = await _tenant_gw(mongo_client, hist_coll=hist_coll, target_coll=target_coll, tenant=a)
    gw_b = await _tenant_gw(mongo_client, hist_coll=hist_coll, target_coll=target_coll, tenant=b)

    db_name = (await mongo_client.db()).name
    coll = await mongo_client.collection(hist_coll, db_name=db_name)

    # Write a valid record, then strip its tenant_id to simulate a pre-upgrade "legacy" row.
    legacy_pk = uuid4()
    await gw_a.write(_doc(legacy_pk, 1, "legacy-value"))
    await coll.update_one(
        {ID_FIELD: str(legacy_pk), REV_FIELD: 1},
        {"$unset": {TENANT_ID_FIELD: ""}},
    )

    # The unowned legacy revision is invisible to every tenant — including the writer.
    with pytest.raises(CoreException):
        await gw_a.read(legacy_pk, 1)
    assert await gw_a.read_many([legacy_pk], [1]) == []
    with pytest.raises(CoreException):
        await gw_b.read(legacy_pk, 1)
    assert await gw_b.read_many([legacy_pk], [1]) == []

    # A newly written (stamped) record stays isolated: tenant B cannot read tenant A's.
    new_pk = uuid4()
    await gw_a.write(_doc(new_pk, 1, "new-isolated"))
    assert (await gw_a.read(new_pk, 1)).title == "new-isolated"
    with pytest.raises(CoreException):
        await gw_b.read(new_pk, 1)
