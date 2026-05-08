"""Integration tests for :class:`~forze_mongo.kernel.platform.RoutedMongoClient`."""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from pymongo import InsertOne

pytest.importorskip("pymongo")
pytest.importorskip("testcontainers.mongodb")

from testcontainers.mongodb import MongoDbContainer

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import InfrastructureError, SecretNotFoundError

from forze_mongo.kernel.platform import RoutedMongoClient
from forze_mongo.kernel.platform.client import MongoClient


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/mongo")


class _MemSecrets:
    def __init__(
        self,
        uris: dict[UUID, str],
        *,
        missing_tenant: UUID | None = None,
        broken_tenant: UUID | None = None,
    ) -> None:
        self._uris = uris
        self._missing_tenant = missing_tenant
        self._broken_tenant = broken_tenant

    async def resolve_str(self, ref: SecretRef) -> str:
        if self._broken_tenant is not None:
            tid = self._tid_for_ref(ref)
            if tid == self._broken_tenant:
                raise RuntimeError("vault unavailable")
        if self._missing_tenant is not None:
            tid = self._tid_for_ref(ref)
            if tid == self._missing_tenant:
                raise SecretNotFoundError(
                    f"No secret for {ref.path!r}",
                    details={"ref": ref.path},
                )
        for tid, uri in self._uris.items():
            if ref.path == f"tenants/{tid}/mongo":
                return uri
        raise SecretNotFoundError(
            f"No secret for {ref.path!r}",
            details={"ref": ref.path},
        )

    def _tid_for_ref(self, ref: SecretRef) -> UUID | None:
        prefix = "tenants/"
        suffix = "/mongo"
        if not ref.path.startswith(prefix) or not ref.path.endswith(suffix):
            return None
        try:
            return UUID(ref.path[len(prefix) : -len(suffix)])
        except ValueError:
            return None

    async def exists(self, ref: SecretRef) -> bool:
        tid = self._tid_for_ref(ref)
        return tid is not None and tid in self._uris


def _tenant_holder() -> tuple[Callable[[], UUID | None], Callable[[UUID | None], None]]:
    slot: list[UUID | None] = [None]

    def getter() -> UUID | None:
        return slot[0]

    def setter(value: UUID | None) -> None:
        slot[0] = value

    return getter, setter


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_mongo_health_crud_roundtrip(mongo_container: MongoDbContainer) -> None:
    uri = mongo_container.get_connection_url()
    t1 = uuid4()
    secrets = _MemSecrets({t1: uri})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        database_name_for_tenant=lambda _tid: f"rt_{uuid4().hex[:10]}",
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        status, ok = await routed.health()
        assert status == "ok"
        assert ok is True

        coll_name = f"c_{uuid4().hex[:8]}"
        coll = await routed.collection(coll_name)
        oid = await routed.insert_one(coll, {"n": 1})
        doc = await routed.find_one(coll, {"_id": oid})
        assert doc is not None and doc.get("n") == 1
        assert await routed.count(coll, {"n": 1}) == 1
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_mongo_port_delegators_roundtrip(mongo_container: MongoDbContainer) -> None:
    """Exercise routed wrappers over :class:`MongoClient` query helpers."""

    uri = mongo_container.get_connection_url()
    t1 = uuid4()
    secrets = _MemSecrets({t1: uri})
    tenant_get, tenant_set = _tenant_holder()
    db_default = f"d_{uuid4().hex[:10]}"
    db_alt = f"alt_{uuid4().hex[:10]}"

    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        database_name_for_tenant=lambda _tid: db_default,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        db = await routed.db()
        assert db.name == db_default
        assert (await routed.db(db_alt)).name == db_alt

        c = f"del_{uuid4().hex[:8]}"
        coll = await routed.collection(c)
        coll_alt = await routed.collection(c, db_name=db_alt)
        assert await routed.bulk_update(coll, [], batch_size=1) == 0

        await routed.insert_many(
            coll,
            [{"i": 1}, {"i": 2}, {"i": 3}],
            batch_size=2,
        )
        await routed.bulk_write(coll, [InsertOne({"i": 10})])

        rows = await routed.find_many(
            coll,
            {},
            projection={"i": 1, "_id": 1},
            sort=[("i", -1)],
            limit=2,
            skip=1,
        )
        assert len(rows) == 2
        assert rows[0]["i"] >= rows[1]["i"]

        agg = await routed.aggregate(
            coll,
            [{"$match": {"i": {"$gte": 2}}}, {"$sort": {"i": 1}}, {"$limit": 3}],
            limit=10,
        )
        assert len(agg) >= 2

        slim = await routed.find_one(coll, {"i": 1}, projection={"i": 1})
        assert slim is not None and "_id" in slim and slim.get("i") == 1

        await routed.bulk_update(
            coll,
            [({"i": 1}, {"$set": {"tag": "a"}}), ({"i": 2}, {"$set": {"tag": "b"}})],
            batch_size=1,
        )
        await routed.update_one(coll, {"i": 10}, {"$set": {"i": 11}})
        await routed.update_one_upsert(
            coll,
            {"i": 404},
            {"$set": {"from": "upsert"}},
        )
        assert await routed.find_one(coll, {"i": 404}) is not None

        await routed.update_many(coll, {"i": {"$lte": 3}}, {"$set": {"bulk_flag": True}})
        await routed.delete_one(coll, {"i": 11})
        removed = await routed.delete_many(coll, {"bulk_flag": True})
        assert removed >= 1
        assert await routed.count(coll, {}) >= 1

        await routed.insert_one(coll_alt, {"marker": True})
        alt_doc = await routed.find_one(coll_alt, {"marker": True})
        assert alt_doc is not None
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_mongo_transaction_guards_standalone(mongo_container: MongoDbContainer) -> None:
    """``is_in_transaction`` / ``require_transaction`` routing without a replica set."""

    uri = mongo_container.get_connection_url()
    t1 = uuid4()
    secrets = _MemSecrets({t1: uri})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        database_name_for_tenant=lambda _tid: f"g_{uuid4().hex[:10]}",
        max_cached_tenants=4,
    )
    await routed.startup()
    try:
        tenant_set(None)
        assert routed.is_in_transaction() is False
        with pytest.raises(InfrastructureError, match="Transactional context"):
            routed.require_transaction()

        tenant_set(t1)
        assert routed.is_in_transaction() is False

        await routed.health()
        assert routed.is_in_transaction() is False

        with pytest.raises(InfrastructureError, match="Transactional context"):
            routed.require_transaction()

        await routed.evict_tenant(t1)
        tenant_set(t1)
        with pytest.raises(InfrastructureError, match="Transactional context"):
            routed.require_transaction()

        tenant_set(None)
        assert routed.is_in_transaction() is False
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_mongo_lru_mru_refresh_evicts_correct_tenant(
    mongo_container: MongoDbContainer,
) -> None:
    """After MRU refresh, adding a third tenant closes the LRU pool (not the refreshed one)."""

    uri = mongo_container.get_connection_url()
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    secrets = _MemSecrets({t1: uri, t2: uri, t3: uri})
    tenant_get, tenant_set = _tenant_holder()

    db_names = {t1: f"a_{uuid4().hex[:8]}", t2: f"b_{uuid4().hex[:8]}", t3: f"c_{uuid4().hex[:8]}"}

    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        database_name_for_tenant=lambda tid: db_names[tid],
        max_cached_tenants=2,
    )
    await routed.startup()
    closes: list[int] = []
    real_close = MongoClient.close

    async def counting_close(self: MongoClient) -> None:
        closes.append(1)
        await real_close(self)

    try:
        with patch.object(MongoClient, "close", counting_close):
            tenant_set(t1)
            c1 = await routed.collection("lruc")
            await routed.insert_one(c1, {"t": "t1"})
            tenant_set(t2)
            c2 = await routed.collection("lruc")
            await routed.insert_one(c2, {"t": "t2"})
            tenant_set(t1)
            await routed.health()
            tenant_set(t3)
            c3 = await routed.collection("lruc")
            await routed.insert_one(c3, {"t": "t3"})
            assert sum(closes) == 1

        tenant_set(t1)
        assert await routed.count(await routed.collection("lruc"), {"t": "t1"}) == 1
        tenant_set(t3)
        assert await routed.count(c3, {"t": "t3"}) == 1
        tenant_set(t2)
        c2b = await routed.collection("lruc")
        assert await routed.count(c2b, {"t": "t2"}) == 1
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_mongo_evict_tenant_and_unknown_noop(
    mongo_container: MongoDbContainer,
) -> None:
    uri = mongo_container.get_connection_url()
    t1 = uuid4()
    secrets = _MemSecrets({t1: uri})
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        database_name_for_tenant=lambda _tid: f"ev_{uuid4().hex[:8]}",
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        await routed.health()
        await routed.evict_tenant(t1)
        await routed.evict_tenant(uuid4())
        status, ok = await routed.health()
        assert ok is True
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_mongo_database_isolation_between_tenants(
    mongo_container: MongoDbContainer,
) -> None:
    uri = mongo_container.get_connection_url()
    ta, tb = uuid4(), uuid4()
    secrets = _MemSecrets({ta: uri, tb: uri})
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        database_name_for_tenant=lambda tid: f"iso_{tid.hex[:8]}",
        max_cached_tenants=4,
    )
    await routed.startup()
    try:
        coll_name = f"iso_{uuid4().hex[:8]}"
        tenant_set(ta)
        ca = await routed.collection(coll_name)
        await routed.insert_one(ca, {"secret": "a"})
        tenant_set(tb)
        cb = await routed.collection(coll_name)
        assert await routed.find_one(cb, {"secret": "a"}) is None
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_mongo_secret_not_found_propagates(
    mongo_container: MongoDbContainer,
) -> None:
    uri = mongo_container.get_connection_url()
    t_ok, t_bad = uuid4(), uuid4()
    secrets = _MemSecrets({t_ok: uri}, missing_tenant=t_bad)
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        database_name_for_tenant=lambda _tid: f"s_{uuid4().hex[:8]}",
        max_cached_tenants=4,
    )
    await routed.startup()
    try:
        tenant_set(t_bad)
        with pytest.raises(SecretNotFoundError):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_mongo_secret_resolve_failure_wrapped(
    mongo_container: MongoDbContainer,
) -> None:
    uri = mongo_container.get_connection_url()
    t_ok, t_bad = uuid4(), uuid4()
    secrets = _MemSecrets({t_ok: uri}, broken_tenant=t_bad)
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        database_name_for_tenant=lambda _tid: f"w_{uuid4().hex[:8]}",
        max_cached_tenants=4,
    )
    await routed.startup()
    try:
        tenant_set(t_bad)
        with pytest.raises(InfrastructureError, match="Failed to resolve Mongo secret"):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_mongo_transaction_require_transaction_replica(
    mongo_replica_container,
) -> None:
    pytest.importorskip("docker")
    _ = mongo_replica_container
    uri = "mongodb://localhost:27017/?replicaSet=rs0"
    t1 = uuid4()
    secrets = _MemSecrets({t1: uri})
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        database_name_for_tenant=lambda _tid: f"tx_{uuid4().hex[:8]}",
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        await routed.health()
        tenant_set(t1)
        with pytest.raises(InfrastructureError, match="Transactional context"):
            routed.require_transaction()

        coll_name = f"tx_{uuid4().hex[:8]}"
        async with routed.transaction():
            assert routed.is_in_transaction() is True
            routed.require_transaction()
            coll = await routed.collection(coll_name)
            await routed.insert_one(coll, {"in_tx": True})

        assert routed.is_in_transaction() is False

        coll2 = await routed.collection(coll_name)
        assert await routed.find_one(coll2, {"in_tx": True}) is not None
    finally:
        await routed.close()
