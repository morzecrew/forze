"""Unit tests for ``forze_mongo.adapters.document``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.coordinators import DocumentCacheCoordinator
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.adapters.document import MongoDocumentAdapter
from forze_mongo.kernel.gateways import MongoReadGateway, MongoWriteGateway


class MyDoc(Document):
    name: str
    is_deleted: bool = False


class MyCreateDoc(CreateDocumentCmd):
    name: str


class MyUpdateDoc(BaseDTO):
    name: str | None = None


class MyReadDoc(ReadDocument):
    name: str
    is_deleted: bool = False


def _read_doc(pk: UUID, *, rev: int = 1, name: str = "item") -> MyReadDoc:
    now = datetime.now(tz=UTC)
    return MyReadDoc(id=pk, rev=rev, created_at=now, last_update_at=now, name=name)


def _domain_doc(pk: UUID, *, rev: int = 1, name: str = "item") -> MyDoc:
    now = datetime.now(tz=UTC)
    return MyDoc(id=pk, rev=rev, created_at=now, last_update_at=now, name=name)


def _build_read_gateway() -> MagicMock:
    gateway = MagicMock(spec=MongoReadGateway)
    gateway.model_type = MyReadDoc
    gateway.client = object()
    gateway.tenant_aware = False
    gateway.get = AsyncMock()
    gateway.get_many = AsyncMock()
    gateway.find = AsyncMock()
    gateway.count = AsyncMock(return_value=0)
    gateway.find_many = AsyncMock(return_value=[])
    return gateway


def _mongo_cc(read_gw, spec: DocumentSpec, *, cache=None, after_commit=None):
    return DocumentCacheCoordinator(
        read_model_type=read_gw.model_type,
        document_name=spec.name,
        cache=cache,
        after_commit=after_commit,
    )


def _doc_spec() -> DocumentSpec[MyReadDoc, MyDoc, MyCreateDoc, MyUpdateDoc]:
    return DocumentSpec(
        name="mongo-adapter-test",
        read=MyReadDoc,
        write={
            "domain": MyDoc,
            "create_cmd": MyCreateDoc,
            "update_cmd": MyUpdateDoc,
        },
    )


def _build_write_gateway(client: object) -> MagicMock:
    gateway = MagicMock(spec=MongoWriteGateway)
    gateway.client = client
    gateway.tenant_aware = False
    gateway.create = AsyncMock()
    gateway.create_many = AsyncMock(return_value=[])
    gateway.ensure = AsyncMock()
    gateway.ensure_many = AsyncMock(return_value=[])
    gateway.upsert = AsyncMock()
    gateway.upsert_many = AsyncMock(return_value=[])
    gateway.update = AsyncMock(return_value=(None, {}))
    gateway.update_many = AsyncMock(return_value=([], []))
    gateway.touch = AsyncMock()
    gateway.touch_many = AsyncMock()
    gateway.kill = AsyncMock()
    gateway.kill_many = AsyncMock()
    gateway.delete = AsyncMock()
    gateway.delete_many = AsyncMock()
    gateway.restore = AsyncMock()
    gateway.restore_many = AsyncMock()
    return gateway


class TestMongoDocumentAdapter:
    def test_post_init_rejects_mismatched_gateway_clients(self) -> None:
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(object())
        with pytest.raises(CoreError, match="same client"):
            MongoDocumentAdapter(
                spec=(ms := _doc_spec()),
                read_gw=read_gw,
                write_gw=write_gw,
                cache_coord=_mongo_cc(read_gw, ms),
            )

    def test_post_init_rejects_tenant_aware_mismatch(self) -> None:
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.tenant_aware = True
        read_gw.tenant_aware = False
        with pytest.raises(CoreError, match="tenant"):
            MongoDocumentAdapter(
                spec=(ms := _doc_spec()),
                read_gw=read_gw,
                write_gw=write_gw,
                cache_coord=_mongo_cc(read_gw, ms),
            )

    def test_eff_batch_size_clamps_extremes(self) -> None:
        rg = _build_read_gateway()
        ms = _doc_spec()
        small = MongoDocumentAdapter(
            spec=ms, read_gw=rg, cache_coord=_mongo_cc(rg, ms), batch_size=5
        )
        assert small.eff_batch_size == 200
        rg = _build_read_gateway()
        ms = _doc_spec()
        large = MongoDocumentAdapter(
            spec=ms, read_gw=rg, cache_coord=_mongo_cc(rg, ms), batch_size=200000
        )
        assert large.eff_batch_size == 200

    @pytest.mark.asyncio
    async def test_get_continues_when_cache_set_fails(self) -> None:
        pk = uuid4()
        expected = _read_doc(pk)
        read_gw = _build_read_gateway()
        read_gw.get.return_value = expected

        cache = MagicMock()
        cache.get = AsyncMock(return_value=None)
        cache.set_versioned = AsyncMock(side_effect=RuntimeError("set failed"))

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        result = await adapter.get(pk)

        assert result == expected
        cache.set_versioned.assert_awaited()

    @pytest.mark.asyncio
    async def test_find_many_short_circuits_when_count_zero(self) -> None:
        read_gw = _build_read_gateway()
        read_gw.count = AsyncMock(return_value=0)
        read_gw.find_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        page = await adapter.find_many(
            None, pagination=None, sorts=None, return_count=True
        )

        assert page.hits == []
        assert page.count == 0
        read_gw.find_many.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_falls_back_to_read_gateway_when_cache_get_fails(self) -> None:
        pk = uuid4()
        expected = _read_doc(pk)
        read_gw = _build_read_gateway()
        read_gw.get.return_value = expected

        cache = MagicMock()
        cache.get = AsyncMock(side_effect=RuntimeError("cache unavailable"))
        cache.set_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        result = await adapter.get(pk, for_update=True)

        assert result == expected
        read_gw.get.assert_awaited_once_with(pk, for_update=True, return_fields=None)
        cache.set_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_many_falls_back_to_read_gateway_when_cache_get_many_fails(
        self,
    ) -> None:
        pks = [uuid4(), uuid4()]
        expected = [_read_doc(pk, rev=i + 1) for i, pk in enumerate(pks)]
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = expected

        cache = MagicMock()
        cache.get_many = AsyncMock(side_effect=RuntimeError("cache unavailable"))
        cache.set_many_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        result = await adapter.get_many(pks)

        assert result == expected
        read_gw.get_many.assert_awaited_once_with(pks, return_fields=None)
        cache.set_many_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_skip_cache_bypasses_cache(self) -> None:
        pk = uuid4()
        expected = _read_doc(pk)
        read_gw = _build_read_gateway()
        read_gw.get.return_value = expected

        cache = MagicMock()
        cache.get = AsyncMock(return_value={"id": str(pk), "bogus": True})
        cache.set_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        result = await adapter.get(pk, skip_cache=True)

        assert result == expected
        read_gw.get.assert_awaited_once_with(pk, for_update=False, return_fields=None)
        cache.get.assert_not_awaited()
        cache.set_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_many_skip_cache_bypasses_cache(self) -> None:
        pks = [uuid4(), uuid4()]
        expected = [_read_doc(pk, rev=i + 1) for i, pk in enumerate(pks)]
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = expected

        cache = MagicMock()
        cache.get_many = AsyncMock(return_value=({}, []))
        cache.set_many_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        result = await adapter.get_many(pks, skip_cache=True)

        assert result == expected
        read_gw.get_many.assert_awaited_once_with(pks, return_fields=None)
        cache.get_many.assert_not_awaited()
        cache.set_many_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_update_delegates_to_write_gateway_and_maps_domain_to_read(
        self,
    ) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        expected_read = _read_doc(pk, rev=2, name="after")
        read_gw.get.return_value = expected_read

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        updated = await adapter.update(pk, 1, MyUpdateDoc(name="after"))

        assert isinstance(updated, MyReadDoc)
        assert updated.id == pk
        assert updated.rev == 2
        assert updated.name == "after"
        write_gw.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_requires_write_gateway(self) -> None:
        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=(rg := _build_read_gateway()),
            cache_coord=_mongo_cc(rg, ms),
        )

        with pytest.raises(CoreError, match="Write gateway is not configured"):
            await adapter.update(uuid4(), 1, MyUpdateDoc(name="x"))


class TestMongoDocumentAdapterReturnNew:
    """``return_new=False`` mutates via write gateway but skips read-back and cache sets."""

    @pytest.mark.asyncio
    async def test_create_skips_read_and_cache_when_return_new_false(self) -> None:
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        dom = _domain_doc(uuid4())
        write_gw.create = AsyncMock(return_value=dom)
        cache = MagicMock()
        cache.set_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )

        assert await adapter.create(MyCreateDoc(name="n"), return_new=False) is None
        write_gw.create.assert_awaited_once()
        read_gw.get.assert_not_awaited()
        cache.set_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_many_skips_read_when_return_new_false(self) -> None:
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        d1, d2 = _domain_doc(uuid4()), _domain_doc(uuid4())
        write_gw.create_many = AsyncMock(return_value=[d1, d2])
        cache = MagicMock()
        cache.set_many_versioned = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
            batch_size=50,
        )

        assert (
            await adapter.create_many(
                [MyCreateDoc(name="a"), MyCreateDoc(name="b")],
                return_new=False,
            )
            is None
        )
        write_gw.create_many.assert_awaited_once()
        read_gw.get_many.assert_not_awaited()
        cache.set_many_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_update_skips_read_when_return_new_false(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.update = AsyncMock(return_value=(None, {}))
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )

        assert (
            await adapter.update(pk, 1, MyUpdateDoc(name="z"), return_new=False) is None
        )
        write_gw.update.assert_awaited_once()
        read_gw.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_touch_skips_read_when_return_new_false(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.touch = AsyncMock()
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )

        assert await adapter.touch(pk, return_new=False) is None
        write_gw.touch.assert_awaited_once_with(pk)
        read_gw.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_touch_many_skips_read_when_return_new_false(self) -> None:
        pks = [uuid4(), uuid4()]
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.touch_many = AsyncMock()
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
            batch_size=40,
        )

        assert await adapter.touch_many(pks, return_new=False) is None
        write_gw.touch_many.assert_awaited_once_with(pks, batch_size=40)
        read_gw.get_many.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_restore_skips_read_when_return_new_false(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.delete = AsyncMock()
        write_gw.restore = AsyncMock()
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )

        assert await adapter.delete(pk, 1, return_new=False) is None
        write_gw.delete.assert_awaited_once_with(pk, rev=1)
        read_gw.get.assert_not_awaited()

        assert await adapter.restore(pk, 2, return_new=False) is None
        write_gw.restore.assert_awaited_once_with(pk, rev=2)
        read_gw.get.assert_not_awaited()


class TestMongoDocumentAdapterCacheHits:
    """Cache read-hit paths and batch cache helpers."""

    @pytest.mark.asyncio
    async def test_get_uses_cache_when_hit(self) -> None:
        pk = uuid4()
        doc = _read_doc(pk)
        payload = doc.model_dump(mode="json")
        read_gw = _build_read_gateway()
        cache = MagicMock()
        cache.get = AsyncMock(return_value=payload)

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        out = await adapter.get(pk)

        assert out.id == pk
        assert out.name == doc.name
        read_gw.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_many_all_cached_no_db_roundtrip(self) -> None:
        pks = [uuid4(), uuid4()]
        docs = [_read_doc(pks[0], rev=1), _read_doc(pks[1], rev=1)]
        hits = {str(pks[i]): docs[i].model_dump(mode="json") for i in range(2)}
        read_gw = _build_read_gateway()
        cache = MagicMock()
        cache.get_many = AsyncMock(return_value=(hits, []))

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        out = await adapter.get_many(pks)

        assert [x.id for x in out] == pks
        read_gw.get_many.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_set_cache_many_swallows_set_many_failure(self) -> None:
        pks = [uuid4()]
        miss_doc = _read_doc(pks[0])
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = [miss_doc]
        cache = MagicMock()
        cache.get_many = AsyncMock(return_value=({}, [str(pks[0])]))
        cache.set_many_versioned = AsyncMock(side_effect=RuntimeError("down"))

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        out = await adapter.get_many(pks)

        assert len(out) == 1
        cache.set_many_versioned.assert_awaited()

    @pytest.mark.asyncio
    async def test_clear_cache_swallows_delete_failure(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.update = AsyncMock(return_value=(None, {"name": "z"}))
        cache = MagicMock()
        cache.delete_many = AsyncMock(side_effect=RuntimeError("down"))

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        assert (
            await adapter.update(pk, 1, MyUpdateDoc(name="z"), return_new=False) is None
        )
        cache.delete_many.assert_awaited()


class TestMongoDocumentAdapterFindManyWithCursor:
    @pytest.mark.asyncio
    async def test_wraps_gateway_rows_into_cursor_page(self) -> None:
        read_gw = _build_read_gateway()
        p1, p2, p3 = uuid4(), uuid4(), uuid4()
        rows = [_read_doc(p1), _read_doc(p2), _read_doc(p3)]
        read_gw.find_many_with_cursor = AsyncMock(return_value=rows)

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        page = await adapter.find_many_with_cursor(
            None,
            cursor={"limit": 2},
            sorts=None,
        )

        assert len(page.hits) == 2
        assert page.has_more is True
        assert page.next_cursor is not None


class TestMongoDocumentAdapterFindAndPage:
    @pytest.mark.asyncio
    async def test_find_passes_return_fields_to_gateway(self) -> None:
        read_gw = _build_read_gateway()
        read_gw.find.return_value = {"name": "x"}
        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        filt = {"$fields": {"name": "x"}}
        row = await adapter.find(filt, return_fields=["name"])
        assert row == {"name": "x"}
        read_gw.find.assert_awaited_once_with(
            filt, for_update=False, return_fields=["name"]
        )

    @pytest.mark.asyncio
    async def test_find_many_with_count_loads_hits_when_nonzero(self) -> None:
        read_gw = _build_read_gateway()
        read_gw.count = AsyncMock(return_value=3)
        read_gw.find_many.return_value = [_read_doc(uuid4())]
        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        page = await adapter.find_many(
            None,
            pagination={"limit": 10, "offset": 0},
            return_count=True,
        )
        assert page.count == 3
        assert len(page.hits) == 1


class TestMongoDocumentAdapterMutationsWithCache:
    """Happy paths that read back and refresh cache (``return_new=True``)."""

    @pytest.mark.asyncio
    async def test_create_many_empty_short_circuits(self) -> None:
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        assert await adapter.create_many([]) == []
        write_gw.create_many.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_many_refreshes_cache(self) -> None:
        p1, p2 = uuid4(), uuid4()
        d1, d2 = _domain_doc(p1, name="a"), _domain_doc(p2, name="b")
        r1, r2 = _read_doc(p1, name="a"), _read_doc(p2, name="b")
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = [r1, r2]
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.create_many = AsyncMock(return_value=[d1, d2])
        cache = MagicMock()
        cache.set_many_versioned = AsyncMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        out = await adapter.create_many([MyCreateDoc(name="a"), MyCreateDoc(name="b")])
        assert len(out) == 2
        cache.set_many_versioned.assert_awaited()

    @pytest.mark.asyncio
    async def test_create_refreshes_cache(self) -> None:
        pk = uuid4()
        dom = _domain_doc(pk, name="n")
        read = _read_doc(pk, name="n")
        read_gw = _build_read_gateway()
        read_gw.get.return_value = read
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.create = AsyncMock(return_value=dom)
        cache = MagicMock()
        cache.set_versioned = AsyncMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        out = await adapter.create(MyCreateDoc(name="n"))
        assert out == read
        cache.set_versioned.assert_awaited()
        cache.delete_many.assert_awaited()

    @pytest.mark.asyncio
    async def test_ensure_upsert_roundtrip_with_id(self) -> None:
        pk = uuid4()
        dom = _domain_doc(pk)
        read = _read_doc(pk)
        read_gw = _build_read_gateway()
        read_gw.get.return_value = read
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.ensure = AsyncMock(return_value=dom)
        write_gw.upsert = AsyncMock(return_value=dom)
        cache = MagicMock()
        cache.set_versioned = AsyncMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        e = await adapter.ensure(MyCreateDoc(id=pk, name="a"))
        assert e == read
        u = await adapter.upsert(
            MyCreateDoc(id=pk, name="b"),
            MyUpdateDoc(name="c"),
        )
        assert u == read

    @pytest.mark.asyncio
    async def test_ensure_many_upsert_many_roundtrip(self) -> None:
        p1, p2 = uuid4(), uuid4()
        d1, d2 = _domain_doc(p1), _domain_doc(p2)
        r1, r2 = _read_doc(p1), _read_doc(p2)
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = [r1, r2]
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.ensure_many = AsyncMock(return_value=[d1, d2])
        write_gw.upsert_many = AsyncMock(return_value=[d1, d2])
        cache = MagicMock()
        cache.set_many_versioned = AsyncMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        em = await adapter.ensure_many(
            [MyCreateDoc(id=p1, name="a"), MyCreateDoc(id=p2, name="b")]
        )
        assert len(em) == 2
        um = await adapter.upsert_many(
            [
                (MyCreateDoc(id=p1, name="x"), MyUpdateDoc()),
                (MyCreateDoc(id=p2, name="y"), MyUpdateDoc()),
            ]
        )
        assert len(um) == 2

    @pytest.mark.asyncio
    async def test_ensure_many_empty_and_upsert_many_empty(self) -> None:
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        assert await adapter.ensure_many([], return_new=True) == []
        assert await adapter.ensure_many([], return_new=False) is None
        assert await adapter.upsert_many([], return_new=True) == []
        assert await adapter.upsert_many([], return_new=False) is None
        write_gw.ensure_many.assert_not_awaited()
        write_gw.upsert_many.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ensure_many_skips_readback_when_return_new_false(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.ensure_many = AsyncMock(return_value=[_domain_doc(pk)])
        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        assert (
            await adapter.ensure_many(
                [MyCreateDoc(id=pk, name="a")],
                return_new=False,
            )
            is None
        )
        read_gw.get_many.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_upsert_many_skips_readback_when_return_new_false(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.upsert_many = AsyncMock(return_value=[_domain_doc(pk)])
        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        assert (
            await adapter.upsert_many(
                [(MyCreateDoc(id=pk, name="a"), MyUpdateDoc())],
                return_new=False,
            )
            is None
        )
        read_gw.get_many.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_update_return_diff_true(self) -> None:
        pk = uuid4()
        diff = {"name": "z"}
        read = _read_doc(pk, rev=2, name="z")
        read_gw = _build_read_gateway()
        read_gw.get.return_value = read
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.update = AsyncMock(return_value=(None, diff))

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        pair = await adapter.update(pk, 1, MyUpdateDoc(name="z"), return_diff=True)
        assert pair == (read, diff)

    @pytest.mark.asyncio
    async def test_update_return_new_false_return_diff(self) -> None:
        pk = uuid4()
        diff = {"name": "z"}
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.update = AsyncMock(return_value=(None, diff))

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        out = await adapter.update(
            pk, 1, MyUpdateDoc(name="z"), return_new=False, return_diff=True
        )
        assert out == diff

    @pytest.mark.asyncio
    async def test_update_many_empty_and_with_diffs(self) -> None:
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
            batch_size=100,
        )
        assert await adapter.update_many([], return_new=True) == []
        assert await adapter.update_many([], return_new=False) is None

        p1, p2 = uuid4(), uuid4()
        r1, r2 = _read_doc(p1, rev=2), _read_doc(p2, rev=2)
        diffs = [{"n": 1}, {"n": 2}]
        read_gw.get_many.return_value = [r1, r2]
        write_gw.update_many = AsyncMock(return_value=([None, None], diffs))

        zipped = await adapter.update_many(
            [
                (p1, 1, MyUpdateDoc(name="a")),
                (p2, 1, MyUpdateDoc(name="b")),
            ],
            return_diff=True,
        )
        assert len(zipped) == 2
        assert zipped[0] == (r1, diffs[0])
        write_gw.update_many.assert_awaited_with(
            [p1, p2],
            [MyUpdateDoc(name="a"), MyUpdateDoc(name="b")],
            revs=[1, 1],
            batch_size=100,
        )

    @pytest.mark.asyncio
    async def test_update_many_return_new_false_return_diff(self) -> None:
        p1 = uuid4()
        diffs = [{"x": 1}]
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.update_many = AsyncMock(return_value=([None], diffs))

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms),
        )
        out = await adapter.update_many(
            [(p1, 1, MyUpdateDoc(name="a"))],
            return_new=False,
            return_diff=True,
        )
        assert out == diffs

    @pytest.mark.asyncio
    async def test_touch_and_touch_many_refresh_cache(self) -> None:
        pk = uuid4()
        read = _read_doc(pk, rev=2)
        read_gw = _build_read_gateway()
        read_gw.get.return_value = read
        read_gw.get_many.return_value = [read]
        write_gw = _build_write_gateway(read_gw.client)
        write_gw.touch = AsyncMock()
        write_gw.touch_many = AsyncMock()
        cache = MagicMock()
        cache.set_versioned = AsyncMock()
        cache.set_many_versioned = AsyncMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        assert await adapter.touch(pk) == read
        assert await adapter.touch_many([pk]) == [read]

    @pytest.mark.asyncio
    async def test_kill_and_kill_many_clear_cache(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        await adapter.kill(pk)
        await adapter.kill_many([pk, uuid4()])
        assert cache.delete_many.await_count == 2

    @pytest.mark.asyncio
    async def test_delete_restore_delete_many_restore_many_with_readback(self) -> None:
        pk = uuid4()
        read = _read_doc(pk, rev=2)
        read_gw = _build_read_gateway()
        read_gw.get.return_value = read
        read_gw.get_many.return_value = [read]
        write_gw = _build_write_gateway(read_gw.client)
        cache = MagicMock()
        cache.set_versioned = AsyncMock()
        cache.set_many_versioned = AsyncMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        assert await adapter.delete(pk, 1) == read
        assert await adapter.restore(pk, 2) == read

        p2 = uuid4()
        r2 = _read_doc(p2)
        read_gw.get_many.return_value = [read, r2]
        assert await adapter.delete_many([(pk, 1), (p2, 1)]) == [read, r2]
        assert await adapter.restore_many([(pk, 2), (p2, 2)]) == [read, r2]

    @pytest.mark.asyncio
    async def test_delete_many_restore_many_return_new_false(self) -> None:
        pk, pk2 = uuid4(), uuid4()
        read_gw = _build_read_gateway()
        write_gw = _build_write_gateway(read_gw.client)
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = MongoDocumentAdapter(
            spec=(ms := _doc_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_mongo_cc(read_gw, ms, cache=cache),
        )
        assert await adapter.delete_many([(pk, 1)], return_new=False) is None
        assert await adapter.restore_many([(pk2, 1)], return_new=False) is None
        read_gw.get_many.assert_not_awaited()
