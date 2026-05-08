"""Unit tests for ``forze_postgres.adapters.document``."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.coordinators import DocumentCacheCoordinator
from forze.base.errors import CoreError, ValidationError
from forze.base.serialization import pydantic_cache_dump
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.adapters.document import PostgresDocumentAdapter
from forze_postgres.kernel.gateways import PostgresReadGateway, PostgresWriteGateway


def _build_read_doc(pk: UUID, *, rev: int = 1) -> ReadDocument:
    now = datetime.now(tz=UTC)
    return ReadDocument(id=pk, rev=rev, created_at=now, last_update_at=now)


def _adapter_spec() -> DocumentSpec:
    return DocumentSpec(name="pytest_doc", read=ReadDocument)


def _build_read_gateway() -> MagicMock:
    gateway = MagicMock(spec=PostgresReadGateway)
    gateway.model_type = ReadDocument
    gateway.client = object()
    gateway.get = AsyncMock()
    gateway.get_many = AsyncMock()
    return gateway


def _pg_cc(
    read_gw,
    spec: DocumentSpec,
    *,
    cache=None,
    after_commit=None,
):
    return DocumentCacheCoordinator(
        read_model_type=read_gw.model_type,
        document_name=spec.name,
        cache=cache,
        after_commit=after_commit,
    )


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

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

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

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

        result = await adapter.get(pk)

        assert result == expected
        read_gw.get.assert_awaited_once_with(pk)
        cache.set_versioned.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_skip_cache_bypasses_cache(self) -> None:
        pk = uuid4()
        expected = _build_read_doc(pk)
        read_gw = _build_read_gateway()
        read_gw.get.return_value = expected

        cache = MagicMock()
        cache.get = AsyncMock(return_value=pydantic_cache_dump(expected))
        cache.set_versioned = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

        result = await adapter.get(pk, skip_cache=True)

        assert result == expected
        read_gw.get.assert_awaited_once_with(pk, for_update=False, return_fields=None)
        cache.get.assert_not_awaited()
        cache.set_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_many_skip_cache_bypasses_cache(self) -> None:
        pks = [uuid4(), uuid4()]
        expected = [_build_read_doc(pk, rev=i + 1) for i, pk in enumerate(pks)]
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = expected

        cache = MagicMock()
        cache.get_many = AsyncMock(
            return_value=(
                {str(pks[0]): pydantic_cache_dump(expected[0])},
                [str(pks[1])],
            )
        )
        cache.set_many_versioned = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

        result = await adapter.get_many(pks, skip_cache=True)

        assert result == expected
        read_gw.get_many.assert_awaited_once_with(pks, return_fields=None)
        cache.get_many.assert_not_awaited()
        cache.set_many_versioned.assert_not_awaited()

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

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

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

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

        result = await adapter.get_many(pks)

        assert result == expected
        read_gw.get_many.assert_awaited_once_with([pks[0], pks[1]])
        cache.set_many_versioned.assert_awaited_once()


# -- Write models for command-path tests -------------------------------------


class TRead(ReadDocument):
    title: str


class TDoc(Document):
    title: str


class TCreate(CreateDocumentCmd):
    title: str


class TUpdate(BaseDTO):
    title: str | None = None


def _full_spec() -> DocumentSpec:
    return DocumentSpec(
        name="full_doc",
        read=TRead,
        write={"domain": TDoc, "create_cmd": TCreate, "update_cmd": TUpdate},
    )


def _tread(pk: UUID | None = None, *, rev: int = 1) -> TRead:
    pk = pk or uuid4()
    now = datetime.now(tz=UTC)
    return TRead(
        id=pk,
        rev=rev,
        created_at=now,
        last_update_at=now,
        title="t",
    )


def _tdoc(pk: UUID | None = None, *, rev: int = 1) -> TDoc:
    pk = pk or uuid4()
    now = datetime.now(tz=UTC)
    return TDoc(
        id=pk,
        rev=rev,
        created_at=now,
        last_update_at=now,
        title="t",
    )


def _read_gw_full() -> MagicMock:
    gw = MagicMock(spec=PostgresReadGateway)
    gw.model_type = TRead
    gw.client = object()
    gw.tenant_aware = False
    gw.get = AsyncMock()
    gw.get_many = AsyncMock()
    gw.find = AsyncMock()
    gw.find_many = AsyncMock()
    gw.count = AsyncMock()
    return gw


def _write_gw() -> MagicMock:
    gw = MagicMock(spec=PostgresWriteGateway)
    gw.tenant_aware = False
    gw.update = AsyncMock(return_value=(None, {}))
    gw.update_many = AsyncMock(return_value=([], []))
    gw.ensure = AsyncMock()
    gw.ensure_many = AsyncMock()
    return gw


class TestPostgresDocumentAdapterInit:
    def test_mismatched_write_client_raises(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = object()

        with pytest.raises(CoreError, match="same client"):
            PostgresDocumentAdapter(
                spec=(ds := _full_spec()),
                read_gw=read_gw,
                write_gw=write_gw,
                cache_coord=_pg_cc(read_gw, ds),
            )

    def test_mismatched_tenant_awareness_raises(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        read_gw.tenant_aware = False
        write_gw.tenant_aware = True

        with pytest.raises(CoreError, match="tenant"):
            PostgresDocumentAdapter(
                spec=(ds := _full_spec()),
                read_gw=read_gw,
                write_gw=write_gw,
                cache_coord=_pg_cc(read_gw, ds),
            )


class TestPostgresDocumentAdapterEffBatchSize:
    def test_clamps_too_small(self) -> None:
        rg = _read_gw_full()
        ds = _full_spec()
        a = PostgresDocumentAdapter(
            spec=ds,
            read_gw=rg,
            cache_coord=_pg_cc(rg, ds),
            batch_size=5,
        )
        assert a.eff_batch_size == 200

    def test_clamps_too_large(self) -> None:
        rg = _read_gw_full()
        ds = _full_spec()
        a = PostgresDocumentAdapter(
            spec=ds,
            read_gw=rg,
            cache_coord=_pg_cc(rg, ds),
            batch_size=500000,
        )
        assert a.eff_batch_size == 200

    def test_uses_in_range_value(self) -> None:
        rg = _read_gw_full()
        ds = _full_spec()
        a = PostgresDocumentAdapter(
            spec=ds,
            read_gw=rg,
            cache_coord=_pg_cc(rg, ds),
            batch_size=150,
        )
        assert a.eff_batch_size == 150


class TestPostgresDocumentAdapterGetPaths:
    @pytest.mark.asyncio
    async def test_return_fields_bypasses_cache(self) -> None:
        pk = uuid4()
        read_gw = _build_read_gateway()
        read_gw.get.return_value = {"id": str(pk)}

        cache = MagicMock()
        cache.get = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

        out = await adapter.get(pk, return_fields=["id"])

        assert out == {"id": str(pk)}
        read_gw.get.assert_awaited_once_with(pk, for_update=False, return_fields=["id"])
        cache.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cache_hit_skips_database(self) -> None:
        pk = uuid4()
        doc = _build_read_doc(pk)
        read_gw = _build_read_gateway()
        read_gw.get = AsyncMock()

        cache = MagicMock()
        cache.get = AsyncMock(return_value=pydantic_cache_dump(doc))

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

        result = await adapter.get(pk)

        assert result.id == doc.id
        read_gw.get.assert_not_awaited()


class TestPostgresDocumentAdapterGetManyOrdering:
    @pytest.mark.asyncio
    async def test_merges_hits_and_misses_in_request_order(self) -> None:
        pks = [uuid4(), uuid4(), uuid4()]
        d0 = _build_read_doc(pks[0], rev=1)
        d1 = _build_read_doc(pks[1], rev=2)
        d2 = _build_read_doc(pks[2], rev=3)

        read_gw = _build_read_gateway()
        read_gw.get_many = AsyncMock(return_value=[d1])

        cache = MagicMock()
        cache.get_many = AsyncMock(
            return_value=(
                {
                    str(pks[0]): pydantic_cache_dump(d0),
                    str(pks[2]): pydantic_cache_dump(d2),
                },
                [str(pks[1])],
            )
        )
        cache.set_many_versioned = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

        result = await adapter.get_many(pks)

        assert [x.id for x in result] == pks
        read_gw.get_many.assert_awaited_once_with([pks[1]])


class TestPostgresDocumentAdapterRequireWrite:
    @pytest.mark.asyncio
    async def test_create_without_write_gateway_raises(self) -> None:
        rg = _read_gw_full()
        ds = _full_spec()
        adapter = PostgresDocumentAdapter(
            spec=ds,
            read_gw=rg,
            cache_coord=_pg_cc(rg, ds),
        )

        with pytest.raises(CoreError, match="not configured"):
            await adapter.create(TCreate(title="x"))


class TestPostgresDocumentAdapterQueryDelegation:
    @pytest.mark.asyncio
    async def test_find_delegates(self) -> None:
        read_gw = _read_gw_full()
        doc = _tread()
        read_gw.find = AsyncMock(return_value=doc)
        filt: dict = {"title": "a"}

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )

        out = await adapter.find(filt, for_update=True)

        assert out == doc
        read_gw.find.assert_awaited_once_with(filt, for_update=True, return_fields=None)

    @pytest.mark.asyncio
    async def test_find_many_short_circuits_when_count_zero(self) -> None:
        read_gw = _read_gw_full()
        read_gw.count = AsyncMock(return_value=0)

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )

        page = await adapter.find_many(
            filters={"x": 1},
            return_count=True,
        )

        assert page.hits == [] and page.count == 0
        read_gw.find_many.assert_not_called()

    @pytest.mark.asyncio
    async def test_find_many_returns_rows_and_count(self) -> None:
        read_gw = _read_gw_full()
        docs = [_tread(), _tread()]
        read_gw.count = AsyncMock(return_value=2)
        read_gw.find_many = AsyncMock(return_value=docs)

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )

        page = await adapter.find_many(
            filters=None, pagination={"limit": 10}, return_count=True
        )

        assert page.hits == docs and page.count == 2

    @pytest.mark.asyncio
    async def test_count_delegates(self) -> None:
        read_gw = _read_gw_full()
        read_gw.count = AsyncMock(return_value=7)

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )

        assert await adapter.count({"a": 1}) == 7
        read_gw.count.assert_awaited_once_with({"a": 1})


class TestPostgresDocumentAdapterCommands:
    @pytest.mark.asyncio
    async def test_create_reads_back_and_sets_cache(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        dom = _tdoc()
        read_doc = _tread(dom.id, rev=dom.rev)
        write_gw.create = AsyncMock(return_value=dom)
        read_gw.get = AsyncMock(return_value=read_doc)

        cache = MagicMock()
        cache.set_versioned = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds, cache=cache),
        )

        out = await adapter.create(TCreate(title="n"))

        assert out == read_doc
        write_gw.create.assert_awaited_once()
        read_gw.get.assert_awaited_once_with(dom.id)
        cache.set_versioned.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_many_empty_short_circuit(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        write_gw.create_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
            batch_size=50,
        )

        assert await adapter.create_many([]) == []
        write_gw.create_many.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_delegates_and_validates_id(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk = uuid4()
        dom = _tdoc(pk)
        read_doc = _tread(pk, rev=dom.rev)
        write_gw.ensure = AsyncMock(return_value=dom)
        read_gw.get = AsyncMock(return_value=read_doc)

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )
        out = await adapter.ensure(TCreate(id=pk, title="n"))
        assert out == read_doc
        write_gw.ensure.assert_awaited_once()
        ca = write_gw.ensure.await_args
        assert ca[0][0].id == pk

    @pytest.mark.asyncio
    async def test_ensure_rejects_missing_id(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )
        with pytest.raises(ValidationError, match="id"):
            await adapter.ensure(TCreate(title="n"))  # type: ignore[call-arg]

    @pytest.mark.asyncio
    async def test_update_clears_cache_and_reloads(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk = uuid4()
        read_doc = _tread(pk, rev=2)
        write_gw.update = AsyncMock(return_value=(None, {}))
        read_gw.get = AsyncMock(return_value=read_doc)

        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds, cache=cache),
        )

        out = await adapter.update(pk, 1, TUpdate(title="z"))

        assert out == read_doc
        write_gw.update.assert_awaited_once_with(pk, TUpdate(title="z"), rev=1)
        cache.delete_many.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_clear_cache_failure_is_swallowed(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        write_gw.update = AsyncMock(return_value=(None, {}))
        read_gw.get = AsyncMock(return_value=_tread())

        cache = MagicMock()
        cache.delete_many = AsyncMock(side_effect=RuntimeError("cache down"))

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds, cache=cache),
        )

        await adapter.update(uuid4(), 1, TUpdate(title="z"))

    @pytest.mark.asyncio
    async def test_touch_and_touch_many(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk = uuid4()
        pks = [pk, uuid4()]
        write_gw.touch = AsyncMock()
        write_gw.touch_many = AsyncMock()
        read_gw.get = AsyncMock(return_value=_tread(pk))
        read_gw.get_many = AsyncMock(return_value=[_tread(pks[0]), _tread(pks[1])])

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
            batch_size=50,
        )

        await adapter.touch(pk)
        write_gw.touch.assert_awaited_once_with(pk)

        await adapter.touch_many(pks)
        write_gw.touch_many.assert_awaited_once_with(pks, batch_size=50)

    @pytest.mark.asyncio
    async def test_touch_many_empty(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        write_gw.touch_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )

        assert await adapter.touch_many([]) == []
        write_gw.touch_many.assert_not_called()

    @pytest.mark.asyncio
    async def test_kill_and_kill_many(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk = uuid4()
        pks = [pk, uuid4()]
        write_gw.kill = AsyncMock()
        write_gw.kill_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
            batch_size=40,
        )

        await adapter.kill(pk)
        write_gw.kill.assert_awaited_once_with(pk)

        await adapter.kill_many(pks)
        write_gw.kill_many.assert_awaited_once_with(pks, batch_size=40)

    @pytest.mark.asyncio
    async def test_kill_many_empty(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        write_gw.kill_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )

        await adapter.kill_many([])
        write_gw.kill_many.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_and_restore(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk = uuid4()
        rd = _tread(pk)
        write_gw.delete = AsyncMock()
        write_gw.restore = AsyncMock()
        read_gw.get = AsyncMock(return_value=rd)

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )

        assert await adapter.delete(pk, 1) == rd
        write_gw.delete.assert_awaited_once_with(pk, rev=1)

        assert await adapter.restore(pk, 2) == rd
        write_gw.restore.assert_awaited_once_with(pk, rev=2)

    @pytest.mark.asyncio
    async def test_update_many_delete_many_restore_many_empty(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        write_gw.update_many = AsyncMock(return_value=([], []))
        write_gw.delete_many = AsyncMock()
        write_gw.restore_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )

        assert await adapter.update_many([]) == []
        assert await adapter.delete_many([]) == []
        assert await adapter.restore_many([]) == []
        write_gw.update_many.assert_not_called()
        write_gw.delete_many.assert_not_called()
        write_gw.restore_many.assert_not_called()


class TestPostgresDocumentAdapterGetManyBranches:
    @pytest.mark.asyncio
    async def test_empty_pks_returns_without_calling_gateway(self) -> None:
        read_gw = _build_read_gateway()
        adapter = PostgresDocumentAdapter(
            spec=(ds := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, ds),
        )

        assert await adapter.get_many([]) == []
        read_gw.get_many.assert_not_called()

    @pytest.mark.asyncio
    async def test_return_fields_bypasses_cache(self) -> None:
        pks = [uuid4()]
        read_gw = _build_read_gateway()
        read_gw.get_many.return_value = [{"id": str(pks[0])}]
        cache = MagicMock()
        cache.get_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

        out = await adapter.get_many(pks, return_fields=["id"])

        assert out == [{"id": str(pks[0])}]
        read_gw.get_many.assert_awaited_once_with(pks, return_fields=["id"])
        cache.get_many.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_all_cache_hits_skips_database(self) -> None:
        pks = [uuid4(), uuid4()]
        d0 = _build_read_doc(pks[0])
        d1 = _build_read_doc(pks[1])
        read_gw = _build_read_gateway()
        read_gw.get_many = AsyncMock()

        cache = MagicMock()
        cache.get_many = AsyncMock(
            return_value=(
                {
                    str(pks[0]): pydantic_cache_dump(d0),
                    str(pks[1]): pydantic_cache_dump(d1),
                },
                [],
            )
        )

        adapter = PostgresDocumentAdapter(
            spec=(doc_spec := _adapter_spec()),
            read_gw=read_gw,
            cache_coord=_pg_cc(read_gw, doc_spec, cache=cache),
        )

        result = await adapter.get_many(pks)

        assert [x.id for x in result] == pks
        read_gw.get_many.assert_not_awaited()


class TestPostgresDocumentAdapterBatchMutations:
    @pytest.mark.asyncio
    async def test_create_many_batches_and_reloads(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        d1 = _tdoc()
        d2 = _tdoc()
        r1 = _tread(d1.id, rev=d1.rev)
        r2 = _tread(d2.id, rev=d2.rev)
        write_gw.create_many = AsyncMock(return_value=[d1, d2])
        read_gw.get_many = AsyncMock(return_value=[r1, r2])
        cache = MagicMock()
        cache.set_many_versioned = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
            batch_size=100,
        )

        out = await adapter.create_many([TCreate(title="a"), TCreate(title="b")])

        assert out == [r1, r2]
        write_gw.create_many.assert_awaited_once()
        ca = write_gw.create_many.await_args
        assert ca[0][0] == [TCreate(title="a"), TCreate(title="b")]
        assert ca[1]["batch_size"] == 100

    @pytest.mark.asyncio
    async def test_update_many_batches(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk1, pk2 = uuid4(), uuid4()
        r1, r2 = _tread(pk1), _tread(pk2)
        write_gw.update_many = AsyncMock(return_value=([], []))
        read_gw.get_many = AsyncMock(return_value=[r1, r2])
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
            batch_size=80,
        )

        updates = [
            (pk1, 1, TUpdate(title="a")),
            (pk2, 2, TUpdate(title="b")),
        ]
        out = await adapter.update_many(updates)

        assert out == [r1, r2]
        write_gw.update_many.assert_awaited_once()
        ua = write_gw.update_many.await_args
        assert ua[0][0] == [pk1, pk2]
        assert ua[0][1] == [TUpdate(title="a"), TUpdate(title="b")]
        assert ua[1]["revs"] == [1, 2]
        assert ua[1]["batch_size"] == 80

    @pytest.mark.asyncio
    async def test_delete_many_and_restore_many(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk1, pk2 = uuid4(), uuid4()
        r1, r2 = _tread(pk1), _tread(pk2)
        write_gw.delete_many = AsyncMock()
        write_gw.restore_many = AsyncMock()
        read_gw.get_many = AsyncMock(return_value=[r1, r2])
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
            batch_size=90,
        )

        dels = [(pk1, 1), (pk2, 2)]
        assert await adapter.delete_many(dels) == [r1, r2]
        write_gw.delete_many.assert_awaited_once()
        da = write_gw.delete_many.await_args
        assert da[0][0] == [pk1, pk2]
        assert da[1]["revs"] == [1, 2]
        assert da[1]["batch_size"] == 90

        assert await adapter.restore_many(dels) == [r1, r2]
        write_gw.restore_many.assert_awaited_once()
        ra = write_gw.restore_many.await_args
        assert ra[0][0] == [pk1, pk2]
        assert ra[1]["revs"] == [1, 2]
        assert ra[1]["batch_size"] == 90


class TestPostgresDocumentAdapterReturnNew:
    """``return_new=False`` performs writes but skips read-back and cache population."""

    @pytest.mark.asyncio
    async def test_create_skips_read_and_cache_when_return_new_false(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        dom = _tdoc()
        write_gw.create = AsyncMock(return_value=dom)
        cache = MagicMock()
        cache.set_versioned = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds, cache=cache),
        )

        assert await adapter.create(TCreate(title="n"), return_new=False) is None
        write_gw.create.assert_awaited_once()
        read_gw.get.assert_not_awaited()
        cache.set_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_many_skips_read_when_return_new_false(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        d1, d2 = _tdoc(), _tdoc()
        write_gw.create_many = AsyncMock(return_value=[d1, d2])
        cache = MagicMock()
        cache.set_many_versioned = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds, cache=cache),
            batch_size=50,
        )

        assert (
            await adapter.create_many(
                [TCreate(title="a"), TCreate(title="b")],
                return_new=False,
            )
            is None
        )
        write_gw.create_many.assert_awaited_once()
        read_gw.get_many.assert_not_awaited()
        cache.set_many_versioned.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_update_skips_read_when_return_new_false(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk = uuid4()
        write_gw.update = AsyncMock(return_value=(None, {}))
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds, cache=cache),
        )

        assert await adapter.update(pk, 1, TUpdate(title="z"), return_new=False) is None
        write_gw.update.assert_awaited_once_with(pk, TUpdate(title="z"), rev=1)
        read_gw.get.assert_not_awaited()
        cache.delete_many.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_many_skips_read_when_return_new_false(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk1, pk2 = uuid4(), uuid4()
        write_gw.update_many = AsyncMock(return_value=([], []))
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds, cache=cache),
            batch_size=80,
        )

        updates = [
            (pk1, 1, TUpdate(title="a")),
            (pk2, 2, TUpdate(title="b")),
        ]
        assert await adapter.update_many(updates, return_new=False) is None
        write_gw.update_many.assert_awaited_once()
        read_gw.get_many.assert_not_awaited()
        cache.delete_many.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_touch_skips_read_when_return_new_false(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk = uuid4()
        write_gw.touch = AsyncMock()
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds, cache=cache),
        )

        assert await adapter.touch(pk, return_new=False) is None
        write_gw.touch.assert_awaited_once_with(pk)
        read_gw.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_touch_many_skips_read_when_return_new_false(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pks = [uuid4(), uuid4()]
        write_gw.touch_many = AsyncMock()
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
            batch_size=50,
        )

        assert await adapter.touch_many(pks, return_new=False) is None
        write_gw.touch_many.assert_awaited_once_with(pks, batch_size=50)
        read_gw.get_many.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_restore_skips_read_when_return_new_false(self) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk = uuid4()
        write_gw.delete = AsyncMock()
        write_gw.restore = AsyncMock()
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds, cache=cache),
        )

        assert await adapter.delete(pk, 1, return_new=False) is None
        write_gw.delete.assert_awaited_once_with(pk, rev=1)
        read_gw.get.assert_not_awaited()

        assert await adapter.restore(pk, 2, return_new=False) is None
        write_gw.restore.assert_awaited_once_with(pk, rev=2)
        read_gw.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_many_restore_many_skips_read_when_return_new_false(
        self,
    ) -> None:
        read_gw = _read_gw_full()
        write_gw = _write_gw()
        write_gw.client = read_gw.client
        pk1, pk2 = uuid4(), uuid4()
        write_gw.delete_many = AsyncMock()
        write_gw.restore_many = AsyncMock()
        cache = MagicMock()
        cache.delete_many = AsyncMock()

        adapter = PostgresDocumentAdapter(
            spec=(ds := _full_spec()),
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=_pg_cc(read_gw, ds),
            batch_size=90,
        )

        dels = [(pk1, 1), (pk2, 2)]
        assert await adapter.delete_many(dels, return_new=False) is None
        write_gw.delete_many.assert_awaited_once()
        read_gw.get_many.assert_not_awaited()

        assert await adapter.restore_many(dels, return_new=False) is None
        write_gw.restore_many.assert_awaited_once()
        read_gw.get_many.assert_not_awaited()
