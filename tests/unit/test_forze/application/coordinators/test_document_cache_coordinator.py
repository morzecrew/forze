"""Tests for :class:`forze.application.coordinators.DocumentCacheCoordinator`."""

from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from pydantic import BaseModel, Field

from forze.application.coordinators import DocumentCacheCoordinator
from forze.base.primitives import uuid7
from forze.base.serialization import pydantic_cache_dump

_pk = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class BareModel(BaseModel):
    """Missing rev — not eligible for versioned caching."""

    id: UUID = Field(default_factory=uuid7)


class DocModel(BaseModel):
    """Read-shaped row with optimistic ``rev``."""

    id: UUID
    rev: int
    payload: str = ""


def _coord(
    *,
    cache,
    model_type=DocModel,
    after_commit=None,
    name: str = "widgets",
):
    return DocumentCacheCoordinator(
        read_model_type=model_type,
        document_name=name,
        cache=cache,
        after_commit=after_commit,
    )


@pytest.mark.asyncio
async def test_after_commit_or_now_without_cache_skips_fn() -> None:
    fn = AsyncMock()

    coord = _coord(cache=None)
    await coord.after_commit_or_now(fn)

    fn.assert_not_called()


@pytest.mark.asyncio
async def test_after_commit_or_now_with_cache_awakens_fn_immediately_without_port() -> (
    None
):
    cache = AsyncMock()
    fn = AsyncMock()

    coord = _coord(cache=cache, after_commit=None)
    await coord.after_commit_or_now(fn)

    fn.assert_awaited_once()


@pytest.mark.asyncio
async def test_after_commit_or_now_delegates_to_port() -> None:
    cache = AsyncMock()
    fn = AsyncMock()
    defer = AsyncMock()

    coord = _coord(cache=cache, after_commit=defer)
    await coord.after_commit_or_now(fn)

    defer.assert_called_once_with(fn)


@pytest.mark.asyncio
async def test_set_one_versioned_writes_cache() -> None:
    cache = AsyncMock()

    doc = DocModel(id=_pk, rev=4, payload="x")
    coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="widgets",
        cache=cache,
    )

    await coord.set_one(doc)

    cache.set_versioned.assert_called_once()
    kw = cache.set_versioned.await_args.args
    assert kw[0] == str(_pk)
    assert kw[1] == "4"


@pytest.mark.asyncio
async def test_set_many_empty_noop() -> None:
    backend = AsyncMock()

    coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="widgets",
        cache=backend,
    )

    await coord.set_many([])

    backend.set_many_versioned.assert_not_called()


@pytest.mark.asyncio
async def test_set_many_bulk_versioned() -> None:
    backend = AsyncMock()

    docs = [DocModel(id=_pk, rev=i, payload=str(i)) for i in range(2)]
    coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="widgets",
        cache=backend,
    )

    await coord.set_many(docs)

    backend.set_many_versioned.assert_called_once()
    mapping = backend.set_many_versioned.await_args.args[0]
    assert set(mapping) == {(str(_pk), "0"), (str(_pk), "1")}


@pytest.mark.asyncio
async def test_clear_delete_many_when_capable() -> None:
    backend = AsyncMock()

    coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="widgets",
        cache=backend,
    )

    await coord.clear(_pk, UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"))

    backend.delete_many.assert_called_once()
    keys, kwargs = (
        backend.delete_many.await_args.args,
        backend.delete_many.await_args.kwargs,
    )
    assert keys[0] == [str(_pk), "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"]
    assert kwargs["hard"] is True


def test_id_rev_capable_false_without_rev() -> None:
    coord = DocumentCacheCoordinator[BareModel](
        read_model_type=BareModel,
        document_name="bare",
        cache=AsyncMock(),
    )

    assert coord.id_rev_capable() is False


def test_id_rev_capable_true() -> None:
    coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="ok",
        cache=AsyncMock(),
    )

    assert coord.id_rev_capable() is True


def test_read_through_eligible() -> None:
    coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="w",
        cache=AsyncMock(),
    )

    assert coord.read_through_eligible(skip_cache=False, return_fields=None)
    assert not coord.read_through_eligible(skip_cache=True, return_fields=None)
    assert not coord.read_through_eligible(
        skip_cache=False,
        return_fields=["id"],
    )

    no_cache_coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="z",
        cache=None,
    )

    assert not no_cache_coord.read_through_eligible(
        skip_cache=False,
        return_fields=None,
    )


@pytest.mark.asyncio
async def test_get_read_through_cache_hit() -> None:
    dumped = pydantic_cache_dump(DocModel(id=_pk, rev=1, payload="a"))
    backend = AsyncMock()

    backend.get = AsyncMock(return_value=dumped)

    coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="w",
        cache=backend,
    )

    fault = AsyncMock()

    miss = AsyncMock()

    out = await coord.get_read_through(
        _pk,
        fetch_on_cache_fault=fault,
        fetch_on_miss_without_lock=miss,
    )

    assert isinstance(out, DocModel)
    assert out.id == _pk
    fault.assert_not_awaited()
    miss.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_read_through_miss_sets_cache_immediately() -> None:
    backend = AsyncMock()

    backend.get = AsyncMock(return_value=None)

    doc = DocModel(id=_pk, rev=9, payload="z")

    coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="w",
        cache=backend,
        after_commit=None,
    )

    fault = AsyncMock()

    miss = AsyncMock(return_value=doc)

    out = await coord.get_read_through(
        _pk,
        fetch_on_cache_fault=fault,
        fetch_on_miss_without_lock=miss,
    )

    assert out == doc
    backend.set_versioned.assert_called_once()


@pytest.mark.asyncio
async def test_get_many_read_through_merges_order() -> None:
    pk0 = UUID("00000000-0000-0000-0000-000000000001")
    pk1 = UUID("00000000-0000-0000-0000-000000000002")
    doc0 = DocModel(id=pk0, rev=1, payload="a")
    doc1 = DocModel(id=pk1, rev=2, payload="b")

    backend = AsyncMock()

    backend.get_many = AsyncMock(
        return_value=(
            {str(pk0): pydantic_cache_dump(doc0)},
            [str(pk1)],
        ),
    )

    coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="w",
        cache=backend,
        after_commit=None,
    )

    fetched = AsyncMock(return_value=[doc1])

    out = await coord.get_many_read_through(
        [pk0, pk1],
        fetch_many_on_cache_fault=AsyncMock(),
        fetch_misses_many=fetched,
    )

    fetched.assert_awaited_once_with([str(pk1)])

    assert [x.id for x in out] == [pk0, pk1]


@pytest.mark.asyncio
async def test_get_read_through_fallback_when_cache_raises() -> None:
    backend = AsyncMock()
    backend.get = AsyncMock(side_effect=RuntimeError("down"))

    coord = DocumentCacheCoordinator[DocModel](
        read_model_type=DocModel,
        document_name="w",
        cache=backend,
    )

    doc = DocModel(id=_pk, rev=3, payload="fallback")

    fb = AsyncMock(return_value=doc)

    miss = AsyncMock()

    out = await coord.get_read_through(
        _pk,
        fetch_on_cache_fault=fb,
        fetch_on_miss_without_lock=miss,
    )

    assert out == doc

    fb.assert_awaited_once()

    miss.assert_not_awaited()
