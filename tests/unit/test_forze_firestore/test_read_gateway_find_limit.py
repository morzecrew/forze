"""Tests for the implicit find cap on :class:`~forze_firestore.kernel.gateways.read.FirestoreReadGateway`."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.base.exceptions import CoreException
from forze.domain.models import Document
from forze_firestore.kernel.gateways.read import FirestoreReadGateway
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #


class _Row(Document):
    title: str


def _gw(**kw: object) -> tuple[FirestoreReadGateway[_Row], MagicMock]:
    client = MagicMock()
    client.collection = AsyncMock(return_value=object())
    client.query_stream = AsyncMock(return_value=[])

    gw = FirestoreReadGateway(
        relation=("db", "t"),
        client=client,
        model_type=_Row,
        codec=codec_for(_Row),
        tenant_aware=False,
        **kw,  # type: ignore[arg-type]
    )
    return gw, client


# ....................... #


@pytest.mark.asyncio
async def test_find_many_no_filters_no_limit_applies_default_cap() -> None:
    """No filters and no limit no longer raises; the implicit cap applies."""

    gw, client = _gw()

    out = await gw.find_many(None, limit=None)

    assert out == []
    assert client.query_stream.await_args.kwargs["limit"] == 10_000


@pytest.mark.asyncio
async def test_find_many_applies_configured_implicit_limit() -> None:
    gw, client = _gw(find_many_implicit_limit=42)

    await gw.find_many(None, limit=None)

    assert client.query_stream.await_args.kwargs["limit"] == 42


@pytest.mark.asyncio
async def test_find_many_explicit_limit_honored() -> None:
    gw, client = _gw(find_many_implicit_limit=42)

    await gw.find_many(None, limit=7)

    assert client.query_stream.await_args.kwargs["limit"] == 7


@pytest.mark.asyncio
async def test_find_many_cap_none_disables() -> None:
    gw, client = _gw(find_many_implicit_limit=None)

    await gw.find_many(None, limit=None)

    assert client.query_stream.await_args.kwargs["limit"] is None


def test_invalid_cap_rejected() -> None:
    with pytest.raises(CoreException, match="at least 1"):
        _gw(find_many_implicit_limit=0)


# ....................... #
# find_many_chunked (streaming)


async def _agen(*batches: list[dict]):
    for batch in batches:
        yield batch


@pytest.mark.asyncio
async def test_find_many_chunked_streams_batches_uncapped() -> None:
    gw, client = _gw()
    captured: dict[str, object] = {}

    def _batched(coll, *, filters=None, order_by=None, limit=None, fetch_batch_size=2000):
        captured["limit"] = limit
        captured["fetch_batch_size"] = fetch_batch_size
        return _agen(
            [{"id": "1", "title": "a"}],
            [{"id": "2", "title": "b"}, {"id": "3", "title": "c"}],
        )

    client.query_stream_batched = _batched

    chunks = [
        chunk
        async for chunk in gw.find_many_chunked(
            None, fetch_batch_size=2, return_fields=["title"]
        )
    ]

    assert [[row["title"] for row in chunk] for chunk in chunks] == [["a"], ["b", "c"]]
    # Chunked passes the limit through unchanged — no implicit cap (stream everything).
    assert captured["limit"] is None
    assert captured["fetch_batch_size"] == 2


@pytest.mark.asyncio
async def test_find_many_chunked_rejects_offset() -> None:
    gw, _ = _gw()

    with pytest.raises(CoreException, match="offset"):
        async for _chunk in gw.find_many_chunked(None, offset=5):
            pass
