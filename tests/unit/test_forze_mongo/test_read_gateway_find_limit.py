"""Tests for the implicit find cap on :class:`~forze_mongo.kernel.gateways.read.MongoReadGateway`."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.base.exceptions import CoreException
from forze.domain.models import Document
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.gateways import MongoReadGateway
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #


class _Row(Document):
    title: str


def _gw(**kw: object) -> tuple[MongoReadGateway[_Row], MagicMock]:
    client = MagicMock(spec=MongoClient)
    client.collection = AsyncMock(return_value=object())
    client.find_many = AsyncMock(return_value=[])
    client.aggregate = AsyncMock(return_value=[{"n": 0}])

    gw = MongoReadGateway(
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
    assert client.find_many.await_args.kwargs["limit"] == 10_000


@pytest.mark.asyncio
async def test_find_many_applies_configured_implicit_limit() -> None:
    gw, client = _gw(find_many_implicit_limit=42)

    await gw.find_many(None, limit=None)

    assert client.find_many.await_args.kwargs["limit"] == 42


@pytest.mark.asyncio
async def test_find_many_explicit_limit_honored() -> None:
    gw, client = _gw(find_many_implicit_limit=42)

    await gw.find_many(None, limit=7)

    assert client.find_many.await_args.kwargs["limit"] == 7


@pytest.mark.asyncio
async def test_find_many_cap_none_disables() -> None:
    gw, client = _gw(find_many_implicit_limit=None)

    await gw.find_many(None, limit=None)

    assert client.find_many.await_args.kwargs["limit"] is None


@pytest.mark.asyncio
async def test_find_many_aggregates_applies_implicit_limit() -> None:
    gw, client = _gw(find_many_implicit_limit=42)

    await gw.find_many(
        None,
        aggregates={"$computed": {"n": {"$count": None}}},
    )

    assert client.aggregate.await_args.kwargs["limit"] == 42


def test_invalid_cap_rejected() -> None:
    with pytest.raises(CoreException, match="at least 1"):
        _gw(find_many_implicit_limit=0)
