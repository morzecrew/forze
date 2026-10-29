"""``MongoCounterAdapter`` creating a counter while another writer races it (no server).

The live battery cannot stage the interleavings these pin: the upsert collides with a
document another caller just created, and that document is gone again — reaped by the TTL
monitor — before this caller can join it; or the pre-route seed collides with a document
that already holds the new ``_id``.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("pymongo")

from forze.application.contracts.counter import counter_capabilities
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_mongo.adapters.counter import MongoCounterAdapter
from forze_mongo.execution.deps.configs import MongoCounterConfig

pytestmark = pytest.mark.unit

# ----------------------- #

DUPLICATE = exc.conflict("Duplicate key violation.")


class _Client:
    """Just enough of ``MongoClientPort`` for one allocation."""

    def __init__(self, *outcomes: Any) -> None:
        self.collection = AsyncMock(return_value=object())
        self.find_one = AsyncMock(return_value=None)  # no live document, no legacy one
        self.find_one_and_update = AsyncMock(side_effect=outcomes)
        self.insert_one = AsyncMock()
        self.delete_one = AsyncMock()

    @asynccontextmanager
    async def detached(self):  # type: ignore[no-untyped-def]
        yield


def _counter(client: _Client) -> MongoCounterAdapter:
    return MongoCounterAdapter(
        client=client,  # type: ignore[arg-type]
        config=MongoCounterConfig(collection=("db", "counters")),
        route="orders",
    )


async def test_a_collision_with_a_vanishing_document_retries_the_creation() -> None:
    # live miss, create collides, join finds nothing (reaped), create lands
    client = _Client(None, DUPLICATE, None, {"value": 1})

    assert await _counter(client).incr() == 1
    assert client.find_one_and_update.await_count == 4


async def test_a_creation_that_keeps_colliding_gives_up_as_a_concurrency_error() -> None:
    client = _Client(None, *([DUPLICATE, None] * 3))

    with pytest.raises(CoreException) as refused:
        await _counter(client).incr()

    assert refused.value.kind is ExceptionKind.CONCURRENCY


async def test_a_creation_that_fails_otherwise_is_not_retried() -> None:
    client = _Client(None, exc.infrastructure("server unavailable"))

    with pytest.raises(CoreException) as failed:
        await _counter(client).incr()

    assert failed.value.kind is ExceptionKind.INFRASTRUCTURE
    assert client.find_one_and_update.await_count == 2


async def test_a_seed_that_collides_retires_the_legacy_document_and_creates_afresh() -> None:
    # live miss (the new ``_id`` holds a lapsed document), the seed collides with it, the
    # creation overwrites it
    client = _Client(None, {"value": 1})
    client.find_one.return_value = {"value": 41}
    client.insert_one.side_effect = DUPLICATE

    assert await _counter(client).incr() == 1
    client.delete_one.assert_awaited_once()
    assert client.find_one_and_update.await_count == 2, "no second live attempt after the seed"


def test_reports_expiry() -> None:
    assert counter_capabilities(_counter(_Client())).expiry
