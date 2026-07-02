"""L1 invalidation push: coordinator subscription, drops, flush-on-reset."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, Awaitable, Callable
from unittest.mock import AsyncMock
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.cache import (
    CacheInvalidation,
    CacheSpec,
    InvalidationCallback,
    L1Spec,
)
from forze.application.integrations.document import DocumentCache
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #

_PK = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class DocModel(BaseModel):
    id: UUID
    rev: int
    payload: str = ""


_CODEC = codec_for(DocModel)
_DOC = DocModel(id=_PK, rev=1, payload="x")


class _PushCache:
    """CachePort stand-in implementing SupportsInvalidationPush structurally."""

    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.callback: InvalidationCallback | None = None
        self.subscriptions = 0
        self.get = AsyncMock(return_value=None)
        self.set_versioned = AsyncMock()
        self.delete_many = AsyncMock()
        self.get_many = AsyncMock(return_value=({}, []))
        self.set_many_versioned = AsyncMock()

    async def subscribe_invalidations(
        self,
        callback: InvalidationCallback,
    ) -> Callable[[], Awaitable[None]] | None:
        self.subscriptions += 1

        if not self.available:
            return None

        self.callback = callback

        async def _unsubscribe() -> None:
            self.callback = None

        return _unsubscribe


class _YieldingPushCache(_PushCache):
    """A push cache whose ``subscribe`` suspends mid-call, widening any race window."""

    async def subscribe_invalidations(
        self,
        callback: InvalidationCallback,
    ) -> Callable[[], Awaitable[None]] | None:
        self.subscriptions += 1
        await asyncio.sleep(0)  # suspend so a concurrent first-reader can interleave here
        self.callback = callback

        async def _unsubscribe() -> None:
            self.callback = None

        return _unsubscribe


def _coord(cache: Any, *, tenant: str | None = None) -> DocumentCache[DocModel]:
    return DocumentCache(
        read_model_type=DocModel,
        read_codec=_CODEC,
        document_name="widgets",
        cache=cache,
        after_commit=None,
        cache_spec=CacheSpec(
            name="c",
            ttl=timedelta(seconds=300),
            l1=L1Spec(ttl=timedelta(seconds=60)),
        ),
        tenant_key=lambda: tenant,
    )


async def _fetch() -> DocModel:
    return _DOC


async def _read(coord: DocumentCache[DocModel]) -> DocModel:
    return await coord.get_read_through(
        _PK,
        fetch_on_cache_fault=_fetch,
        fetch_on_miss_without_lock=_fetch,
    )


# ----------------------- #


class TestSubscription:
    async def test_first_read_subscribes_once(self) -> None:
        cache = _PushCache()
        coord = _coord(cache)

        await _read(coord)
        await _read(coord)
        await _read(coord)

        assert cache.subscriptions == 1
        assert cache.callback is not None

    async def test_concurrent_first_reads_subscribe_once(self) -> None:
        # Two first-readers race. The guard sets its ``started`` flag synchronously *before*
        # the subscribe await, so the reader that runs second — while the first is suspended
        # mid-subscribe — sees the subscription already claimed and does not double-subscribe.
        cache = _YieldingPushCache()
        coord = _coord(cache)

        await asyncio.gather(_read(coord), _read(coord))

        assert cache.subscriptions == 1  # exactly one subscription despite the race

    async def test_unavailable_push_degrades_to_ttl_only(self) -> None:
        cache = _PushCache(available=False)
        coord = _coord(cache)

        await _read(coord)
        second = await _read(coord)  # L1 still works, just TTL-bounded

        assert second == _DOC
        assert cache.get.await_count == 1

    async def test_plain_port_without_capability_is_fine(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = None
        del cache.subscribe_invalidations  # AsyncMock would fake the protocol

        coord = _coord(cache)
        assert await _read(coord) == _DOC

    async def test_failing_subscription_does_not_break_reads(self) -> None:
        cache = _PushCache()

        async def _boom(_cb: InvalidationCallback) -> None:
            raise RuntimeError("tracking down")

        cache.subscribe_invalidations = _boom  # type: ignore[method-assign]
        coord = _coord(cache)

        assert await _read(coord) == _DOC  # warning logged, read unaffected


class TestInvalidationEvents:
    async def test_pushed_key_drops_l1_entry(self) -> None:
        cache = _PushCache()
        coord = _coord(cache)

        await _read(coord)  # warm L1 + subscribe
        assert cache.callback is not None

        cache.callback(CacheInvalidation(key=str(_PK)))

        await _read(coord)  # must re-fetch through L2
        assert cache.get.await_count == 2

    async def test_tenant_scoped_event_drops_only_that_tenant(self) -> None:
        cache = _PushCache()
        tenant = {"current": "t1"}
        coord = DocumentCache(
            read_model_type=DocModel,
            read_codec=_CODEC,
            document_name="widgets",
            cache=cache,
            after_commit=None,
            cache_spec=CacheSpec(
                name="c",
                ttl=timedelta(seconds=300),
                l1=L1Spec(ttl=timedelta(seconds=60)),
            ),
            tenant_key=lambda: tenant["current"],
        )

        await _read(coord)  # warms t1's entry
        tenant["current"] = "t2"
        await _read(coord)  # warms t2's entry
        assert cache.callback is not None

        cache.callback(CacheInvalidation(key=str(_PK), tenant="t1"))

        # t2's entry survived: this read is an L1 hit.
        await _read(coord)
        assert cache.get.await_count == 2

        # t1's entry was dropped: this read goes back to L2.
        tenant["current"] = "t1"
        await _read(coord)
        assert cache.get.await_count == 3

    async def test_reset_event_flushes_everything(self) -> None:
        cache = _PushCache()
        coord = _coord(cache)

        await _read(coord)
        assert cache.callback is not None

        cache.callback(CacheInvalidation(key=None))

        await _read(coord)
        assert cache.get.await_count == 2

    async def test_unrelated_key_leaves_entry_alone(self) -> None:
        cache = _PushCache()
        coord = _coord(cache)

        await _read(coord)
        assert cache.callback is not None

        cache.callback(CacheInvalidation(key="some-other-pk"))

        await _read(coord)  # still an L1 hit
        assert cache.get.await_count == 1
