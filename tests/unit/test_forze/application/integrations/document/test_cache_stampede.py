"""Cache stampede protection: singleflight collapse + XFetch early refresh."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.cache import CacheSpec
from forze.application.integrations.document import DocumentCache
from forze.base.exceptions import CoreException
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #

_PK = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class DocModel(BaseModel):
    id: UUID
    rev: int
    payload: str = ""


_CODEC = codec_for(DocModel)
_DOC = DocModel(id=_PK, rev=1, payload="x")


def _coord(*, cache: AsyncMock, spec: CacheSpec | None = None) -> DocumentCache[DocModel]:
    return DocumentCache(
        read_model_type=DocModel,
        read_codec=_CODEC,
        document_name="widgets",
        cache=cache,
        after_commit=None,
        cache_spec=spec,
    )


def _xf_spec(beta: float = 1.0) -> CacheSpec:
    return CacheSpec(name="c", ttl=timedelta(seconds=300), early_refresh_beta=beta)


# ----------------------- #


class TestSingleflight:
    async def test_concurrent_misses_fetch_once(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = None
        coord = _coord(cache=cache)

        fetches = 0
        release = asyncio.Event()

        async def fetch() -> DocModel:
            nonlocal fetches
            fetches += 1
            await release.wait()
            return _DOC

        async def read() -> DocModel:
            return await coord.get_read_through(
                _PK,
                fetch_on_cache_fault=fetch,
                fetch_on_miss_without_lock=fetch,
            )

        a = asyncio.create_task(read())
        await asyncio.sleep(0)
        b = asyncio.create_task(read())
        await asyncio.sleep(0)
        release.set()

        assert await a == _DOC
        assert await b == _DOC
        assert fetches == 1
        # Only the leader warms the cache.
        cache.set_versioned.assert_awaited_once()

    async def test_followers_share_leader_failure(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = None
        coord = _coord(cache=cache)
        release = asyncio.Event()

        async def fetch() -> DocModel:
            await release.wait()
            raise RuntimeError("db down")

        async def read() -> DocModel:
            return await coord.get_read_through(
                _PK,
                fetch_on_cache_fault=fetch,
                fetch_on_miss_without_lock=fetch,
            )

        a = asyncio.create_task(read())
        await asyncio.sleep(0)
        b = asyncio.create_task(read())
        await asyncio.sleep(0)
        release.set()

        with pytest.raises(RuntimeError):
            await a

        with pytest.raises(RuntimeError):
            await b

    async def test_follower_retries_leadership_when_leader_cancelled(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = None
        coord = _coord(cache=cache)

        fetches = 0
        release = asyncio.Event()

        async def fetch() -> DocModel:
            nonlocal fetches
            fetches += 1
            await release.wait()
            return _DOC

        async def read() -> DocModel:
            return await coord.get_read_through(
                _PK,
                fetch_on_cache_fault=fetch,
                fetch_on_miss_without_lock=fetch,
            )

        leader = asyncio.create_task(read())
        await asyncio.sleep(0)
        follower = asyncio.create_task(read())
        await asyncio.sleep(0)

        leader.cancel()
        await asyncio.sleep(0)
        release.set()

        assert await follower == _DOC
        assert fetches == 2  # follower became the new leader

        with pytest.raises(asyncio.CancelledError):
            await leader


class TestEarlyRefresh:
    async def test_envelope_written_when_enabled(self) -> None:
        cache = AsyncMock()
        coord = _coord(cache=cache, spec=_xf_spec())

        await coord.set_one(_DOC, delta=0.5)

        ((key, rev, payload), _) = cache.set_versioned.await_args
        assert key == str(_PK)
        assert rev == "1"
        assert payload["_xf"]["d"] == 0.5
        assert payload["doc"]["rev"] == 1

    async def test_plain_bytes_without_beta(self) -> None:
        cache = AsyncMock()
        coord = _coord(cache=cache)

        await coord.set_one(_DOC)

        ((_, _, payload), _) = cache.set_versioned.await_args
        assert isinstance(payload, bytes)

    async def test_expired_envelope_elects_refresh(self) -> None:
        cache = AsyncMock()
        # Written far past expiry with a real recompute cost: election certain.
        cache.get.return_value = {
            "_xf": {"at": time.time() - 10_000, "d": 0.5},
            "doc": {"id": str(_PK), "rev": 1, "payload": "stale"},
        }
        coord = _coord(cache=cache, spec=_xf_spec())

        async def fetch() -> DocModel:
            return _DOC

        res = await coord.get_read_through(
            _PK,
            fetch_on_cache_fault=fetch,
            fetch_on_miss_without_lock=fetch,
        )

        assert res.payload == "x"  # refreshed, not the stale hit
        cache.set_versioned.assert_awaited_once()

    async def test_fresh_envelope_serves_hit(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = {
            "_xf": {"at": time.time(), "d": 0.001},
            "doc": {"id": str(_PK), "rev": 1, "payload": "hot"},
        }
        coord = _coord(cache=cache, spec=_xf_spec())

        async def fetch() -> DocModel:  # pragma: no cover — must not run
            raise AssertionError("fetched despite fresh hit")

        res = await coord.get_read_through(
            _PK,
            fetch_on_cache_fault=fetch,
            fetch_on_miss_without_lock=fetch,
        )

        assert res.payload == "hot"

    async def test_legacy_payload_decodes_with_beta_enabled(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = {"id": str(_PK), "rev": 1, "payload": "old"}
        coord = _coord(cache=cache, spec=_xf_spec())

        async def fetch() -> DocModel:  # pragma: no cover — must not run
            raise AssertionError("fetched despite hit")

        res = await coord.get_read_through(
            _PK,
            fetch_on_cache_fault=fetch,
            fetch_on_miss_without_lock=fetch,
        )

        assert res.payload == "old"

    def test_spec_rejects_non_positive_beta(self) -> None:
        with pytest.raises(CoreException):
            CacheSpec(name="c", early_refresh_beta=0.0)
