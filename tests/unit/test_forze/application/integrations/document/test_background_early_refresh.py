"""Background early refresh: serve the elected hit, recompute detached."""

from __future__ import annotations

import asyncio
import contextvars
import time
from datetime import timedelta
from typing import Awaitable, Callable
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
_FRESH = DocModel(id=_PK, rev=2, payload="fresh")


def _elected_envelope() -> dict:
    """A cached envelope far past expiry with real recompute cost: election certain."""

    return {
        "_xf": {"at": time.time() - 10_000, "d": 0.5},
        "doc": {"id": str(_PK), "rev": 1, "payload": "stale"},
    }


def _spec(background: bool = True) -> CacheSpec:
    return CacheSpec(
        name="c",
        ttl=timedelta(seconds=300),
        early_refresh_beta=1.0,
        early_refresh_background=background,
    )


def _coord(
    cache: AsyncMock,
    *,
    background: bool = True,
    after_commit=None,
) -> DocumentCache[DocModel]:
    return DocumentCache(
        read_model_type=DocModel,
        read_codec=_CODEC,
        document_name="widgets",
        cache=cache,
        after_commit=after_commit,
        cache_spec=_spec(background),
    )


async def _read(
    coord: DocumentCache[DocModel],
    fetch: Callable[[], Awaitable[DocModel]],
) -> DocModel:
    return await coord.get_read_through(
        _PK,
        fetch_on_cache_fault=fetch,
        fetch_on_miss_without_lock=fetch,
    )


async def _drain_tasks(coord: DocumentCache[DocModel]) -> None:
    while coord._bg_tasks:  # noqa: SLF001
        await asyncio.gather(*list(coord._bg_tasks), return_exceptions=True)  # noqa: SLF001


# ----------------------- #


class TestValidation:
    def test_background_requires_beta(self) -> None:
        with pytest.raises(CoreException):
            CacheSpec(name="c", early_refresh_background=True)


class TestBackgroundRefresh:
    async def test_elected_read_serves_hit_and_refreshes_detached(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = _elected_envelope()
        coord = _coord(cache)

        async def fetch() -> DocModel:
            return _FRESH

        result = await _read(coord, fetch)

        # The elected reader got the still-valid cached entry, not the fetch.
        assert result.payload == "stale"

        await _drain_tasks(coord)

        # The detached refresh re-warmed the backend with the fresh document.
        ((_, rev, _), _) = cache.set_versioned.await_args
        assert rev == "2"

    async def test_background_failure_invisible_to_caller(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = _elected_envelope()
        coord = _coord(cache)

        async def boom() -> DocModel:
            raise RuntimeError("db down")

        result = await _read(coord, boom)
        assert result.payload == "stale"

        await _drain_tasks(coord)  # swallowed + logged, never raised

        cache.set_versioned.assert_not_awaited()

    async def test_no_spawn_while_load_in_flight(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = _elected_envelope()
        coord = _coord(cache)

        # Another reader is already leading a load for this key.
        gate = asyncio.Event()

        async def leader_load() -> DocModel:
            await gate.wait()
            return _FRESH

        leader = asyncio.create_task(
            coord._inflight.run(str(_PK), leader_load)  # noqa: SLF001
        )
        await asyncio.sleep(0)  # let the leader register in flight
        assert str(_PK) in coord._inflight  # noqa: SLF001

        async def fetch() -> DocModel:  # pragma: no cover — must not run
            raise AssertionError("spawned despite in-flight load")

        result = await _read(coord, fetch)

        assert result.payload == "stale"
        assert not coord._bg_tasks  # noqa: SLF001

        gate.set()
        await leader

    async def test_spawn_deferred_until_after_commit(self) -> None:
        captured: list = []

        async def after_commit(fn) -> None:  # records instead of running
            captured.append(fn)

        cache = AsyncMock()
        cache.get.return_value = _elected_envelope()
        coord = _coord(cache, after_commit=after_commit)

        async def fetch() -> DocModel:
            return _FRESH

        result = await _read(coord, fetch)

        assert result.payload == "stale"
        assert not coord._bg_tasks  # noqa: SLF001 — nothing spawned inside the "tx"
        assert len(captured) == 1

        await captured[0]()  # the commit happens: now the refresh spawns
        await _drain_tasks(coord)

        # The refresh's own warm also rides after_commit; run it too.
        for fn in captured[1:]:
            await fn()

        cache.set_versioned.assert_awaited()

    async def test_context_propagates_into_detached_fetch(self) -> None:
        tenant: contextvars.ContextVar[str | None] = contextvars.ContextVar(
            "test_tenant", default=None
        )
        seen: list[str | None] = []

        cache = AsyncMock()
        cache.get.return_value = _elected_envelope()
        coord = _coord(cache)

        async def fetch() -> DocModel:
            seen.append(tenant.get())
            return _FRESH

        token = tenant.set("t-42")

        try:
            await _read(coord, fetch)

        finally:
            tenant.reset(token)  # the request unbinds before the task finishes

        await _drain_tasks(coord)

        assert seen == ["t-42"]  # the snapshot survived the caller's reset

    async def test_task_set_self_cleans(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = _elected_envelope()
        coord = _coord(cache)

        async def fetch() -> DocModel:
            return _FRESH

        await _read(coord, fetch)
        await _drain_tasks(coord)

        assert not coord._bg_tasks  # noqa: SLF001

    async def test_flag_off_keeps_inline_refresh(self) -> None:
        cache = AsyncMock()
        cache.get.return_value = _elected_envelope()
        coord = _coord(cache, background=False)

        async def fetch() -> DocModel:
            return _FRESH

        result = await _read(coord, fetch)

        # The elected reader awaited the recompute, exactly as before.
        assert result.payload == "fresh"
        assert not coord._bg_tasks  # noqa: SLF001


class TestRefreshFanoutCap:
    async def test_saturated_coordinator_drops_new_election(self) -> None:
        cache = AsyncMock()
        coord = DocumentCache(
            read_model_type=DocModel,
            read_codec=_CODEC,
            document_name="widgets",
            cache=cache,
            after_commit=None,
            cache_spec=_spec(True),
            max_inflight_refresh=1,
        )

        # Saturate with one in-flight refresh task.
        blocker = asyncio.Event()
        held = asyncio.get_running_loop().create_task(blocker.wait())
        coord._bg_tasks.add(held)  # noqa: SLF001
        held.add_done_callback(coord._bg_tasks.discard)  # noqa: SLF001

        async def fetch() -> DocModel:
            return _FRESH

        # A distinct key cannot spawn while at the cap — dropped, not queued.
        await coord._schedule_background_refresh("other-key", fetch)  # noqa: SLF001
        assert len(coord._bg_tasks) == 1  # noqa: SLF001

        blocker.set()
        await asyncio.gather(held, return_exceptions=True)
