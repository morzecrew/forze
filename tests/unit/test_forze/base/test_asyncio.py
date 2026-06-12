"""Tests for :mod:`forze.base.asyncio` helpers."""

from __future__ import annotations

import asyncio

import pytest

from forze.base.asyncio import maybe_await, run_to_completion


@pytest.mark.asyncio
async def test_maybe_await_returns_plain_value() -> None:
    assert await maybe_await(42) == 42


@pytest.mark.asyncio
async def test_maybe_await_awaits_coroutine() -> None:
    async def _value() -> str:
        return "ok"

    assert await maybe_await(_value()) == "ok"


# ....................... #


class TestRunToCompletion:
    async def test_returns_result_without_cancellation(self) -> None:
        async def _value() -> str:
            return "ok"

        assert await run_to_completion(_value()) == "ok"

    async def test_propagates_error_without_cancellation(self) -> None:
        async def _boom() -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await run_to_completion(_boom())

    async def test_completes_despite_cancellation_then_reraises(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        finished: list[str] = []

        async def _critical() -> None:
            started.set()
            await release.wait()
            finished.append("done")

        async def _outer() -> None:
            await run_to_completion(_critical())

        task = asyncio.create_task(_outer())
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert finished == ["done"]

    async def test_cancellation_wins_over_inner_error(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def _critical() -> None:
            started.set()
            await release.wait()
            raise ValueError("inner failure")

        async def _outer() -> None:
            await run_to_completion(_critical())

        task = asyncio.create_task(_outer())
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_survives_repeated_cancellation(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        finished: list[str] = []

        async def _critical() -> None:
            started.set()
            await release.wait()
            finished.append("done")

        async def _outer() -> None:
            await run_to_completion(_critical())

        task = asyncio.create_task(_outer())
        await started.wait()

        for _ in range(3):
            task.cancel()
            await asyncio.sleep(0)

        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert finished == ["done"]
