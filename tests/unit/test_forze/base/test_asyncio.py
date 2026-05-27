"""Tests for :func:`~forze.base.asyncio.maybe_await`."""

from __future__ import annotations

import pytest

from forze.base.asyncio import maybe_await


@pytest.mark.asyncio
async def test_maybe_await_returns_plain_value() -> None:
    assert await maybe_await(42) == 42


@pytest.mark.asyncio
async def test_maybe_await_awaits_coroutine() -> None:
    async def _value() -> str:
        return "ok"

    assert await maybe_await(_value()) == "ok"
