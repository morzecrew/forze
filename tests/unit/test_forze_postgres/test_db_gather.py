"""Unit tests for :func:`~forze_postgres.kernel.db_gather.gather_db_work`."""

import asyncio
from unittest.mock import MagicMock

import pytest

pytest.importorskip("psycopg")

from forze_postgres.kernel.db_gather import gather_db_work
from forze_postgres.kernel.platform.client import PostgresClient


@pytest.mark.asyncio
async def test_gather_db_work_empty() -> None:
    client = MagicMock(spec=PostgresClient)
    client.gather_concurrency_semaphore = MagicMock(
        return_value=asyncio.Semaphore(8),
    )
    assert await gather_db_work(client, []) == []


@pytest.mark.asyncio
async def test_gather_db_work_serializes_in_transaction() -> None:
    client = MagicMock(spec=PostgresClient)
    client.gather_concurrency_semaphore = MagicMock(
        return_value=asyncio.Semaphore(8),
    )
    client.is_in_transaction.return_value = True
    client.query_concurrency_limit.return_value = 99

    order: list[int] = []

    async def track(x: int) -> int:
        order.append(x)
        return x * 10

    out = await gather_db_work(
        client,
        [lambda: track(1), lambda: track(2), lambda: track(3)],
    )

    assert out == [10, 20, 30]
    assert order == [1, 2, 3]


@pytest.mark.asyncio
async def test_gather_db_work_limit_one_serializes() -> None:
    client = MagicMock(spec=PostgresClient)
    client.gather_concurrency_semaphore = MagicMock(
        return_value=asyncio.Semaphore(8),
    )
    client.is_in_transaction.return_value = False
    client.query_concurrency_limit.return_value = 1

    order: list[int] = []

    async def track(x: int) -> int:
        order.append(x)
        return x

    out = await gather_db_work(
        client,
        [lambda: track(1), lambda: track(2)],
    )

    assert out == [1, 2]
    assert order == [1, 2]


@pytest.mark.asyncio
async def test_gather_db_work_allows_parallel_when_limit_gt_one() -> None:
    """When limit > 1 and not in a transaction, multiple makers may run concurrently."""
    client = MagicMock(spec=PostgresClient)
    client.gather_concurrency_semaphore = MagicMock(
        return_value=asyncio.Semaphore(8),
    )
    client.is_in_transaction.return_value = False
    client.query_concurrency_limit.return_value = 2

    running = 0
    max_running = 0

    async def work(label: str) -> str:
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0.03)
        running -= 1
        return label

    out = await gather_db_work(
        client,
        [lambda: work("a"), lambda: work("b")],
    )

    assert sorted(out) == ["a", "b"]
    assert max_running == 2
