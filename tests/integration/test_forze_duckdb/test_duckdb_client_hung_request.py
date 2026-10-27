"""A statement timeout returns to the caller while a hung HTTP request is still running.

RFC 0078 EXECUTION-LOG D-028: ``cursor.interrupt()`` does not cut a blocking HTTP call short,
so the worker runs on until the request itself gives up. D-042 has ``DuckDbClient`` raise at the
statement timeout instead of waiting for it, while the worker keeps its thread and its admission
slot. A local socket that accepts and never answers stands in for a hung catalog or store, so no
network is involved beyond installing ``httpfs``.
"""

from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import Iterator
from datetime import timedelta

import pytest

from forze.base.exceptions import CoreException
from forze_duckdb import DuckDbClient
from forze_duckdb.kernel.client import QUERY_TIMEOUT_CODE

pytestmark = pytest.mark.integration

_REQUEST_TIMEOUT_S = 3

# ----------------------- #


@pytest.fixture
def blackhole() -> Iterator[int]:
    """A port whose listen backlog takes the connection and nothing ever answers."""

    with socket.socket() as srv:
        srv.bind(("127.0.0.1", 0))
        srv.listen(8)
        yield srv.getsockname()[1]


async def test_the_caller_gets_the_timeout_before_the_worker_returns(blackhole: int) -> None:
    admission = asyncio.Semaphore(1)
    client = DuckDbClient()
    await client.initialize(":memory:", extensions=("httpfs",), admission=admission)
    await client.run_command(f"SET GLOBAL http_timeout={_REQUEST_TIMEOUT_S}")
    await client.run_command("SET GLOBAL http_retries=0")
    started = time.monotonic()

    try:
        with pytest.raises(CoreException) as ei:
            await client.run_query(
                f"SELECT * FROM read_csv('http://127.0.0.1:{blackhole}/x.csv')",
                timeout=timedelta(milliseconds=300),
            )

        assert ei.value.code == QUERY_TIMEOUT_CODE
        assert time.monotonic() - started < 1.5  # not the request's own 3 s
        assert admission.locked()  # the worker still holds its slot

    finally:
        await client.close()  # waits for the worker to unwind

    assert time.monotonic() - started >= _REQUEST_TIMEOUT_S - 0.5
    assert not admission.locked()
