"""Pytest configuration for Temporal integration tests."""

from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio
from temporalio.contrib.pydantic import pydantic_data_converter

pytest.importorskip("temporalio")
pytest.importorskip("testcontainers")

from forze_temporal.kernel.client import TemporalClient, TemporalConfig

from .temporal_dev_server import (
    TemporalDevTarget,
    ensure_docker_available,
    start_temporal_dev_container,
)


async def await_listed_schedules[T](
    fetch: Callable[[], Awaitable[Sequence[T]]],
    *,
    count: int,
    timeout: timedelta = timedelta(seconds=30),
) -> Sequence[T]:
    """Poll a schedule listing until at least *count* entries are visible.

    Schedule listings read from Temporal's visibility store, which lags writes: asserting
    on a listing taken right after a create is a race that a namespace with other
    schedule traffic loses. Waiting keeps such an assertion about *isolation* or
    *pagination* rather than about visibility timing.
    """

    deadline = time.monotonic() + timeout.total_seconds()
    listed: Sequence[T] = ()

    while time.monotonic() < deadline:
        listed = await fetch()

        if len(listed) >= count:
            return listed

        await asyncio.sleep(0.25)

    pytest.fail(f"only {len(listed)}/{count} schedules became visible within {timeout}")


@pytest.fixture(scope="session", autouse=True)
def _reap_orphaned_time_skipping_servers():
    """Reap orphaned ``temporal-test-server`` subprocesses before the session.

    Each ``WorkflowEnvironment.start_time_skipping()`` spawns a test-server subprocess that
    ``env.shutdown()`` tears down in a ``finally``. If a previous pytest run was hard-killed
    mid-test, that ``finally`` never ran and the subprocess **orphans** — and a stray can
    wedge a fresh run at the first time-skipping test. Clear any strays before this session
    starts its own (no server exists yet at this point, so the kill is safe).

    Skipped under ``pytest-xdist``: parallel workers each run their own server, so a blanket
    kill could tear down a sibling worker's live one. The default (serial) run is unaffected.
    """

    import os
    import shutil
    import subprocess

    if "PYTEST_XDIST_WORKER" not in os.environ:
        pkill = shutil.which("pkill")
        if pkill is not None:
            subprocess.run(
                [pkill, "-f", "temporal-test-server-sdk-python"], check=False
            )

    yield


@pytest_asyncio.fixture
async def workflow_env():
    """Temporal test environment with time skipping (no Docker)."""
    from temporalio.testing import WorkflowEnvironment

    env = await WorkflowEnvironment.start_time_skipping()
    try:
        yield env
    finally:
        await env.shutdown()


@pytest_asyncio.fixture
async def workflow_env_with_host_target():
    """Time-skipping env bound to an explicit localhost port (for RoutedTemporalClient secrets)."""

    from temporalio.testing import WorkflowEnvironment

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    env = await WorkflowEnvironment.start_time_skipping(
        port=port, data_converter=pydantic_data_converter
    )
    try:
        yield env, f"127.0.0.1:{port}"

    finally:
        await env.shutdown()


# ....................... #


@pytest.fixture(scope="session")
def temporal_dev_target() -> TemporalDevTarget:
    """Session-scoped Temporal dev server (Docker, Schedules API enabled)."""

    ensure_docker_available()
    container, target = start_temporal_dev_container()
    try:
        yield target
    finally:
        container.stop()


@pytest_asyncio.fixture
async def temporal_dev_env(temporal_dev_target: TemporalDevTarget):
    """Connected Temporal SDK + Forze :class:`TemporalClient` against the dev server."""

    from temporalio.client import Client

    sdk_client = await Client.connect(
        temporal_dev_target.grpc_address,
        data_converter=pydantic_data_converter,
    )
    forze_client = TemporalClient()
    await forze_client.initialize(
        temporal_dev_target.grpc_address,
        config=TemporalConfig(namespace="default"),
    )
    try:
        yield SimpleNamespace(
            client=sdk_client,
            forze_client=forze_client,
            target=temporal_dev_target.grpc_address,
        )
    finally:
        await forze_client.close()
